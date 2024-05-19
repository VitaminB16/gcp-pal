import os
import json

from gcp_pal.utils import try_import

from gcp_pal.utils import get_auth_default, log, ClientHandler, ModuleHandler


class CloudScheduler:

    def __init__(self, name=None, project=None, location="europe-west2"):
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        if isinstance(name, str) and name.startswith("projects/"):
            name = name.split("/")[-1]
        self.name = name
        self.location = location
        self.parent = f"projects/{self.project}/locations/{self.location}"
        self.full_name = f"{self.parent}/jobs/{self.name}"

        self.scheduler = ModuleHandler("google.cloud").please_import(
            "scheduler_v1", who_is_calling="CloudScheduler"
        )
        self.types = self.scheduler.types
        self.client = ClientHandler(self.scheduler.CloudSchedulerClient).get()
        self.exceptions = ModuleHandler("google.api_core.exceptions").please_import(
            who_is_calling="CloudScheduler"
        )

    def __repr__(self):
        return f"CloudScheduler({self.name})"

    def ls(self, full_name=False):
        """
        List all jobs in the project.

        Args:
        - full_name (bool): Return full name or just the job name.

        Returns:
        - List[google.cloud.scheduler_v1.Job]: List of jobs.
        """
        jobs = self.client.list_jobs(parent=self.parent)
        if full_name:
            output = [x.name for x in jobs]
        else:
            output = [x.name.split("/")[-1] for x in jobs]
        return output

    def get(self, name=None):
        """
        Get a job by name.

        Args:
        - name (str): Name of the job.

        Returns:
        - google.cloud.scheduler_v1.Job: Job object.
        """
        name = name or self.full_name
        if not name.startswith("projects/"):
            name = f"{self.parent}/jobs/{name}"
        return self.client.get_job(name=name)

    def exists(self):
        """
        Check if a job exists.

        Returns:
        - bool: True if the job exists.
        """
        try:
            self.client.get_job(name=self.full_name)
            return True
        except Exception:
            return False

    def create(
        self,
        schedule,
        target,
        time_zone="UTC",
        payload={},
        description=None,
        service_account=None,
    ):
        """
        Create a job.

        Args:
        - schedule (str): Schedule in cron format.
        - target (str): Target of the job. Can be a URL (HTTP trigger) or a Pub/Sub topic.
        - payload (str): Payload to send to the job.
        - time_zone (str): Time zone. Default is "UTC".
        - description (str): Description of the job.
        - service_account (str): Service account email. If "DEFAULT", uses the default service account PROJECT@PROJECT.iam.gserviceaccount.com.

        Returns:
        - google.cloud.scheduler_v1.Job: Job object.
        """
        oauth_token = None
        oidc_token = None
        if service_account == "DEFAULT":
            service_account = f"{self.project}@{self.project}.iam.gserviceaccount.com"
        if service_account:
            oauth_token = self.types.OAuthToken(service_account_email=service_account)
            oidc_token = self.types.OidcToken(service_account_email=service_account)
        job = self.types.Job(
            name=self.full_name,
            schedule=schedule,
            time_zone=time_zone,
            description=description,
        )
        payload = json.dumps(payload).encode("utf-8")
        if not target.startswith("http"):
            job.pubsub_target = self.types.PubsubTarget(topic_name=target, data=payload)
        else:
            http_method = self.types.HttpMethod.POST
            job.http_target = self.types.HttpTarget(
                uri=target,
                http_method=http_method,
                body=payload,
                oauth_token=oauth_token,
                oidc_token=oidc_token,
            )

        if self.exists():
            output = self.client.update_job(job=job)
            log(f"CloudScheduler - Job updated: {self.name}.")
        else:
            output = self.client.create_job(parent=self.parent, job=job)
            log(f"CloudScheduler - Job created: {self.name}.")
        return output

    def delete(self, errors="ignore"):
        """
        Delete a job.

        Args:
        - errors (str): If job does not exist, ignore or raise an error.

        Returns:
        - google.cloud.scheduler_v1.Job: Job object.
        """
        try:
            output = self.client.delete_job(name=self.full_name)
            log(f"CloudScheduler - Job deleted: {self.name}.")
            return output
        except Exception as e:
            if errors == "ignore":
                log(f"CloudScheduler - Job {self.name} does not exist to delete.")
                return None
            else:
                raise e

    def status(self):
        """
        Get the status of the job.

        Returns:
        - str: Job status.
        """
        got = self.get()
        if got.last_attempt_time is None:
            return "Has not run yet"
        status = got.status
        try:
            code = status.code
        except AttributeError:
            code = 0
        if code == 0:
            return "Success"
        elif code == 2:
            return "Failed"
        return status

    def state(self):
        """
        Get the state of the job.

        Returns:
        - str: Job state. Possible values are:
            - ENABLED: The job is enabled and can be executed.
            - PAUSED: The job is paused. It will not run.
            - UPDATE_FAILED: The job failed to update.
            - DISABLED: The job is disabled. It will not run.
        """
        got = self.get()
        state = got.state.name
        return state

    def run(self, force=True):
        """
        Run the job.

        Args:
        - force (bool): If job is paused, force run it by resuming it.

        Returns:
        - google.cloud.scheduler_v1.Job: Job object.
        """
        try:
            output = self.client.run_job(name=self.full_name)
        except self.exceptions.FailedPrecondition:
            if force:
                self.resume()
                output = self.client.run_job(name=self.full_name)
            else:
                raise
        log(f"CloudScheduler - Job ran: {self.name}.")
        return output

    def pause(self):
        """
        Pause the job.

        Returns:
        - google.cloud.scheduler_v1.Job: Job object.
        """
        output = self.client.pause_job(name=self.full_name)
        log(f"CloudScheduler - Job paused: {self.name}.")
        return output

    def resume(self):
        """
        Resume the job.

        Returns:
        - google.cloud.scheduler_v1.Job: Job object.
        """
        output = self.client.resume_job(name=self.full_name)
        log(f"CloudScheduler - Job resumed: {self.name}.")
        return output


if __name__ == "__main__":
    CloudScheduler("test-job").create(
        schedule="*/5 * * * *",
        time_zone="UTC",
        target="https://123ex123am123ple123.com",
        payload={"key": "value"},
    )
    print(CloudScheduler().ls())
    print(CloudScheduler("test-job").status())
    print(CloudScheduler("test-job").state())
    CloudScheduler("test-job").run()
    print(CloudScheduler("test-job").status())
    CloudScheduler("test-job").pause()
    print(CloudScheduler("test-job").state())
    CloudScheduler("test-job").delete()
