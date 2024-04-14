import os
import json
import random
from uuid import uuid4

from gcp_tools.utils import try_import

try_import("google.cloud.run_v2", "CloudRun")
from google.cloud import run_v2
from google.cloud.run_v2 import Service, TrafficTarget, Condition, Job
from google.cloud.run_v2.types import (
    EnvVar,
    ResourceRequirements,
    UpdateServiceRequest,
    UpdateJobRequest,
    CreateServiceRequest,
    CreateJobRequest,
)
from google.protobuf import field_mask_pb2

from gcp_tools.pydocker import Docker
from gcp_tools.utils import get_auth_default, log


class CloudRun:

    _client = {}
    _jobs_client = {}

    def __init__(self, name=None, project=None, location="europe-west2"):
        self.project = project or os.getenv("PROJECT") or get_auth_default()[1]
        self.location = location
        self.parent = f"projects/{self.project}/locations/{self.location}"
        self.name = name
        self.full_name = f"{self.parent}/services/{self.name}"
        self.image_url = None

        if self.project in CloudRun._client:
            self.client = CloudRun._client[self.project]
            self.jobs_client = CloudRun._jobs_client[self.project]
        else:
            self.client = run_v2.ServicesClient()
            self.jobs_client = run_v2.JobsClient()
            CloudRun._client[self.project] = self.client
            CloudRun._jobs_client[self.project] = self.jobs_client

    def ls_jobs(self, active_only=False, full_id=False):
        """
        List all jobs in the project.
        """
        jobs = self.jobs_client.list_jobs(parent=self.parent)
        if active_only:
            jobs = [f for f in jobs if f.terminal_condition.type_ == "Ready"]
        output = [x.name for x in jobs]
        if not full_id:
            output = [f.split("/")[-1] for f in output]
        return output

    def ls_services(self, active_only=False, full_id=False):
        """
        List all services in the project.
        """
        services = self.client.list_services(parent=self.parent)
        if active_only:
            services = [
                f for f in services if all([x.percent > 0 for x in f.traffic_statuses])
            ]
        output = [x.name for x in services]
        if not full_id:
            output = [f.split("/")[-1] for f in output]
        return output

    def ls(self, active_only=False, full_id=False, jobs=False):
        """
        List all services or jobs in the project.

        Args:
        - active_only (bool): Only list active services or jobs. Default is False.
        - full_id (bool): Return full resource IDs, e.g. 'projects/my-project/locations/europe-west2/services/my-service'. Default is False.
        - jobs (bool): List jobs instead of services. Default is False.

        Returns:
        - (list): A list of service or job names.
        """
        if jobs:
            return self.ls_jobs(active_only=active_only, full_id=full_id)
        return self.ls_services(active_only=active_only, full_id=full_id)

    def get(self, job=False):
        """
        Get a service or job by name.

        Args:
        - job (bool): Get a job instead of a service. Default is False.

        Returns:
        - (Service) or (Job): The service or job object.
        """
        if job:
            return self.jobs_client.get_job(name=self.full_name)
        return self.client.get_service(name=self.full_name)

    def exists(self, job=False):
        """
        Check if a service or job exists.

        Args:
        - job (bool): Check if a job exists instead of a service. Default is False.

        Returns:
        - (bool): True if the service or job exists, False otherwise.
        """
        try:
            self.get(job=job)
            return True
        except:
            return False

    def build_and_push_docker_image(
        self, path=".", image_tag="latest", dockerfile="Dockerfile"
    ):
        """
        Build and push a Docker image to GCR.

        Args:
        - path (str): The path to the build context.
        - image_tag (str): The tag to apply to the image.
        - dockerfile (str): The path to the Dockerfile from the context.

        Returns:
        - (str): The GCR location of the image (e.g. 'gcr.io/my-project/my-image:latest')
        """
        docker = Docker(name=self.name, project=self.project, tag=image_tag)
        gcr_location = docker.build_and_push(path=path, dockerfile=dockerfile)
        self.image_url = gcr_location
        if gcr_location is None:
            raise ValueError("Failed to build and push Docker image.")
        return gcr_location

    def deploy_service(
        self,
        image_url=None,
        env_vars_file=None,
        memory="512Mi",
        min_instances=0,
        max_instances=0,
        container_kwargs={},
        service_kwargs={},
        wait_to_complete=True,
    ):
        """
        Deploy a Cloud Run service using a Docker image.
        Args:
        - env_vars_file (str): YAML file path for environment variables.
        - memory (str): Amount of memory to allocate to each service instance.
        - min_instances (int): Minimum number of instances.
        - max_instances (int): Maximum number of instances. Set to 0 for auto-scaling.
        - container_kwargs (dict): Additional container configuration.
        - service_kwargs (dict): Additional service configuration.
        """
        image_url = image_url or self.image_url
        # gcloud run deploy seems to set HOST=0.0.0.0 by default?
        default_env_vars = {"HOST": "0.0.0.0"}
        service = Service(
            template={
                "containers": [
                    {
                        "image": image_url,
                        "env": self._parse_env_vars(
                            env_vars_file, default=default_env_vars
                        ),
                        "resources": ResourceRequirements(limits={"memory": memory}),
                        **container_kwargs,
                    }
                ],
                "scaling": {
                    "min_instance_count": min_instances,
                    "max_instance_count": max_instances,
                },
            },
            **service_kwargs,
        )
        service_exists = self.exists()
        if not service_exists:
            log(f"Cloud Run - Creating service '{self.name}'...")
            args = {"parent": self.parent, "service": service, "service_id": self.name}
            request = CreateServiceRequest(**args)
            response = self.client.create_service(request=request)
        else:
            log(f"Cloud Run - Updating service '{self.name}'...")
            # Add 'name' to service.template.containers
            service.name = self.full_name
            request = UpdateServiceRequest(service=service)
            response = self.client.update_service(request=request)
        if wait_to_complete:
            log(f"Cloud Run - Waiting for service '{self.name}' to complete...")
            response.result(timeout=300)
        return response

    def deploy_job(
        self,
        image_url=None,
        env_vars_file=None,
        memory="512Mi",
        container_kwargs={},
        job_kwargs={},
    ):
        """
        Deploy a Cloud Run job using a Docker image.
        Args:
        - env_vars_file (str): YAML file path for environment variables.
        - memory (str): Amount of memory to allocate to the job.
        - container_kwargs (dict): Additional container configuration.
        - job_kwargs (dict): Additional job configuration.
        """
        image_url = image_url or self.image_url
        job = Job(
            name=self.full_name,
            template={
                "containers": [
                    {
                        "image": image_url,
                        "env": self._parse_env_vars(env_vars_file),
                        "resources": ResourceRequirements(limits={"memory": memory}),
                        **container_kwargs,
                    }
                ]
            },
            **job_kwargs,
        )
        response = self.jobs_client.create_job(parent=self.parent, job=job)
        return response

    def call(
        self,
        data={},
        errors="ignore",
        to_json=True,
    ):
        """
        Calls a cloud function.

        Args:
        - data (dict|str): The data to send to the cloud function. If a dict, it will be converted to JSON. Defaults to {}.
        - errors (str): How to handle errors. Options are "ignore", "raise" or "log". Defaults to "ignore".
        - to_json (bool): Whether to convert the response to JSON. Defaults to True.

        Returns:
        - (dict) The response from the cloud function.
        """
        if isinstance(data, dict):
            data = json.dumps(data)

        from gcp_tools import Request

        uri = self.get().uri

        output = Request(uri).post(data)
        if output.status_code != 200:
            msg = f"Cloud Function - Error calling '{self.name}': {output.text}"
            if errors == "raise":
                raise Exception(msg)
            elif errors == "log":
                log(msg)
        if to_json:
            try:
                output = output.json()
            except json.JSONDecodeError:
                pass
        return output

    def invoke(self, **kwargs):
        """
        Alias for call method.
        """
        return self.call(**kwargs)

    def _parse_env_vars(self, yaml_file, default={}):
        """
        Parse YAML file to extract environment variables as a list of EnvVar objects.
        """
        from gcp_tools.utils import load_yaml

        if yaml_file is None:
            return []
        data = load_yaml(yaml_file)
        data = {**default, **data}
        return [EnvVar(name=k, value=str(v)) for k, v in data.items()]

    def deploy(
        self,
        path=".",
        job=False,
        image_tag="random",
        dockerfile="Dockerfile",
        **kwargs,
    ):
        """
        Deploy a Cloud Run service or job.

        Args:
        - path (str): The path to the build context or the image URL.
        - job (bool): Deploy a job instead of a service. Default is False.
        - image_tag (str): The tag to apply to the image. Default is 'random', which will generate a unique tag.
        - dockerfile (str): The path to the Dockerfile from the context.
        - kwargs: Additional arguments to pass to the deploy_service or deploy_job method.

        Returns:
        - (Service) or (Job): The service or job object.
        """
        if image_tag == "random":
            random_tag = random.getrandbits(64)
            image_tag = f"{random_tag:x}"
        if (
            path.startswith("gs:")
            or path.startswith("http")
            or path.startswith("gcr.io/")
        ):
            log(f"Cloud Run - Deploying {path} directly.")
            image_url = path
        else:
            log(f"Cloud Run - Building and pushing Docker image from {path}.")
            image_url = self.build_and_push_docker_image(
                path=path, image_tag=image_tag, dockerfile=dockerfile
            )
        if job:
            return self.deploy_job(image_url, **kwargs)
        else:
            return self.deploy_service(image_url, **kwargs)


if __name__ == "__main__":
    CloudRun("test-app").deploy(path="samples/cloud_run")
    output = CloudRun("test-app").call(data={"data": 16})
    print(output)
