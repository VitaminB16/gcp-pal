import os
import json

from gcp_tools.utils import try_import

try_import("google.cloud.functions_v1", "CloudFunctions")
import google.cloud.functions_v1 as functions_v1
from google.cloud.functions_v1.types import CloudFunction

from gcp_tools.utils import get_default_project, log


class CloudFunctions:

    _clients = {}

    def __init__(self, name=None, project=None, location="europe-west2"):
        self.name = name
        self.project = project or os.environ.get("PROJECT") or get_default_project()
        self.location = location
        self.parent = f"projects/{self.project}/locations/{self.location}"
        self.location_id = self.parent
        self.function_id = f"{self.parent}/functions/{self.name}"

        if self.project in self._clients:
            self.client = self._clients[self.project]
        else:
            self.client = functions_v1.CloudFunctionsServiceClient()
            self._clients[self.project] = self.client

    def __repr__(self):
        return f"CloudFunctions({self.name})"

    def ls(self, active_only=False):
        """
        Lists all cloud functions in the project.

        Args:
        - active_only (bool): Whether to only list active cloud functions.

        Returns:
        - (list) List of cloud functions.
        """
        parent = f"projects/{self.project}/locations/{self.location}"
        request = functions_v1.ListFunctionsRequest(parent=parent)
        page_result = self.client.list_functions(request)
        if active_only:
            output = [f.name for f in page_result if f.status.name == "ACTIVE"]
        else:
            output = [f.name for f in page_result]
        return output

    def get(self, name=None):
        """
        Gets a cloud function.

        Args:
        - name (str): The name of the cloud function.

        Returns:
        - (dict) The cloud function.
        """
        if name:
            function_id = f"{self.parent}/functions/{name}"
        else:
            function_id = self.function_id
        request = functions_v1.GetFunctionRequest(name=function_id)
        output = self.client.get_function(request)
        return output

    def exists(self):
        """
        Checks if a cloud function exists.

        Returns:
        - (bool) True if the cloud function exists, False otherwise.
        """
        try:
            self.get()
            return True
        except Exception as e:
            return False

    def call(self, data=None):
        """
        Calls a cloud function.

        Args:
        - data (dict|str): The data to send to the cloud function. If a dict is provided, it will be converted to a JSON string.

        Returns:
        - (dict) The response from the cloud function.
        """
        if data is None:
            data = {}
        payload = json.dumps(data) if isinstance(data, dict) else data
        request = functions_v1.CallFunctionRequest(name=self.name, data=payload)
        print(f"Sending request to cloud function {self.name}...")
        result = self.client.call_function(request)
        return result

    def deploy(
        self,
        source,
        entry_point,
        runtime,
        if_exists="REPLACE",
        **kwargs,
    ):
        """
        Deploys a cloud function.

        Args:
        - source (str): The path to the source code.
        - entry_point (str): The name of the function to execute.
        - runtime (str): The runtime of the cloud function.
        - generation (int): The generation of the cloud function.
        - kwargs (dict): Additional arguments to pass to the cloud function. Available arguments are:
            - description (str): The description of the cloud function.
            - timeout (int): The timeout of the cloud function in seconds.
            - available_memory_mb (int): The amount of memory available to the cloud function in MB.
            - service_account_email (str): The service account email to use for the cloud function.
            - version_id (str): The version ID of the cloud function.
            - labels (dict): The labels to apply to the cloud function.
            - environment_variables (dict): The environment variables to set for the cloud function.
            - max_instances (int): The maximum number of instances to allow for the cloud function.
            - min_instances (int): The minimum number of instances to allow for the cloud function.


        Returns:
        - (dict) The response from the cloud function.
        """
        input_args = {
            "name": self.name,
            "runtime": runtime,
            "entry_point": entry_point,
            "source": source,
            "if_exists": if_exists,
            **kwargs,
        }
        if source.startswith("https://") or source.startswith("gs://"):
            return self.deploy_from_repo(**input_args)
        else:
            return self.deploy_from_zip(**input_args)

    def deploy_from_zip(
        self,
        source,
        entry_point,
        **kwargs,
    ):
        """
        Deploys a cloud function from a zip file.

        Args:
        - source (str): The path to the source code.
        - entry_point (str): The name of the function to execute.
        - kwargs (dict): Additional arguments to pass to the cloud function.

        Returns:
        - (dict) The response from the cloud function.
        """
        if not os.path.exists(source):
            raise FileNotFoundError(f"Local file not found: {source}")

        from gcp_tools import Storage
        from gcp_tools.utils import zip_directory

        log(f"Creating zip file from {source} and uploading to GCS...")
        zip_path = zip_directory(source)
        # Upload the zip file to GCS
        bucket_name = f"{self.project}-cloud-functions"
        upload_path = f"{bucket_name}/cloud-functions/{self.name}/{self.name}.zip"
        Storage(upload_path).upload(zip_path)
        # Deploy the cloud function
        source_archive_url = Storage(upload_path).path
        return self.deploy_from_repo(
            source_archive_url, entry_point=entry_point, **kwargs
        )

    def deploy_from_repo(
        self,
        source,
        entry_point,
        trigger="HTTP",
        https_trigger=None,
        event_trigger=None,
        if_exists="REPLACE",
        **kwargs,
    ):
        """
        Deploys a cloud function from a repository.

        Args:
        - source (str): The path to the source code.
        - entry_point (str): The name of the function to execute.
        - kwargs (dict): Additional arguments to pass to the cloud function.

        Returns:
        - (dict) The response from the cloud function.
        """
        log(f"Deploying cloud function '{self.name}' from repository {source}...")
        function_exists = self.exists()
        if https_trigger or trigger == "HTTP":
            https_trigger = functions_v1.HttpsTrigger(url=https_trigger)
            kwargs["https_trigger"] = https_trigger
            kwargs.pop("event_trigger", None)
        elif event_trigger or trigger == "EVENT":
            event_trigger = functions_v1.EventTrigger(event_type=event_trigger)
            kwargs["event_trigger"] = event_trigger
            kwargs.pop("https_trigger", None)
        cloud_function = CloudFunction(
            name=self.function_id,
            entry_point=entry_point,
            source_archive_url=source,
            **kwargs,
        )
        if function_exists and if_exists == "REPLACE":
            log(f"Updating existing cloud function '{self.name}'...")
            operation = self.client.update_function(function=cloud_function)
        else:
            print(f"Deploying cloud function '{self.name}'...")
            operation = self.client.create_function(
                location=self.location_id,
                function=cloud_function,
            )
        output = operation.result()

        # Check that the function was deployed and is active
        function = self.get()
        print(
            f"Cloud function '{self.name}' was deployed. Status: {function.status.name}, Version: {function.version_id}. URL: {function.https_trigger.url}"
        )
        return output


if __name__ == "__main__":
    CloudFunctions("sample").deploy(
        "samples/cloud_function", "entry_point", runtime="python310"
    )
    function = CloudFunctions("sample").get()
    print("--" * 50)
    print(function)
