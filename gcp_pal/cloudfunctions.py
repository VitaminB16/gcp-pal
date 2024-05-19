import os
import json

from gcp_pal.utils import (
    get_auth_default,
    log,
    get_all_kwargs,
    ClientHandler,
    ModuleHandler,
)


class CloudFunctions:

    def __init__(self, name=None, project=None, location="europe-west2"):
        if isinstance(name, str) and name.startswith("projects/"):
            name = name.split("/")[-1]
        self.name = name
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.location = location
        self.parent = f"projects/{self.project}/locations/{self.location}"
        self.location_id = self.parent
        self.function_id = self.name or ""
        if not self.function_id.startswith(self.parent):
            self.function_id = f"{self.parent}/functions/{self.name}"
        if self.name == self.function_id:
            self.name = self.function_id.split("/")[-1]

        self.functions = ModuleHandler("google.cloud").please_import(
            "functions_v2", who_is_calling="CloudFunctions"
        )
        self.client = ClientHandler(self.functions.FunctionServiceClient).get()

    def __repr__(self):
        return f"CloudFunctions({self.name})"

    def ls(self, active_only=False, full_id=False):
        """
        Lists all cloud functions in the project.

        Args:
        - active_only (bool): Whether to only list active cloud functions.

        Returns:
        - (list) List of cloud functions.
        """
        parent = f"projects/{self.project}/locations/{self.location}"
        request = self.functions.ListFunctionsRequest(parent=parent)
        page_result = self.client.list_functions(request)
        if active_only:
            output = [f.name for f in page_result if f.status.name == "ACTIVE"]
        else:
            output = [f.name for f in page_result]
        if not full_id:
            output = [f.split("/")[-1] for f in output]
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
        request = self.functions.GetFunctionRequest(name=function_id)
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

        from gcp_pal import Request

        uri = self.get().service_config.uri

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
        Alias for call.
        """
        return self.call(**kwargs)

    def deploy(
        self,
        path,
        entry_point,
        runtime="python310",
        environment=2,
        trigger="HTTP",
        if_exists="REPLACE",
        wait_to_complete=True,
        service_account_email=None,
        **kwargs,
    ):
        """
        Deploys a cloud function.

        Args:
        - path (str): The path to the source code.
        - entry_point (str): The name of the function to execute.
        - runtime (str): The runtime of the cloud function.
        - environment (int): The environment (generation) of the cloud function.
        - kwargs (dict): Additional arguments to pass to the cloud function. Available arguments are:
            - description (str): The description of the cloud function.
            - timeout (int): The timeout of the cloud function in seconds.
            - available_memory_mb (int): The amount of memory available to the cloud function in MB.
            - service_account_email (str): The service account email to use for the cloud function. Defaults to PROJECT@PROJECT.iam.gserviceaccount.com.
            - version_id (str): The version ID of the cloud function.
            - labels (dict): The labels to apply to the cloud function.
            - environment_variables (dict): The environment variables to set for the cloud function.
            - max_instances (int): The maximum number of instances to allow for the cloud function.
            - min_instances (int): The minimum number of instances to allow for the cloud function.

        Returns:
        - (dict) The response from the cloud function.
        """
        input_kwargs = get_all_kwargs(locals())
        if path.startswith("https://") or path.startswith("gs://"):
            return self.deploy_from_repo(**input_kwargs)
        else:
            return self.deploy_from_zip(**input_kwargs)

    def deploy_from_zip(
        self,
        path,
        entry_point,
        source_bucket=None,
        **kwargs,
    ):
        """
        Deploys a cloud function from a zip file.

        Args:
        - path (str): The path to the source code.
        - entry_point (str): The name of the function to execute.
        - source_bucket (str): The bucket to upload the zip file to. Defaults to PROJECT-cloud-functions.
        - kwargs (dict): Additional arguments to pass to the cloud function.

        Returns:
        - (dict) The response from the cloud function.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Local file not found: {path}")

        from gcp_pal import Storage
        from gcp_pal.utils import zip_directory

        log(f"Cloud Function - Creating zip file from {path} and uploading to GCS...")
        zip_path = zip_directory(path)
        # Upload the zip file to GCS
        bucket_name = source_bucket or f"{self.project}-cloud-functions"
        upload_path = f"{bucket_name}/cloud-functions/{self.name}/{self.name}.zip"
        Storage(upload_path).upload(zip_path)
        # Deploy the cloud function
        source_archive_url = Storage(upload_path).path
        return self.deploy_from_repo(
            source_archive_url, entry_point=entry_point, **kwargs
        )

    def deploy_from_repo(
        self,
        path,
        entry_point,
        trigger="HTTP",
        if_exists="REPLACE",
        environment=2,
        wait_to_complete=True,
        service_account_email=None,
        **kwargs,
    ):
        """
        Deploys a cloud function from a repository.

        Args:
        - path (str): The path to the source code.
        - entry_point (str): The name of the function to execute.
        - kwargs (dict): Additional arguments to pass to the cloud function.

        Returns:
        - (dict) The response from the cloud function.
        """

        log(f"Cloud Function - Deploying '{self.name}' from repository {path}...")
        if path.startswith("gs://"):
            from gcp_pal import Storage

            obj = Storage(path)
            bucket_name, file_name = obj.bucket_name, obj.file_name
            storage_source = self.functions.StorageSource(
                bucket=bucket_name, object=file_name
            )
            source = self.functions.Source(storage_source=storage_source)
        else:
            source = self.functions.Source(
                repository=self.functions.RepoSource(url=path)
            )
        if service_account_email is None:
            default_account = f"{self.project}@{self.project}.iam.gserviceaccount.com"
            service_account_email = default_account
        environment = self.functions.Environment(environment)
        all_kwargs = get_all_kwargs(locals())
        function_exists = self.exists()
        function_kwargs, service_kwargs, build_kwargs = self._split_deploy_kwargs(
            all_kwargs
        )
        kwargs = {k: v for k, v in kwargs.items() if k not in all_kwargs}
        build_config = self.functions.BuildConfig(**build_kwargs)
        service_config = self.functions.ServiceConfig(**service_kwargs)
        cloud_function = self.functions.Function(
            name=self.function_id,
            build_config=build_config,
            service_config=service_config,
            **function_kwargs,
            **kwargs,
        )
        if function_exists and if_exists == "REPLACE":
            log(f"Cloud Function - Updating '{self.name}'...")
            request = self.functions.UpdateFunctionRequest(
                function=cloud_function, update_mask=None
            )
            output = self.client.update_function(request)
        else:
            print(f"Cloud Function - Creating '{self.name}'...")
            request = self.functions.CreateFunctionRequest(
                function=cloud_function, parent=self.parent, function_id=self.name
            )
            output = self.client.create_function(request)
        self._handle_deploy_response(output, wait_to_complete)
        return output

    def state(self):
        """
        Returns the state of the cloud function.

        Returns:
        - (str) The state of the cloud function.
        """
        function = self.get()
        return function.state.name

    def status(self):
        """
        Alias for state.
        """
        return self.state()

    def delete(self, wait_to_complete=True, errors="ignore"):
        """
        Deletes a cloud function.

        Args:
        - wait_to_complete (bool): Whether to wait for the deletion to complete.
        - errors (str): How to handle errors. Options are "ignore", "raise" or "log". Defaults to "ignore".

        Returns:
        - (dict) The response from the delete request.
        """
        request = self.functions.DeleteFunctionRequest(name=self.function_id)
        try:
            output = self.client.delete_function(request)
            if wait_to_complete:
                output = output.result(timeout=300)
            print(f"Cloud Function - Deleted '{self.name}'.")
        except Exception as e:
            if errors == "raise":
                raise e
            elif errors == "log":
                log(f"Cloud Function - Error deleting: {e}")
            output = None
        return output

    def _handle_deploy_response(self, response, wait_to_complete):
        """
        Handles the response from a deploy request.

        Args:
        - response (dict): The response from the deploy request.
        - wait_to_complete (bool): Whether to wait for the deployment to complete.

        Returns:
        - (dict) The response from the deploy request.
        """
        output = response
        if wait_to_complete:
            log("Cloud Function - Waiting for the deployment to complete...")
            output.result(timeout=300)
        # Check that the function was deployed and is active
        function = self.get()
        service_config = function.service_config
        print(f"Cloud Function - '{self.name}': {function.state.name}.")
        if wait_to_complete:
            print(f"Version: {service_config.revision}")
            print(f"URI: {service_config.uri}")
        return response

    def _split_deploy_kwargs(self, kwargs):
        """
        Splits the deploy kwargs into function, service and build kwargs.

        Returns:
        - (dict, dict, dict) The function, service and build kwargs.
        """
        function_kwargs = {}
        service_kwargs = {}
        build_kwargs = {}
        for key, value in kwargs.items():
            if key in self.functions.Function.__annotations__:
                function_kwargs[key] = value
            elif key in self.functions.ServiceConfig.__annotations__:
                service_kwargs[key] = value
            elif key in self.functions.BuildConfig.__annotations__:
                build_kwargs[key] = value
        return function_kwargs, service_kwargs, build_kwargs


if __name__ == "__main__":
    CloudFunctions("test_sample1").deploy(
        path="samples/cloud_function",
        entry_point="entry_point",
        runtime="python310",
        environment=2,
    )
    exit()
