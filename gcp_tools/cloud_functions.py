import os

from gcp_tools.utils import get_default_project, try_import

try_import("google.cloud.functions_v1", "CloudFunctions")
import google.cloud.functions_v1 as functions_v1


class CloudFunctions:

    _clients = {}

    def __init__(self, name=None, project=None, location="europe-west2"):
        self.name = name
        self.project = project or os.environ.get("PROJECT") or get_default_project()
        self.location = location

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
        name = name or self.name
        request = functions_v1.GetFunctionRequest(name=name)
        return self.client.get_function(request)

    def call(self, data=None):
        """
        Calls a cloud function.

        Args:
        - data (dict): The data to send to the cloud function.

        Returns:
        - (dict) The response from the cloud function.
        """
        request = functions_v1.CallFunctionRequest(name=self.name, data=data)
        print(f"Sending request to cloud function {self.name}...")
        return self.client.call_function(request)


if __name__ == "__main__":
    active_cloud_functions = CloudFunctions().ls(active_only=True)
    cf_name = active_cloud_functions[0]
    payload = {}
    print(CloudFunctions(cf_name).call(payload))
