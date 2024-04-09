import json
import requests
from gcp_tools.utils import try_import

try_import("google.auth", "Requests")
try_import("google.oauth2", "Requests")
try_import("google.auth.transport.requests", "Requests")
import requests
import google.auth
import google.auth.exceptions
from google.oauth2 import id_token
from google.auth.transport.requests import Request as AuthRequest

from gcp_tools.utils import log


class Request:
    """
    Class for making authorized requests to a cloud run service.

    Examples:
    - `Request("https://[CLOUD_RUN_URL]").get()` -> Get request to cloud run service
    - `Request("https://[CLOUD_RUN_URL]").post({"key": "value"})` -> Post request to cloud run service
    """

    def __init__(self, url, project=None, service_account=None):
        """
        Args:
        - url (str): URL of the cloud run service
        - project (str): Project ID
        """
        self.url = url

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        self.credentials, self.project = google.auth.default(scopes=scopes)

        self.project = project or self.project

        default_sa = f"{self.project}@{self.project}.iam.gserviceaccount.com"
        self.service_account = service_account or default_sa

        self.identity_token = self.get_identity_token()
        self.headers = {
            "Authorization": f"Bearer {self.identity_token}",
            "Content-type": "application/json",
        }

    def __repr__(self):
        return f"Request({self.url})"

    def get_identity_token(self):
        # Attempt to fetch an identity token for the given URL
        auth_req = AuthRequest()
        try:
            # Ensure the credentials are valid and refreshed
            if not self.credentials.valid:
                self.credentials.refresh(auth_req)

            # The audience URL should be the URL of the cloud function or service you are accessing.
            # Make sure this matches exactly what's expected by the service.
            token = id_token.fetch_id_token(auth_req, self.url)
            return token
        except google.auth.exceptions.DefaultCredentialsError:
            log(f"Fetching credentials via access token.")
            return self._fetch_identity_access_token(auth_req)
        except google.auth.exceptions.RefreshError as e:
            log(f"Error refreshing credentials: {e}")
            return None

    def _fetch_identity_access_token(self, auth_request):
        """
        Fetches an access token from the identity token.

        Args:
        - auth_request (google.auth.transport.requests.Request): Request object

        Returns:
        - str: Access token
        """
        self.credentials.refresh(auth_request)
        access_token = self.credentials.token
        iam_url = "https://iamcredentials.googleapis.com"
        url = f"{iam_url}/v1/projects/-/serviceAccounts/{self.service_account}:generateIdToken"
        params = {"audience": self.url}
        headers = {
            "Content-type": "text/json; charset=utf-8",
            "Metadata-Flavor": "Google",
            "Authorization": f"Bearer {access_token}",
        }
        response = requests.post(url, params=params, headers=headers)
        if response.status_code == 200:
            response_json = response.json()
            return response_json.get("token")
        else:
            log(f"Error fetching identity token: {response.text}")
        return None

    def post(self, payload=None, **kwargs):
        response = requests.post(self.url, json=payload, headers=self.headers, **kwargs)
        return response

    def get(self, **kwargs):
        response = requests.get(self.url, headers=self.headers, **kwargs)
        return response

    def put(self, payload=None, **kwargs):
        response = requests.put(self.url, json=payload, headers=self.headers, **kwargs)
        return response


if __name__ == "__main__":
    uri = "https://python-roh-jfzraqzsma-nw.a.run.app"
    payload = {"task_name": "events"}
    json_payload = json.dumps(payload)
    response = Request(uri).post(json_payload)
    print(response.json())
    print(response.status_code)
