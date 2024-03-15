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
    - Request("https://[CLOUD_RUN_URL]").get() -> Get request to cloud run service
    - Request("https://[CLOUD_RUN_URL]").post({"key": "value"}) -> Post request to cloud run service
    """

    def __init__(self, url, project=None):
        self.url = url

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        self.credentials, self.project = google.auth.default(scopes=scopes)
        self.project = project or self.project

        self.identity_token = self.get_identity_token()
        self.headers = {
            "Authorization": f"Bearer {self.identity_token}",
            "Content-type": "application/json",
        }

    def get_identity_token(self):
        # Attempt to fetch an identity token for the given URL
        try:
            # Ensure the credentials are valid and refreshed
            if not self.credentials.valid:
                self.credentials.refresh(AuthRequest())

            # The audience URL should be the URL of the cloud function or service you are accessing.
            # Make sure this matches exactly what's expected by the service.
            token = id_token.fetch_id_token(AuthRequest(), self.url)
            return token
        except google.auth.exceptions.RefreshError as e:
            log(f"Error refreshing credentials: {e}")
            return None
        except Exception as e:
            log(f"Error obtaining identity token: {e}")
            return None

    def post(self, payload, **kwargs):
        response = requests.post(self.url, json=payload, headers=self.headers, **kwargs)
        return response

    def get(self, **kwargs):
        response = requests.get(self.url, headers=self.headers, **kwargs)
        return response

    def put(self, payload, **kwargs):
        response = requests.put(self.url, json=payload, headers=self.headers, **kwargs)
        return response


if __name__ == "__main__":
    import os

    cloud_run_url = os.environ.get("CLOUD_RUN_URL")
    payload = {"key": "value"}
    response = Request(cloud_run_url).post(payload)
    print(response.json())
    print(response.status_code)
