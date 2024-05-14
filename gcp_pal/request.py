from gcp_pal.utils import log, ModuleHandler


class Request:
    """
    Class for making authorized requests to a cloud run service.

    Examples:
    - `Request("https://[CLOUD_RUN_URL]").get()` -> Get request to cloud run service
    - `Request("https://[CLOUD_RUN_URL]").post({"key": "value"})` -> Post request to cloud run service
    """

    _identity_token = None

    def __init__(self, url, project=None, service_account=None):
        """
        Args:
        - url (str): URL of the cloud run service
        - project (str): Project ID
        """
        self.url = url

        self.requests = ModuleHandler("requests").please_import(who_is_calling="Request")
        self.google_auth = ModuleHandler("google.auth").please_import(
            who_is_calling="Request"
        )
        self.id_token = ModuleHandler("google.oauth2").please_import(
            "id_token", who_is_calling="Request"
        )
        self.AuthRequest = ModuleHandler(
            "google.auth.transport.requests"
        ).please_import("Request", who_is_calling="Request")
        self.exceptions = ModuleHandler("google.auth.exceptions").please_import(
            who_is_calling="Request"
        )

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        self.credentials, self.project = self.google_auth.default(scopes=scopes)

        self.project = project or self.project

        default_sa = f"{self.project}@{self.project}.iam.gserviceaccount.com"
        self.service_account = service_account or default_sa

        self.identity_token = self.get_identity_token()
        self.headers = {
            "Authorization": f"Bearer {self.identity_token}",
            "Content-type": "application/json",
        }
        self.args = {}

    def __repr__(self):
        return f"Request({self.url})"

    def get_identity_token(self):
        # Attempt to fetch an identity token for the given URL
        auth_req = self.AuthRequest()
        try:
            # Ensure the credentials are valid and refreshed
            if not self.credentials.valid:
                self.credentials.refresh(auth_req)

            # The audience URL should be the URL of the cloud function or service you are accessing.
            # Make sure this matches exactly what's expected by the service.
            token = self.id_token.fetch_id_token(auth_req, self.url)
            return token
        except self.exceptions.DefaultCredentialsError:
            log(f"Request - Fetching credentials via access token.")
            return self._fetch_identity_access_token(auth_req)
        except self.exceptions.RefreshError as e:
            log(f"Request - Error refreshing credentials: {e}")
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
        response = self.requests.post(url, params=params, headers=headers)
        if response.status_code == 200:
            response_json = response.json()
            return response_json.get("token")
        else:
            if "iam.serviceAccounts.getOpenIdToken" in response.text:
                role = "Service Account Token Creator"
                acc = self.service_account
                log(
                    f"Request - Error: Ensure the service account {acc} has the '{role}' role."
                )
            else:
                log(f"Request - Error fetching identity token: {response.text}")
        return None

    def post(self, payload=None, **kwargs):
        arg_name = "data" if isinstance(payload, dict) else "json"
        self.args = {arg_name: payload, "headers": self.headers, **kwargs}
        response = self.requests.post(self.url, **self.args)
        return response

    def get(self, **kwargs):
        response = self.requests.get(self.url, headers=self.headers, **kwargs)
        return response

    def put(self, payload=None, **kwargs):
        arg_name = "data" if isinstance(payload, dict) else "json"
        self.args = {arg_name: payload, "headers": self.headers, **kwargs}
        response = self.requests.put(self.url, **self.args)
        return response


if __name__ == "__main__":
    uri = "https://python-roh-jfzraqzsma-nw.a.run.app"
    payload = {"task_name": "events"}
    response = Request(uri).post(payload)
    print(response.json())
    print(response.status_code)
