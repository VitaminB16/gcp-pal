import os
import json

from gcp_pal.utils import try_import

try_import("google.cloud.secretmanager", "SecretManager")
try_import("google.api_core.exceptions", "SecretManager")
from google.cloud import secretmanager
import google.api_core.exceptions

from gcp_pal.utils import get_auth_default, log


class SecretManager:

    _client = {}

    def __init__(self, name=None, project=None):
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.parent = f"projects/{self.project}"
        if isinstance(name, str) and name.startswith("projects/"):
            name = name.split("/")[-1]
        self.name = name
        self.full_name = f"{self.parent}/secrets/{self.name}"

        if self.project in SecretManager._client:
            self.client = SecretManager._client[self.project]
        else:
            self.client = secretmanager.SecretManagerServiceClient()
            SecretManager._client[self.project] = self.client

    def __repr__(self):
        return f"SecretManager({self.name})"

    def ls(self, label=None, filter=None, full_name=False):
        """
        List all secrets in the project.

        Args:
        - label (str): Filter by label.
        - filter (str): Filter by filter string. Can't be used with `label`.
        - full_name (bool): Return full name or just the secret name.

        Returns:
        - List[google.cloud.secretmanager.Secret]: List of secrets.
        """
        filter = filter or (f"labels.{label}" if label else None)
        request = {"parent": self.parent}
        if filter:
            request["filter"] = filter
        secrets = self.client.list_secrets(request=request)
        if full_name:
            output = [x.name for x in secrets]
        else:
            output = [x.name.split("/")[-1] for x in secrets]
        return output

    def get(self, name=None):
        """
        Get a secret by name.

        Args:
        - name (str): Name of the secret.

        Returns:
        - google.cloud.secretmanager.Secret: Secret object.
        """
        name = name or self.full_name
        if not name.startswith("projects/"):
            name = f"{self.parent}/secrets/{name}"
        return self.client.get_secret(name=name)

    def exists(self, name=None):
        """
        Check if a secret exists.

        Args:
        - name (str): Name of the secret.

        Returns:
        - bool: True if secret exists, False otherwise.
        """
        try:
            self.get(name=name)
            return True
        except google.api_core.exceptions.NotFound:
            return False

    def create(self, value=None, labels=None, replication=None, if_exists="update"):
        """
        Create a secret.

        Args:
        - value (str): Secret value.
        - labels (dict): Labels.
        - replication (str): Replication policy.
        - if_exists (str): If secret already exists, update (add new version)
                           or replace (delete and re-create). Default is 'update'.

        Returns:
        - google.cloud.secretmanager.Secret: Secret object.
        """
        if replication is None:
            replication = {"automatic": {}}

        secret = secretmanager.Secret(
            name=self.full_name,
            replication=replication,
            labels=labels,
        )
        try:
            self.client.create_secret(
                secret=secret, parent=self.parent, secret_id=self.name
            )
            log(f"Secret Manager - Created secret {self.name}.")
        except google.api_core.exceptions.AlreadyExists:
            if if_exists == "update":
                log(
                    f"Secret Manager - Secret {self.name} already exists. Adding new version..."
                )
                pass
            elif if_exists == "replace":
                log(
                    f"Secret Manager - Secret {self.name} already exists. Deleting and recreating..."
                )
                self.delete()
                return self.create(
                    value=value,
                    labels=labels,
                    replication=replication,
                    if_exists=None,
                )
            else:
                msg = f"Secret {self.name} already exists. Use if_exists='update' or if_exists='replace'."
                raise ValueError(msg)
        value = json.dumps(value) if isinstance(value, dict) else value
        if value:
            # Encode the secret data using base64.
            secret_data = value.encode("UTF-8")
            self.client.add_secret_version(
                parent=self.full_name, payload={"data": secret_data}
            )
            log(f"Secret Manager - Added new version to secret {self.name}.")
        return self.full_name

    def delete(self, errors="ignore"):
        """
        Delete a secret.

        Returns:
        - None
        """
        try:
            output = self.client.delete_secret(name=self.full_name)
        except google.api_core.exceptions.NotFound:
            if errors == "ignore":
                log(f"Secret Manager - Secret {self.name} not found to delete.")
                return None
            else:
                raise
        log(f"Secret Manager - Deleted secret {self.name}.")
        return output

    def value(self, decode=True):
        """
        Get the value of a secret.

        Args:
        - decode (bool): Attempt to decode the secret value from JSON. Default is True.

        Returns:
        - str: Secret value.
        """
        response = self.client.access_secret_version(
            name=f"{self.full_name}/versions/latest"
        )
        output = response.payload.data.decode("UTF-8")
        if decode:
            try:
                output = json.loads(output)
            except json.JSONDecodeError:
                pass
        return output

    def get_secret_value(self, decode=True):
        """
        Alias for `value`.
        """
        return self.value(decode=decode)


if __name__ == "__main__":
    SecretManager("test_secret2").create("test_value2", labels={"env": "dev"})
    sm = SecretManager().ls(label="env:dev")
    secret_name = sm[0]
    value = SecretManager(secret_name).value()
    print(value)
    SecretManager(secret_name).delete()