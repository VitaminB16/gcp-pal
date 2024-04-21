from uuid import uuid4

from gcp_pal import SecretManager


def test_secret_manager():
    success = {}
    secret_name = f"test-secret-{uuid4()}"
    secret_name2 = f"test-secret-{uuid4()}"
    success[0] = secret_name not in SecretManager().ls()
    success[1] = not SecretManager(secret_name).exists()

    SecretManager(secret_name).create({"key": "value"}, labels={"test": "test"})
    success[2] = SecretManager(secret_name).exists()
    success[3] = secret_name in SecretManager().ls()

    secret = SecretManager(secret_name).get()
    success[4] = secret.labels == {"test": "test"}
    success[5] = str(secret.replication.automatic) == ""

    value = SecretManager(secret_name).value()
    success[6] = value == {"key": "value"}

    SecretManager(secret_name2).create({"key": "value"}, labels={"test2": "test2"})
    success[7] = secret_name2 in SecretManager().ls()
    success[8] = SecretManager().exists(secret_name2)
    success[9] = SecretManager().ls(label="test2:test2") == [secret_name2]

    SecretManager(secret_name).delete()
    SecretManager(secret_name2).delete()

    success[10] = not SecretManager(secret_name).exists()
    success[11] = secret_name not in SecretManager().ls()
    success[12] = not SecretManager(secret_name2).exists()
    success[13] = secret_name2 not in SecretManager().ls()

    failed = [k for k, v in success.items() if not v]
    assert not failed
