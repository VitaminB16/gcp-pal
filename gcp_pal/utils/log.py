import os
import logging

from gcp_pal.utils import try_import

err = try_import("google.cloud.logging", "logging", errors="ignore")
try_import("google.cloud.logging.handlers.transports", "logging", errors="ignore")
if err is not None:
    from google.cloud import logging as gcp_logging
    from google.cloud.logging.handlers.transports import SyncTransport

    if os.getenv("PLATFORM", "GCP") in ["GCP", "local"]:
        client = gcp_logging.Client()
        handler = gcp_logging.handlers.CloudLoggingHandler(
            client, name="gcp_pal", transport=SyncTransport
        )
        client.setup_logging()


def log(*args, **kwargs):
    """
    Function for logging to Google Cloud Logs. Logs a message as usual, and logs a dictionary of data as jsonPayload.

    Args:
        *args (list): list of elements to "print" to google cloud logs.
        **kwargs (dict): dictionary of elements to "print" to google cloud logs.

    Examples:
    >>> log("Hello, world!")
    message: "Hello, world!"
    >>> log("Hello, world!", {"a": 1, "b": 2})
    message: "Hello, world!"
    payload: {"a": 1, "b": 2}
    """

    # Use these environment variables as payload to log to Google Cloud Logs
    env_keys = ["PLATFORM"]
    env_data = {key: os.getenv(key, None) for key in env_keys}
    log_data = {k: v for k, v in env_data.items() if v is not None}

    # If any arguments are a dictionary, add it to the log_data so it can be queried in Google Cloud Logs
    for arg in args:
        if isinstance(arg, dict):
            log_data.update(arg)
        log_data["message"] = " ".join([str(a) for a in args])

    if os.getenv("PLATFORM", "Local") in ["GCP"]:
        logging.info(log_data)
    else:
        # If running locally, use a normal print
        print(log_data["message"], **kwargs)
