import os
import logging
import collections.abc
from google.cloud import pubsub_v1, logging as gcp_logging
from google.cloud.logging.handlers.transports import SyncTransport


LIST_LIKE_TYPES = (list, tuple, set, frozenset, collections.abc.KeysView)

if os.getenv("PLATFORM", "GCP") in ["GCP", "local"]:
    client = gcp_logging.Client()
    handler = gcp_logging.handlers.CloudLoggingHandler(
        client, name="gcp_tools", transport=SyncTransport
    )
    client.setup_logging()


def force_list(x):
    """
    Force x to be a list
    """
    if isinstance(x, LIST_LIKE_TYPES):
        return x
    else:
        return [x]


def is_series(obj):
    """Check if an object is a pandas Series without importing pandas."""
    return type(obj).__name__ == "Series"


def is_dataframe(obj):
    """Check if an object is a pandas DataFrame without importing pandas."""
    return type(obj).__name__ == "DataFrame"


def log(*args, **kwargs):
    """
    Function for logging to Google Cloud Logs. Logs a message as usual, and logs a dictionary of data as jsonPayload.

    Arguments:
        *args (list): list of elements to "print" to google cloud logs.
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


def reverse_dict(d):
    """
    Reverse a dictionary. If the dictionary is not one-to-one, the last value in the dictionary will be the one that is kept.

    Args:
    - d (dict): The dictionary to reverse.

    Returns:
    - dict: The reversed dictionary.
    """
    return {v: k for k, v in d.items()}


def get_default_project():
    """
    Get the default project from google.auth.default() and store it in an environment variable.

    Returns:
    - str: The default project.
    """
    import google.auth

    project = os.getenv("_GOOGLE_AUTH_DEFAULT_PROJECT", None)
    if project is None:
        project = google.auth.default()[1]
        os.environ["_GOOGLE_AUTH_DEFAULT_PROJECT"] = project
        print(f"Obtained default project: {project}")
    return project
