import os
import time
import datetime

from gcp_tools.utils import try_import

try_import("google.cloud.logging", "Logging")
from google.cloud import logging

from gcp_tools.utils import get_auth_default, log


class LogEntry:
    def __init__(self, project, log_name, resource, severity, message, timestamp):
        self.project = project
        self.log_name = log_name
        self.resource = resource
        self.severity = severity
        self.message = message
        self.timestamp = timestamp
        self.time_zone = timestamp.tzinfo
        self.timestamp_str = (
            timestamp.isoformat(sep=" ", timespec="milliseconds").split("+")[0]
            + f" {self.time_zone}"
        )

    def to_dict(self):
        return {
            "project": self.project,
            "log_name": self.log_name,
            "resource": self.resource,
            "severity": self.severity,
            "message": self.message,
            "timestamp": self.timestamp,
            "timestamp_str": self.timestamp_str,
        }

    def to_api_repr(self):
        return {
            "project": self.project,
            "log_name": self.log_name,
            "resource": self.resource,
            "severity": self.severity,
            "message": self.message,
            "timestamp": self.timestamp,
            "timestamp_str": self.timestamp_str,
        }

    def __str__(self):
        return f"LogEntry - [{self.timestamp_str}] {self.message}"

    def __repr__(self):
        return f"LogEntry - [{self.timestamp_str}] {self.message}"


class Logging:

    _client = {}

    def __init__(self, project=None):
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.filters = []

        if self.project in Logging._client:
            self.client = Logging._client[self.project]
        else:
            self.client = logging.Client(project=self.project)
            Logging._client[self.project] = self.client

    def ls(
        self,
        filter=None,
        severity=None,
        limit=None,
        time_start=None,
        time_end=None,
        time_range=None,
        order_by="timestamp desc",
    ):
        """
        List all logs in a project

        Args:
        - filter (str): Filter results
        - severity (str): Severity level
        - limit (int): Number of logs to return
        - time_start (datetime.datetime): Start time for logs
        - time_end (datetime.datetime): End time for logs
        - time_range (int): Time range in hours. Can't be used with time_start and time_end.
        - order_by (str): Order logs by ascending or descending. Default is desc.

        Returns:
        - (list[dict]): List of log entries
        """
        if time_range:
            time_end = datetime.datetime.now(datetime.timezone.utc)
            time_start = time_end - datetime.timedelta(hours=time_range)
        filter = self._generate_filter(filter, severity, time_start, time_end)
        log(f"Logging - Filter: {filter}")
        logs = self.client.list_entries(
            filter_=filter, max_results=limit, order_by=order_by
        )
        output = []
        for log_entry in logs:
            output.append(
                LogEntry(
                    project=log_entry.resource.labels["project_id"],
                    log_name=log_entry.log_name,
                    resource=log_entry.resource,
                    severity=log_entry.severity,
                    message=log_entry.payload,
                    timestamp=log_entry.timestamp,
                )
            )
        return output

    def stream(self, filter=None, severity=None, time_start=None, interval=5):
        """
        Stream logs in a project in real-time by polling the log entries.

        Args:
        - filter (str): Filter results. Default is None.
        - severity (str): Severity level. Default is None.
        - time_start (datetime.datetime): Start time for logs. Default is now.
        - interval (int): Polling interval in seconds. Default is 5 seconds.

        Yields:
        - (LogEntry): Yield log entries as they are found.
        """
        if time_start is None:
            time_start = datetime.datetime.now(datetime.timezone.utc)

        last_end_time = time_start

        while True:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            # Buffer to account for GCP log latency
            log_latency = datetime.timedelta(seconds=5)
            time_end = current_time - log_latency
            if time_end <= last_end_time:
                time.sleep(interval)  # Wait until window is positive
                continue

            log_filter = self._generate_filter(
                filter, severity, last_end_time.isoformat(), time_end.isoformat()
            )
            logs = self.client.list_entries(filter_=log_filter)

            for log_entry in logs:
                le = LogEntry(
                    project=log_entry.resource.labels["project_id"],
                    log_name=log_entry.log_name,
                    resource=log_entry.resource,
                    severity=log_entry.severity,
                    message=log_entry.payload,
                    timestamp=log_entry.timestamp,
                )
                print(le)

            last_end_time = time_end  # Shift the time window
            time.sleep(interval)

    def _generate_filter(
        self, filter=None, severity=None, time_start=None, time_end=None
    ):
        if filter:
            self.filters.append(filter)
        if severity:
            filter_str = f"severity={severity}"
            self.filters.append(filter_str)
        if time_start:
            if not isinstance(time_start, str):
                time_start = time_start.isoformat()
            filter_str = f'timestamp>="{time_start}"'
            self.filters.append(filter_str)
        if time_end:
            if not isinstance(time_end, str):
                time_end = time_end.isoformat()
            filter_str = f'timestamp<="{time_end}"'
            self.filters.append(filter_str)
        filter = " AND ".join(self.filters)
        self.filters = []  # Reset filters
        return filter


if __name__ == "__main__":
    logs = Logging().ls(time_range=1, limit=10)
    for log_entry in logs:
        print(log_entry)
