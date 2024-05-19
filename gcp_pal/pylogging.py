import os
import time
import datetime

from gcp_pal.utils import try_import


from gcp_pal.utils import get_auth_default, log, ClientHandler, ModuleHandler


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
        self.message_str = self._parse_message()

    def to_dict(self):
        return {
            "project": self.project,
            "log_name": self.log_name,
            "resource": self.resource,
            "severity": self.severity,
            "message": self.message,
            "timestamp": self.timestamp,
            "timestamp_str": self.timestamp_str,
            "message_str": self.message_str,
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
            "message_str": self.message_str,
        }

    def _parse_message(self):
        if isinstance(self.message, dict) and "message" in self.message:
            return self.message["message"]
        return self.message

    def __str__(self):
        return f"LogEntry - [{self.timestamp_str}] {self.message_str}"

    def __repr__(self):
        return f"LogEntry - [{self.timestamp_str}] {self.message_str}"


class Logging:

    def __init__(self, project=None):
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.filters = []

        self.loggingClient = ModuleHandler("google.cloud").please_import(
            "logging", who_is_calling="Logging"
        )
        self.client = ClientHandler(self.loggingClient).get(project=self.project)

    def ls(
        self,
        query=None,
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
        - query (str): Query for filtering results
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
        query = self._generate_query(query, severity, time_start, time_end)
        log(f"Logging - Filter: {query}")
        if limit is None and query is None:
            limit = 100
        logs = self.client.list_entries(
            filter_=query, max_results=limit, order_by=order_by
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

    def stream(self, query=None, severity=None, time_start=None, interval=5):
        """
        Stream logs in a project in real-time by polling the log entries.

        Args:
        - query (str): Query for filtering the logs. Default is None.
        - severity (str): Severity level. Default is None.
        - time_start (datetime.datetime): Start time for logs. Default is now.
        - interval (int): Polling interval in seconds. Default is 5 seconds.

        Yields:
        - (LogEntry): Yield log entries as they are found.
        """
        if time_start is None:
            time_start = datetime.datetime.now(datetime.timezone.utc)

        last_end_time = time_start
        time_zone = last_end_time.tzinfo
        time_start_str = last_end_time.isoformat(sep=" ").replace(
            "+00:00", f" {time_zone}"
        )
        log(f"Logging - Start Time: {time_start_str}. Streaming...")

        while True:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            # Buffer to account for GCP log latency
            log_latency = datetime.timedelta(seconds=10)
            time_end = current_time - log_latency
            if time_end <= last_end_time:
                time.sleep(interval)  # Wait until window is positive
                continue

            log_filter = self._generate_query(
                query, severity, last_end_time.isoformat(), time_end.isoformat()
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

    def _generate_query(
        self, query=None, severity=None, time_start=None, time_end=None
    ):
        if query:
            self.filters.append(query)
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
        query = " AND ".join(self.filters)
        self.filters = []  # Reset filters
        query = None if query == "" else query
        return query


if __name__ == "__main__":
    Logging().stream()
