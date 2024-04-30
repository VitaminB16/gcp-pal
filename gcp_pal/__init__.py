from gcp_pal.utils import LazyLoader


# Define lazy-loaded modules and classes
BigQuery = LazyLoader("gcp_pal.bigquery", "BigQuery")
CloudFunctions = LazyLoader("gcp_pal.cloudfunctions", "CloudFunctions")
CloudRun = LazyLoader("gcp_pal.cloudrun", "CloudRun")
CloudScheduler = LazyLoader("gcp_pal.cloudscheduler", "CloudScheduler")
Firestore = LazyLoader("gcp_pal.firestore", "Firestore")
PubSub = LazyLoader("gcp_pal.pubsub", "PubSub")
Logging = LazyLoader("gcp_pal.pylogging", "Logging")
Request = LazyLoader("gcp_pal.request", "Request")
Schema = LazyLoader("gcp_pal.schema", "Schema")
Storage = LazyLoader("gcp_pal.storage", "Storage")
Parquet = LazyLoader("gcp_pal.storage", "Parquet")
SecretManager = LazyLoader("gcp_pal.secretmanager", "SecretManager")
Project = LazyLoader("gcp_pal.project", "Project")
Dataplex = LazyLoader("gcp_pal.dataplex", "Dataplex")
