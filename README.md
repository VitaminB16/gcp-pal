<!--
TODO:
[x] Firestore Module
[x] PubSub Module
[x] Request Module
[x] BigQuery Module
[x] Storage Module
[x] Parquet Module
[x] Schema Module
[x] Cloud Functions Module
[x] Docker Module
[x] Cloud Run Module
[x] Logging Module
[x] Secret Manager Module
[x] Cloud Scheduler Module
[x] Add examples
[x] Publish to PyPI
[x] Tests
[x] Project Module
[x] Dataplex Module
...
-->

# GCP Pal Library

The `gcp-pal` library provides a set of utilities for interacting with Google Cloud Platform (GCP) services, streamlining the process of implementing GCP functionalities within your Python applications.

The utilities are designed to work with the `google-cloud` Python libraries, providing a more user-friendly and intuitive interface for common tasks.

- Source code: **https://github.com/VitaminB16/gcp-pal**
- PyPI: **https://pypi.org/project/gcp-pal/**

---

## Table of Contents

| Section Link | Module Class |
|--------------|-------------|
| [Installation](#installation) |  |
| [Configuration](#configuration) |  |
| [Firestore Module](#firestore-module) | `gcp_pal.Firestore` |
| [PubSub Module](#pubsub-module) | `gcp_pal.PubSub` |
| [Request Module](#request-module) | `gcp_pal.Request` |
| [BigQuery Module](#bigquery-module) | `gcp_pal.BigQuery` |
| [Storage Module](#storage-module) | `gcp_pal.Storage` |
| [Parquet Module](#parquet-module) | `gcp_pal.Parquet` |
| [Schema Module](#schema-module) | `gcp_pal.Schema` |
| [Cloud Functions Module](#cloud-functions-module) | `gcp_pal.CloudFunctions` |
| [Docker Module](#docker-module) | `gcp_pal.Docker` |
| [Cloud Run Module](#cloud-run-module) | `gcp_pal.CloudRun` |
| [Logging Module](#logging-module) | `gcp_pal.Logging` |
| [Secret Manager Module](#secret-manager-module) | `gcp_pal.SecretManager` |
| [Cloud Scheduler Module](#cloud-scheduler-module) | `gcp_pal.CloudScheduler` |
| [Project Module](#project-module) | `gcp_pal.Project` |
| [Dataplex Module](#dataplex-module) | `gcp_pal.Dataplex` |



---

## Installation

The package is available on PyPI as `gcp-pal`. To install with `pip`:

```bash
pip install gcp-pal
```

The library has no dependencies other than Python 3.10 or newer. The modules are set up to notify the user if any required libraries are missing. For example, when attempting to use the `Firestore` module:

```python
from gcp_pal import Firestore
Firestore()
# ImportError: Module 'Firestore' requires 'google.cloud.firestore' (PyPI: 'google-cloud-firestore') to be installed.
```

Which lets the user know that the `google-cloud-firestore` package is required to use the `Firestore` module.

---

## Configuration

Before you can start using the `gcp-pal` library with Firestore or any other GCP services, make sure you either have set up your GCP credentials properly or have the necessary permissions to access the services you want to use:

```bash
gcloud auth application-default login
```

And specify the project ID to be used as the default for all API requests:

```bash
gcloud config set project PROJECT_ID
```

---

## Firestore Module

The Firestore module in the `gcp-pal` library allows you to perform read and write operations on Firestore documents and collections.

### Initializing Firestore

First, import the Firestore class from the `gcp_pal` module:

```python
from gcp_pal import Firestore
```

### Writing Data to Firestore

To write data to a Firestore document, create a dictionary with your data, specify the path to your document, and use the `write` method:

```python
data = {
    "field1": "value1",
    "field2": "value2"
}

path = "collection/document"
Firestore(path).write(data)
```

### Reading Data from Firestore

To read a single document from Firestore, specify the document's path and use the `read` method:

```python
path = "collection/document"
document = Firestore(path).read()
print(document)
# Output: {'field1': 'value1', 'field2': 'value2'}
```

### Reading All Documents in a Collection

To read all documents within a specific collection, specify the collection's path and use the `read` method:

```python
path = "collection"
documents = Firestore(path).read()
print(documents)
# Output: {'document': {'field1': 'value1', 'field2': 'value2'}}
```

### Working with Pandas DataFrames

The Firestore module also supports writing and reading Pandas DataFrames, preserving their structure and data types:

```python
import pandas as pd

# Example DataFrame
df = pd.DataFrame({
    "field1": ["value1"],
    "field2": ["value2"]
})

path = "collection/document"
Firestore(path).write(df)

read_df = Firestore(path).read()
print(read_df)
# Output:
#    field1 field2
# 0  value1 value2
```

### List the Firestore documents and collections

To list all documents and collections within a Firestore database, use the `ls` method similar to bash:

```python
colls = Firestore().ls()
print(colls)
# Output: ['collection']
docs = Firestore("collection").ls()
print(docs)
# Output: ['document1', 'document2']
```

---

## PubSub Module

The PubSub module in the `gcp-pal` library allows you to publish and subscribe to PubSub topics.

### Initializing PubSub

First, import the PubSub class from the `gcp_pal` module:

```python
from gcp_pal import PubSub
```

### Publishing Messages to a Topic

To publish a message to a PubSub topic, specify the topic's name and the message you want to publish:

```python
topic = "topic-name"
message = "Hello, PubSub!"
PubSub(topic).publish(message)
```

---

## Request Module

The Request module in the `gcp-pal` library allows you to make authorized HTTP requests.

### Initializing Request

Import the Request class from the `gcp_pal` module:

```python
from gcp_pal import Request
```

### Making Authorized Get/Post/Put Requests

To make an authorized requests, specify the URL you want to access and use the relevant method:

```python
url = "https://example.com/api"

get_response = Request(url).get()
print(get_response)
# Output: <Response [200]>
post_response = Request(url).post(data={"key": "value"})
print(post_response)
# Output: <Response [201]>
put_response = Request(url).put(data={"key": "value"})
print(put_response)
# Output: <Response [200]>
```

---

## BigQuery Module

The BigQuery module in the `gcp-pal` library allows you to perform read and write operations on BigQuery datasets and tables.

### Initializing BigQuery

Import the BigQuery class from the `gcp_pal` module:

```python
from gcp_pal import BigQuery
```

### Listing objects

To list all objects (datasets and tables) within a BigQuery project, use the `ls` method similar to bash:

```python
datasets = BigQuery().ls()
print(datasets)
# Output: ['dataset1', 'dataset2']
tables = BigQuery(dataset="dataset1").ls()
print(tables)
# Output: ['table1', 'table2']
```

### Creating objects

To create an object (dataset or table) within a BigQuery project, initialize the BigQuery class with the object's path and use the `create` method:

```python
BigQuery(dataset="new-dataset").create()
# Output: Dataset "new-dataset" created
BigQuery("new-dataset2.new-table").create(schema=schema) 
# Output: Dataset "new-dataset2" created, table "new-dataset2.new-table" created
```

To create a table from a Pandas DataFrame, pass the DataFrame to the `create` method:

```python
df = pd.DataFrame({
    "field1": ["value1"],
    "field2": ["value2"]
})
BigQuery("new-dataset3.new-table").create(data=df)
# Output: Dataset "new-dataset3" created, table "new-dataset3.new-table" created, data inserted
```

### Deleting objects

Deleting objects is similar to creating them, but you use the `delete` method instead:

```python
BigQuery(dataset="dataset").delete()
# Output: Dataset "dataset" and all its tables deleted
BigQuery("dataset.table").delete()
# Output: Table "dataset.table" deleted
```

### Querying data

To read data from a BigQuery table, use the `query` method:

```python
query = "SELECT * FROM dataset.table"
data = BigQuery().query(query)
print(data)
# Output: [{'field1': 'value1', 'field2': 'value2'}]
```

Alternatively, there is a simple read method to read the data from a table with the given `columns`, `filters` and `limit`:

```python
data = BigQuery("dataset.table").read(
    columns=["field1"],
    filters=[("field1", "=", "value1")],
    limit=1,
    to_dataframe=True,
)
print(data)
# Output: pd.DataFrame({'field1': ['value1']})
```

By default, the `read` method returns a Pandas DataFrame, but you can also get the data as a list of dictionaries by setting the `to_dataframe` parameter to `False`.

### Inserting data

To insert data into a BigQuery table, use the `insert` method:

```python
data = {
    "field1": "value1",
    "field2": "value2"
}
BigQuery("dataset.table").insert(data)
# Output: Data inserted
```

### External tables

One can also create BigQuery external tables by specifying the file path:

```python
file_path = "gs://bucket/file.parquet"
BigQuery("dataset.external_table").create(file_path)
# Output: Dataset "dataset" created, external table "dataset.external_table" created
```

The allowed file formats are CSV, JSON, Avro, Parquet (single and partitioned), ORC.

---

## Storage Module

The Storage module in the `gcp-pal` library allows you to perform read and write operations on Google Cloud Storage buckets and objects.

### Initializing Storage

Import the Storage class from the `gcp_pal` module:

```python
from gcp_pal import Storage
```

### Listing objects

Similar to the other modules, listing objects in a bucket is done using the `ls` method:

```python
buckets = Storage().ls()
print(buckets)
# Output: ['bucket1', 'bucket2']
objects = Storage("bucket1").ls()
print(objects)
# Output: ['object1', 'object2']
```

### Creating buckets

To create a bucket, use the `create` method:

```python
Storage("new-bucket").create()
# Output: Bucket "new-bucket" created
```

### Deleting objects

Deleting objects is similar to creating them, but you use the `delete` method instead:

```python
Storage("bucket").delete()
# Output: Bucket "bucket" and all its objects deleted
Storage("bucket/object").delete()
# Output: Object "object" in bucket "bucket" deleted
```

### Uploading and downloading objects

To upload an object to a bucket, use the `upload` method:

```python
Storage("bucket/uploaded_file.txt").upload("local_file.txt")
# Output: File "local_file.txt" uploaded to "bucket/uploaded_file.txt"
```

To download an object from a bucket, use the `download` method:

```python
Storage("bucket/uploaded_file.txt").download("downloaded_file.txt")
# Output: File "bucket/uploaded_file.txt" downloaded to "downloaded_file.txt"
```

---

## Parquet Module

The Parquet module in the `gcp-pal` library allows you to read and write Parquet files in Google Cloud Storage.

### Initializing Parquet

Import the Parquet class from the `gcp_pal` module:

```python
from gcp_pal import Parquet
```

### Reading Parquet files

To read a Parquet file from Google Cloud Storage, use the `read` method:

```python
data = Parquet("bucket/file.parquet").read()
print(data)
# Output: pd.DataFrame({'field1': ['value1'], 'field2': ['value2']})
```

### Writing Parquet files

To write a Pandas DataFrame to a Parquet file in Google Cloud Storage, use the `write` method:

```python
df = pd.DataFrame({
    "field1": ["value1"],
    "field2": ["value2"]
})
Parquet("bucket/file.parquet").write(df)
# Output: Parquet file "file.parquet" created in "bucket"
```

Partitioning can be specified via the `partition_cols` parameter:

```python
Parquet("bucket/file.parquet").write(df, partition_cols=["field1"])
# Output: Parquet file "file.parquet" created in "bucket" partitioned by "field1"
```

---

## Schema Module

The Schema module allows one to translate schemas between different formats, such as Python, PyArrow, BigQuery, and Pandas.

### Initializing Schema

Import the `Schema` class from the `gcp_pal` module:

```python
from gcp_pal.schema import Schema
```

### Translating schemas

To translate a schema from one format to another, use the respective methods:

```python
str_schema = {
    "a": "int",
    "b": "str",
    "c": "float",
    "d": {
        "d1": "datetime",
        "d2": "timestamp",
    },
}
python_schema = Schema(str_schema).str()
# {
#    "a": int,
#    "b": str,
#    "c": float,
#    "d": {
#        "d1": datetime,
#        "d2": datetime,
#    },
# }
pyarrow_schema = Schema(str_schema).pyarrow()
# pa.schema(
#    [
#        pa.field("a", pa.int64()),
#        pa.field("b", pa.string()),
#        pa.field("c", pa.float64()),
#        pa.field("d", pa.struct([
#            pa.field("d1", pa.timestamp("ns")),
#            pa.field("d2", pa.timestamp("ns")),
#        ])),
#    ]
# )
bigquery_schema = Schema(str_schema).bigquery()
# [
#     bigquery.SchemaField("a", "INTEGER"),
#     bigquery.SchemaField("b", "STRING"),
#     bigquery.SchemaField("c", "FLOAT"),
#     bigquery.SchemaField("d", "RECORD", fields=[
#        bigquery.SchemaField("d1", "DATETIME"),
#        bigquery.SchemaField("d2", "TIMESTAMP"),
#     ]),
# ]
pandas_schema = Schema(str_schema).pandas()
# {
#    "a": "int64",
#    "b": "object",
#    "c": "float64",
#    "d": {
#        "d1": "datetime64[ns]",
#        "d2": "datetime64[ns]",
#    },
# }
```

### Infering schemas

To infer and translate a schema from a dictionary of data or a Pandas DataFrame, use the `is_data` parameter:

```python
df = pd.DataFrame(
    {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1.0, 2.0, 3.0],
        "date": [datetime.datetime.now() for _ in range(3)],
    }
)
inferred_schema = Schema(df, is_data=True).schema
# {
#   "a": "int",
#   "b": "str",
#   "c": "float",
#   "date": "datetime",
# }
pyarrow_schema = Schema(df, is_data=True).pyarrow()
# pa.schema(
#    [
#        pa.field("a", pa.int64()),
#        pa.field("b", pa.string()),
#        pa.field("c", pa.float64()),
#        pa.field("date", pa.timestamp("ns")),
#    ]
# )
```

---

## Cloud Functions Module

The Cloud Functions module in the `gcp-pal` library allows you to deploy and manage Cloud Functions.

### Initializing Cloud Functions

Import the `CloudFunctions` class from the `gcp_pal` module:

```python
from gcp_pal import CloudFunctions
```

### Deploying Cloud Functions

To deploy a Cloud Function, specifty the function's name, the source codebase, the entry point and any other parameters that are to be passed to `BuildConfig`, `ServiceConfig` and `Function` (see [docs](https://cloud.google.com/python/docs/reference/cloudfunctions/latest/google.cloud.functions_v2.types)):

```python
CloudFunctions("function-name").deploy(
    path="path/to/function_codebase",
    entry_point="main",
    environment=2,
)
```

Deploying a Cloud Function from a local source depends on the `gcp_toole.Storage` module. The source codebase is uploaded to a `{PROJECT_ID}-cloud-functions` bucket and is deployed from there. An alternative bucket can be specified via the `source_bucket` parameter:

```python
CloudFunctions("function-name").deploy(
    path="path/to/function_codebase",
    entry_point="main",
    environment=2,
    source_bucket="bucket-name",
)
```

### Listing Cloud Functions

To list all Cloud Functions within a project, use the `ls` method:

```python
functions = CloudFunctions().ls()
print(functions)
# Output: ['function1', 'function2']
```

### Deleting Cloud Functions

To delete a Cloud Function, use the `delete` method:

```python
CloudFunctions("function-name").delete()
# Output: Cloud Function "function-name" deleted
```

### Invoking Cloud Functions

To invoke a Cloud Function, use the `invoke` (or `call`) method:

```python
response = CloudFunctions("function-name").invoke({"key": "value"})
print(response)
# Output: {'output_key': 'output_value'}
```

### Getting Cloud Function details

To get the details of a Cloud Function, use the `get` method:

```python
details = CloudFunctions("function-name").get()
print(details)
# Output: {'name': 'projects/project-id/locations/region/functions/function-name', 
#          'build_config': {...}, 'service_config': {...}, 'state': {...}, ... }
```

---

## Docker Module

The Docker module in the `gcp-pal` library allows you to build and push Docker images to Google Container Registry.

### Initializing Docker

Import the Docker class from the `gcp_pal` module:

```python
from gcp_pal import Docker
```

### Building Docker images

```python
Docker("image-name").build(path="path/to/context", dockerfile="Dockerfile")
# Output: Docker image "image-name:latest" built based on "path/to/context" codebase and "path/to/context/Dockerfile".
```

The default `tag` is `"latest"` but can be specified via the `tag` parameter:

```python
Docker("image-name", tag="5fbd72c").build(path="path/to/context", dockerfile="Dockerfile")
# Output: Docker image "image-name:5fbd72c" built based on "path/to/context" codebase and "path/to/context/Dockerfile".
```

### Pushing Docker images

```python
Docker("image-name").push()
# Output: Docker image "image-name" pushed to Google Container Registry.
```

The default destination is `"gcr.io/{PROJECT_ID}/{IMAGE_NAME}:{TAG}"` but can be specified via the `destination` parameter:

```python
Docker("image-name").push(destination="gcr.io/my-project/image-name:5fbd72c")
# Output: Docker image "image-name" pushed to "gcr.io/my-project/image-name:5fbd72c".
```

---

## Cloud Run Module

The Cloud Run module in the `gcp-pal` library allows you to deploy and manage Cloud Run services.

### Initializing Cloud Run

Import the `CloudRun` class from the `gcp_pal` module:

```python
from gcp_pal import CloudRun
```

### Deploying Cloud Run services

```python
CloudRun("test-app").deploy(path="samples/cloud_run")
# Output: 
# - Docker image "test-app" built based on "samples/cloud_run" codebase and "samples/cloud_run/Dockerfile".
# - Docker image "test-app" pushed to Google Container Registry as "gcr.io/{PROJECT_ID}/test-app:random_tag".
# - Cloud Run service "test-app" deployed from "gcr.io/{PROJECT_ID}/test-app:random_tag".
```

The default tag is a random string but can be specified via the `image_tag` parameter:

```python
CloudRun("test-app").deploy(path="samples/cloud_run", image_tag="5fbd72c")
# Output: Cloud Run service deployed
```

### Listing Cloud Run services

To list all Cloud Run services within a project, use the `ls` method:

```python
services = CloudRun().ls()
print(services)
# Output: ['service1', 'service2']
```

To list the job, set the `job` parameter to `True`:

```python
jobs = CloudRun(job=True).ls()
print(jobs)
# Output: ['job1', 'job2']
```

### Deleting Cloud Run services

To delete a Cloud Run service, use the `delete` method:

```python
CloudRun("service-name").delete()
# Output: Cloud Run service "service-name" deleted
```

Similarly to delete a job, set the `job` parameter to `True`:

```python
CloudRun("job-name", job=True).delete()
```

### Invoking Cloud Run services

To invoke a Cloud Run service, use the `invoke`/`call` method:

```python
response = CloudRun("service-name").invoke({"key": "value"})
print(response)
# Output: {'output_key': 'output_value'}
```

### Getting Cloud Run service details

To get the details of a Cloud Run service, use the `get` method:

```python
details = CloudRun("service-name").get()
print(details)
# Output: ...
```

To get the status of a Cloud Run service, use the `status`/`state` method:

```python
service_status = CloudRun("service-name").status()
print(service_status)
# Output: Active
job_status = CloudRun("job-name", job=True).status()
print(job_status)
# Output: Active
```

---

## Logging Module

The Logging module in the `gcp-pal` library allows you to access and manage logs from Google Cloud Logging.

### Initializing Logging

Import the Logging class from the `gcp_pal` module:

```python
from gcp_pal import Logging
```

### Listing logs

To list all logs within a project, use the `ls` method:

```python
logs = Logging().ls(limit=2)
for log in logs:
    print(log)
# Output: LogEntry - [2024-04-16 17:30:04.308 UTC] {Message payload}
```

Where each entry is a `LogEntry` object with the following attributes: `project`, `log_name`, `resource`, `severity`, `message`, `timestamp`, `time_zone`, `timestamp_str`.

The `message` attribute is the main payload of the log entry.

### Filtering logs

To filter logs based on a query, use the `filter` method:

```python
logs = Logging().ls(filter="severity=ERROR")
# Output: [LogEntry - [2024-04-16 17:30:04.308 UTC] {Message payload}, ...]
```

Some common filters are also supported natively: `severity` (str), `time_start` (str), `time_end` (str), `time_range` (int: hours). For example, the following are equivalent:

```python
# Time now: 2024-04-16 17:30:04.308 UTC
logs = Logging().ls(filter="severity=ERROR AND time_start=2024-04-16T16:30:04.308Z AND time_end=2024-04-16T17:30:04.308Z")
logs = Logging().ls(severity="ERROR", time_start="2024-04-16T16:30:04.308Z", time_end="2024-04-16T17:30:04.308Z")
logs = Logging().ls(severity="ERROR", time_range=1)
```

### Streaming logs

To stream logs in real-time, use the `stream` method:

```python
Logging().stream()
# LogEntry - [2024-04-16 17:30:04.308 UTC] {Message payload}
# LogEntry - [2024-04-16 17:30:05.308 UTC] {Message payload}
# ...
```

---

## Secret Manager Module

The Secret Manager module in the `gcp-pal` library allows you to access and manage secrets from Google Cloud Secret Manager.

### Initializing Secret Manager

Import the SecretManager class from the `gcp_pal` module:

```python
from gcp_pal import SecretManager
```

### Creating secrets

To create a secret, specify the secret's name and value:

```python
SecretManager("secret1").create("value1", labels={"env": "dev"})
# Output: Secret 'secret1' created
```


### Listing secrets

To list all secrets within a project, use the `ls` method:

```python
secrets = SecretManager().ls()
print(secrets)
# Output: ['secret1', 'secret2']
```

The `ls` method also supports filtering secrets based on `filter` or `label` parameters:

```python
secrets = SecretManager().ls(filter="name:secret1")
print(secrets)
# Output: ['secret1']
secrets = SecretManager().ls(label="env:*")
print(secrets)
# Output: ['secret1', 'secret2']
```

### Accessing secrets

To access the value of a secret, use the `value` method:

```python
value = SecretManager("secret1").value()
print(value)
# Output: 'value1'
```

### Deleting secrets

To delete a secret, use the `delete` method:

```python
SecretManager("secret1").delete()
# Output: Secret 'secret1' deleted
```

---

## Cloud Scheduler Module

The Cloud Scheduler module in the `gcp-pal` library allows you to create and manage Cloud Scheduler jobs.

### Initializing Cloud Scheduler

Import the CloudScheduler class from the `gcp_pal` module:

```python
from gcp_pal import CloudScheduler
```

### Creating Cloud Scheduler jobs

To create a Cloud Scheduler job, specify the job's name in the constructor, and use the `create` method to set the schedule and target:

```python
CloudScheduler("job-name").create(
    schedule="* * * * *",
    time_zone="UTC",
    target="https://example.com/api",
    payload={"key": "value"},
)
# Output: Cloud Scheduler job "job-name" created with HTTP target "https://example.com/api"
```

If the `target` is not an HTTP endpoint, it will be treated as a PubSub topic:

```python
CloudScheduler("job-name").create(
    schedule="* * * * *",
    time_zone="UTC",
    target="pubsub-topic-name",
    payload={"key": "value"},
)
# Output: Cloud Scheduler job "job-name" created with PubSub target "pubsub-topic-name"
```

Additionally, `service_account` can be specified to add the OAuth and OIDC tokens to the request:

```python
CloudScheduler("job-name").create(
    schedule="* * * * *",
    time_zone="UTC",
    target="https://example.com/api",
    payload={"key": "value"},
    service_account="PROJECT@PROJECT.iam.gserviceaccount.com",
)
# Output: Cloud Scheduler job "job-name" created with HTTP target "https://example.com/api" and OAuth+OIDC tokens
```

### Listing Cloud Scheduler jobs

To list all Cloud Scheduler jobs within a project, use the `ls` method:

```python
jobs = CloudScheduler().ls()
print(jobs)
# Output: ['job1', 'job2']
```

### Deleting Cloud Scheduler jobs

To delete a Cloud Scheduler job, use the `delete` method:

```python
CloudScheduler("job-name").delete()
# Output: Cloud Scheduler job "job-name" deleted
```

### Managing Cloud Scheduler jobs

To pause or resume a Cloud Scheduler job, use the `pause` or `resume` methods:

```python
CloudScheduler("job-name").pause()
# Output: Cloud Scheduler job "job-name" paused
CloudScheduler("job-name").resume()
# Output: Cloud Scheduler job "job-name" resumed
```

To run a Cloud Scheduler job immediately, use the `run` method:

```python
CloudScheduler("job-name").run()
# Output: Cloud Scheduler job "job-name" run
```

If the job is paused, it will be resumed before running. To prevent this, set the `force` parameter to `False`:

```python
CloudScheduler("job-name").run(force=False)
# Output: Cloud Scheduler job "job-name" not run if it is paused
```


---

## Project Module

The Project module in the `gcp-pal` library allows you to access and manage Google Cloud projects.

### Initializing Project

Import the Project class from the `gcp_pal` module:

```python
from gcp_pal import Project
```

### Listing projects

To list all projects available to the authenticated user, use the `ls` method:

```python
projects = Project().ls()
print(projects)
# Output: ['project1', 'project2']
```

### Creating projects

To create a new project, use the `create` method:

```python
Project("new-project").create()
# Output: Project "new-project" created
```

### Deleting projects

To delete a project, use the `delete` method:

```python
Project("project-name").delete()
# Output: Project "project-name" deleted
```

Google Cloud will delete the project after 30 days. During this time, to undelete a project, use the `undelete` method:

```python
Project("project-name").undelete()
# Output: Project "project-name" undeleted
```

### Getting project details

To get the details of a project, use the `get` method:

```python
details = Project("project-name").get()
print(details)
# Output: {'name': 'projects/project-id', 'project_id': 'project-id', ...}
```

To obtain the project number use the `number` method:

```python
project_number = Project("project-name").number()
print(project_number)
# Output: "1234567890"
```


---

## Dataplex Module

The Dataplex module in the `gcp-pal` library allows you to interact with Dataplex services.

### Initializing Dataplex

Import the Dataplex class from the `gcp_pal` module:

```python
from gcp_pal import Dataplex
```

### Listing Dataplex objects

The Dataplex module supports listing all lakes, zones, and assets within a Dataplex instance:

```python
lakes = Dataplex().ls()
print(lakes)
# Output: ['lake1', 'lake2']
zones = Dataplex("lake1").ls()
print(zones)
# Output: ['zone1', 'zone2']
assets = Dataplex("lake1/zone1").ls()
print(assets)
# Output: ['asset1', 'asset2']
```

### Creating Dataplex objects

To create a lake, zone, or asset within a Dataplex instance, use the `create_lake`, `create_zone`, and `create_asset` methods.

To create a lake:

```python
Dataplex("lake1").create_lake()
# Output: Lake "lake1" created
```

To create a zone (zone type and location type are required):

```python
Dataplex("lake1/zone1").create_zone(zone_type="raw", location_type="single-region")
# Output: Zone "zone1" created in Lake "lake1"
```

To create an asset (asset source and asset type are required):

```python
Dataplex("lake1/zone1").create_asset(asset_source="dataset_name", asset_type="bigquery")
# Output: Asset "asset1" created in Zone "zone1" of Lake "lake1"
```

### Deleting Dataplex objects

Deleting objects can be done using a single `delete` method:

```python
Dataplex("lake1/zone1/asset1").delete()
# Output: Asset "asset1" deleted
Dataplax("lake1/zone1").delete()
# Output: Zone "zone1" and all its assets deleted
Dataplex("lake1").delete()
# Output: Lake "lake1" and all its zones and assets deleted
```

