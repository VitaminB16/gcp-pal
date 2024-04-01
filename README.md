<!--
TODO:
[x] Firestore Module
[x] PubSub Module
[x] Request Module
[x] BigQuery Module
[x] Storage Module
[x] Parquet Module
[x] Schema Module
[ ] Combine Storage, Parquet, BigQuery and Firestore for a universal Storage module
[ ] Logging Module
[ ] Secret Manager Module
[ ] Deploy to PyPi
...
-->

# GCP Tools Library

The `gcp-tools` library provides a set of utilities for interacting with Google Cloud Platform (GCP) services, streamlining the process of implementing GCP functionalities within your Python applications.

The utilities are designed to work with the `google-cloud` Python libraries, providing a more user-friendly and intuitive interface for common tasks.

---

## Configuration

Before you can start using the `gcp-tools` library with Firestore or any other GCP services, make sure you either have set up your GCP credentials properly or have the necessary permissions to access the services you want to use:

```bash
gcloud auth application-default login
```

And specify the project ID to be used as the default for all API requests:

```bash
gcloud config set project PROJECT_ID
```

---

## Firestore Module

The Firestore module in the `gcp-tools` library allows you to perform read and write operations on Firestore documents and collections.

### Initializing Firestore

First, import the Firestore class from the `gcp_tools` module:

```python
from gcp_tools import Firestore
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

The PubSub module in the `gcp-tools` library allows you to publish and subscribe to PubSub topics.

### Initializing PubSub

First, import the PubSub class from the `gcp_tools` module:

```python
from gcp_tools import PubSub
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

The Request module in the `gcp-tools` library allows you to make authorized HTTP requests.

### Initializing Request

Import the Request class from the `gcp_tools` module:

```python
from gcp_tools import Request
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

The BigQuery module in the `gcp-tools` library allows you to perform read and write operations on BigQuery datasets and tables.

### Initializing BigQuery

Import the BigQuery class from the `gcp_tools` module:

```python
from gcp_tools import BigQuery
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

The Storage module in the `gcp-tools` library allows you to perform read and write operations on Google Cloud Storage buckets and objects.

### Initializing Storage

Import the Storage class from the `gcp_tools` module:

```python
from gcp_tools import Storage
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

The Parquet module in the `gcp-tools` library allows you to read and write Parquet files in Google Cloud Storage.

### Initializing Parquet

Import the Parquet class from the `gcp_tools` module:

```python
from gcp_tools import Parquet
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

Import the Schema class from the `gcp-tools.schema` module:

```python
from gcp_tools.schema import Schema
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
