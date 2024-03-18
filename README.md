<!--
TODO:
[x] Firestore Module
[x] PubSub Module
[x] Request Module
[ ] Storage Module
[ ] Parquet Module
[ ] BigQuery Module
[ ] Combine Storage, Parquet, BigQuery and Firestore for a universal Storage module
[ ] Logging Module
[ ] Secret Manager Module
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

---

## Firestore Module

The Firestore module in the `gcp-tools` library allows you to perform read and write operations on Firestore documents and collections with ease.

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

The PubSub module in the `gcp-tools` library allows you to publish and subscribe to PubSub topics with ease.

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

The Request module in the `gcp-tools` library allows you to make authorized HTTP requests with ease.

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

The BigQuery module in the `gcp-tools` library allows you to perform read and write operations on BigQuery datasets and tables with ease.

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
BigQuery(table="new-dataset2.new-table").create(schema=schema) 
# Output: Dataset "new-dataset2" created, table "new-dataset2.new-table" created
```

To create a table from a Pandas DataFrame, pass the DataFrame to the `create` method:

```python
df = pd.DataFrame({
    "field1": ["value1"],
    "field2": ["value2"]
})
BigQuery(table="new-dataset3.new-table").create(data=df)
# Output: Dataset "new-dataset3" created, table "new-dataset3.new-table" created, data inserted
```

### Deleting objects

Deleting objects is similar to creating them, but you use the `delete` method instead:

```python
BigQuery(dataset="dataset").delete()
# Output: Dataset "dataset" and all its tables deleted
BigQuery(table="dataset.table").delete()
# Output: Table "dataset.table" deleted
```
