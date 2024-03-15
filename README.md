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
from gcp_tools.firestore import Firestore
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