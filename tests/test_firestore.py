import json
import pandas as pd
from uuid import uuid4
from google.cloud import firestore

from gcp_tools.firestore import Firestore


def dicts_equal(d1, d2):
    return json.dumps(d1, sort_keys=True) == json.dumps(d2, sort_keys=True)


def test_write_dict():
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1, 2, 3],
    }
    collection_name = "test_write_dict"
    document_name = f"test_document_{uuid4()}"
    Firestore(f"{collection_name}/{document_name}").write(data)
    firestore_client = firestore.Client()
    doc_ref = firestore_client.collection(collection_name).document(document_name)
    written_data = doc_ref.get().to_dict()
    success = dicts_equal(written_data, data)
    # Clean up
    doc_ref.delete()
    assert success


def test_write_df():
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1, 2, 3],
    }
    collection_name = "test_write_df"
    document_name = f"test_document_{uuid4()}"
    df = pd.DataFrame(data)
    Firestore(f"{collection_name}/{document_name}").write(df)
    firestore_client = firestore.Client()
    doc_ref = firestore_client.collection(collection_name).document(document_name)
    written_data = doc_ref.get().to_dict()
    expected_metadata = {
        "dtypes": {"a": "int64", "b": "object", "c": "int64"},
        "object_type": "<class 'pandas.core.frame.DataFrame'>",
    }
    expected_data = {
        "a": {"0": 1, "1": 2, "2": 3},
        "b": {"0": "a", "1": "b", "2": "c"},
        "c": {"0": 1, "1": 2, "2": 3},
    }
    expected_output = {"data": expected_data, "metadata": expected_metadata}
    success = dicts_equal(written_data, expected_output)
    # Clean up
    doc_ref.delete()
    assert success


def test_read_dict():
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1, 2, 3],
    }
    collection_name = "test_read_dict"
    document_name = f"test_document_{uuid4()}"
    firestore_client = firestore.Client()
    doc_ref = firestore_client.collection(collection_name).document(document_name)
    doc_ref.set(data)
    read_data = Firestore(f"{collection_name}/{document_name}").read()
    success = dicts_equal(read_data, data)
    # Clean up
    doc_ref.delete()
    assert success


def test_read_df():
    metadata = {
        "dtypes": {"a": "float64", "b": "object", "c": "int64"},
        "object_type": "<class 'pandas.core.frame.DataFrame'>",
    }
    data = {
        "a": {"0": 1.0, "1": 2.0, "2": 3.0},
        "b": {"0": "a", "1": "b", "2": "c"},
        "c": {"0": 1, "1": 2, "2": 3},
    }
    collection_name = "test_read_df"
    document_name = f"test_document_{uuid4()}"
    firestore_client = firestore.Client()
    doc_ref = firestore_client.collection(collection_name).document(document_name)
    doc_ref.set({"data": data, "metadata": metadata})
    read_data = Firestore(f"{collection_name}/{document_name}").read(apply_schema=True)
    expected_data = pd.DataFrame(data).reset_index(drop=True)
    success = read_data.equals(expected_data)
    # Clean up
    doc_ref.delete()
    assert success


def test_delete():
    collection_name = "test_delete"
    document_name = f"test_document_{uuid4()}"
    firestore_client = firestore.Client()
    doc_ref = firestore_client.collection(collection_name).document(document_name)
    doc_ref.set({"a": 1})
    Firestore(f"{collection_name}/{document_name}").delete()
    read_data = doc_ref.get().to_dict()
    success = read_data is None
    # Clean up
    doc_ref.delete()
    assert success


def test_read_collection():
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1, 2, 3],
    }
    import pandas as pd

    data = pd.DataFrame(data)
    collection_name = "test_read_collection"
    Firestore(f"{collection_name}/test_document1").write(data)
    Firestore(f"{collection_name}/test_document2").write(data)
    Firestore(f"{collection_name}/test_document3").write(data)
    output = Firestore(collection_name).read()
    success1 = len(output) == 3
    success2 = all(isinstance(output[x], pd.DataFrame) for x in output)
    # Clean up
    Firestore(collection_name).delete()
    success3 = Firestore(collection_name).ls() == []
    assert success1
    assert success2
    assert success3
    assert output["test_document1"].equals(data)
    assert output["test_document2"].equals(data)
    assert output["test_document3"].equals(data)
