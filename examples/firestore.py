"""
Example module to demonstrate how the Firestore module can be used for Google Cloud Firestore operations.
"""


def example_perations():
    from gcp_pal import Firestore
    from uuid import uuid4

    collection_name = f"example_collection_{uuid4()}"

    # Create a collection
    Firestore(collection_name).create()

    # List all collections
    collections = Firestore().ls()
    print(collections)

    # Delete the collection
    Firestore(collection_name).delete()

    # List all collections
    collections = Firestore().ls()
    print(collections)


def example_read_write():
    from gcp_pal import Firestore
    from uuid import uuid4

    collection_name = f"example_collection_{uuid4()}"
    document_name = f"example_document_{uuid4()}"
    document_data = {"name": "John Doe", "age": 30}

    # Write data to Firestore
    document_path = f"{collection_name}/{document_name}"
    Firestore(document_path).write(document_data)

    # Read data from Firestore
    data = Firestore(document_path).read()
    print(data)

    Firestore(collection_name).delete()


def example_read_write_df():
    from gcp_pal import Firestore
    from uuid import uuid4
    import pandas as pd

    collection_name = f"example_collection_{uuid4()}"
    document_name = f"example_document_{uuid4()}"
    document_data = pd.DataFrame({"name": ["John Doe"], "age": [30]})

    # Write data to Firestore
    document_path = f"{collection_name}/{document_name}"
    Firestore(document_path).write(document_data)

    # Read data from Firestore
    data = Firestore(document_path).read()
    print(data)

    Firestore(collection_name).delete()


def example_get_ref():
    from gcp_pal import Firestore
    from uuid import uuid4

    collection_name = f"example_collection_{uuid4()}"
    document_name = f"example_document_{uuid4()}"
    document_data = {"name": "John Doe", "age": 30}

    # Write data to Firestore
    document_path = f"{collection_name}/{document_name}"
    Firestore(document_path).write(document_data)

    # Get the reference to the document
    ref = Firestore(document_path).get()
    ref.update({"age": 31})

    # Read data from Firestore
    data = Firestore(document_path).read()

    print(data)

    Firestore(collection_name).delete()


if __name__ == "__main__":
    example_get_ref()
