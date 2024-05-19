from __future__ import annotations

import os
import json
import concurrent.futures
from gcp_pal.utils import try_import

from gcp_pal.schema import enforce_schema
from gcp_pal.utils import (
    is_dataframe,
    get_auth_default,
    log,
    ClientHandler,
    ModuleHandler,
)


class Firestore:
    """
    Class for operating Firestore
    """

    def __init__(self, path=None, project=None):
        """
        Args:
        - path (str): Path to the Firestore document or collection
        - project (str): Project ID

        Storage-equivalent examples:
        - `Firestore().ls()` -> List all collections
        - `Firestore("collection").ls()` -> List all documents in collection "collection"
        - `Firestore("bucket/path").read()` -> Read from Firestore "bucket/path"
        - `Firestore("gs://project/bucket/path").read()` -> Read from Firestore "bucket/path"
        - `Firestore("gs://project/bucket/path").write(data)` -> Write to Firestore "bucket/path"
        - `Firestore("gs://project/bucket/path").delete()` -> Delete from Firestore "bucket/path"
        """
        self.project = project or os.getenv("PROJECT") or get_auth_default()[1]
        self.path = path

        # Only initialize the client once per project
        self.firestore = ModuleHandler("google.cloud").please_import(
            "firestore", who_is_calling="Firestore"
        )
        self.client = ClientHandler(self.firestore.Client).get(project=self.project)

    def __repr__(self):
        return f"Firestore({self.path})"

    def _parse_path(self, method="get"):
        """
        Parse the path into project, bucket, and path. This allows Firestore to be used in the same way as GCS.

        Returns:
        - Path elements (list): List of alternating path elements [collection, document, collection, ...]

        Examples:
        - Firestore("collection/document")._parse_path() -> ["collection", "document"]
        - Firestore("gs://project/bucket/collection/document")._parse_path() -> ["collection", "document"]
        - Firestore("gs://project/bucket/output/data.csv")._parse_path() -> ["output", "data.csv"]
        """
        if self.path is None:
            return None
        if "gs://" in self.path:
            # gs://project/bucket/path -> project, bucket, path
            # path -> collection/document/../collection/document
            self.path = self.path.replace("gs://", "")
            self.bucket, self.path = self.path.split("/", 1)
        path_elements = self.path.split("/")
        return path_elements

    def get(self, method=None):
        """
        Get a reference to a Firestore document or collection.

        Returns:
        - Firestore reference (DocumentReference or CollectionReference)

        Examples:
        - Firestore("collection/document").get() -> Reference to document "collection/document"
        - Firestore("collection").get() -> Reference to collection "collection"
        """
        path_elements = self._parse_path(method)
        if path_elements is None:
            return None
        doc_ref = self.client.collection(path_elements.pop(0))
        ref_type = "document"
        while len(path_elements) > 0:
            doc_ref = getattr(doc_ref, ref_type)(path_elements.pop(0))
            ref_type = "document" if ref_type == "collection" else "collection"
        return doc_ref

    def async_read(self, paths_list, allow_empty=False, apply_schema=False, schema={}):
        """
        Read a list of paths from Firestore asynchronously.

        Args:
        - paths_list (list): List of paths to read from Firestore
        - allow_empty (bool): If True, return an empty DataFrame if the document is empty
        - apply_schema (bool): If True, apply the schema from FIRESTORE_SCHEMAS.
                               Also converts the output to a DataFrame.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(
                    Firestore(path).read,
                    allow_empty=allow_empty,
                    apply_schema=apply_schema,
                    schema=schema,
                )
                for path in paths_list
            ]
            concurrent.futures.wait(futures)
            output = {
                path.split("/")[-1]: future.result()
                for path, future in zip(paths_list, futures)
            }
        return output

    def read(self, allow_empty=False, apply_schema=False, schema={}, path_schemas={}):
        """
        Read from Firestore

        Args:
        - allow_empty (bool): If True, return an empty DataFrame if the document is empty
        - apply_schema (bool): If True, apply the schema from FIRESTORE_SCHEMAS.
                               Also converts the output to a DataFrame.
        - schema (dict): Schema to enforce on the output
        - path_schemas (dict): Schemas to enforce on specific paths.

        Returns:
        - Output from Firestore (DataFrame or dict)

        Examples:
        - Firestore("coll/doc").read() -> Read from Firestore "coll/doc"
        - Firestore("coll").read() -> Read all docs from Firestore collection "coll"
        - Firestore("coll/doc").read(apply_schema=True, path_schemas={"coll/doc": {"a": int}})
                                   -> Read from Firestore "coll/doc" and enforce schema on "a"
        """
        if path_schemas:
            path_schemas = path_schemas.get(self.path, {})
            schema = {**schema, **path_schemas}
        doc_ref = self.get(method="get")
        if self._ref_type(doc_ref) == "collection":
            # If the reference is a collection, return a list of documents
            paths_list = [f"{self.path}/{doc.id}" for doc in doc_ref.stream()]
            return self.async_read(
                paths_list,
                allow_empty=allow_empty,
                apply_schema=apply_schema,
                schema=schema,
            )
        output = doc_ref.get().to_dict()
        metadata = {}
        object_type = None
        dtypes = None
        if output is None and allow_empty:
            output = {}
        if set(output.keys()) in [{"data"}, {"data", "metadata"}]:
            metadata = output.get("metadata", {})
            object_type = metadata.get("object_type", None)
            dtypes = metadata.get("dtypes", None)
            output = output.get("data", output)
            if dtypes is not None:
                apply_schema = True
        if apply_schema:
            if object_type == "<class 'pandas.core.frame.DataFrame'>":
                try_import("pandas", "DataFrame")
                from pandas import DataFrame

                output = DataFrame(output)
                output = output.reset_index(drop=True)
            output = enforce_schema(output, schema=schema, dtypes=dtypes)
        log(f"Firestore - read {self.path}")
        return output

    def write(self, data, columns=None):
        """
        Write to Firestore

        Args:
        - data (DataFrame or dict): Data to write to Firestore
        - columns (list): Columns to write from the DataFrame

        Returns:
        - True if successful

        Examples:
        - Firestore("coll/doc").write(data) -> Write data to Firestore "coll/doc"
        """
        doc_ref = self.get(method="set")
        dtypes, object_type = None, None
        object_type = str(type(data))
        if is_dataframe(data):
            if columns is not None:
                data = data[columns]
            data.reset_index(drop=True, inplace=True)
            dtypes = data.dtypes.astype(str).to_dict()
            data = data.to_json()
        try:
            doc_ref.set(data)
        except ValueError as e:
            # This happens if the data is a dictionary with integer keys
            doc_ref.set(json.loads(json.dumps(data)))
        except AttributeError as e:
            # This happens if the data is a list of dictionaries
            if isinstance(data, str):
                # This happens if the data came from DataFrame.to_json()
                data = json.loads(data)
            output = {"data": data, "metadata": {}}
            if dtypes is not None:
                output["metadata"]["dtypes"] = dtypes
            if object_type is not None:
                output["metadata"]["object_type"] = object_type
            doc_ref.set(output)
        log(f"Firestore - written {self.path}")
        return True

    def create(self, **kwargs):
        """
        Create an empty Firestore document or collection.
        """
        ref = self.get()
        ref_type = self._ref_type(ref)
        if ref_type == "document":
            ref.set({})
        elif ref_type == "collection":
            ref.add({})
        else:
            raise ValueError("Unsupported Firestore reference type.")
        log(f"Firestore - created {ref_type}: Firestore/{self.path}")
        return True

    def delete(self):
        """
        Recursively deletes documents and collections from Firestore.
        """
        ref = self.get()
        ref_type = self._ref_type(ref)
        if ref_type == "document":
            self._delete_document(ref)
        elif ref_type == "collection":
            self._delete_collection(ref)
        else:
            raise ValueError("Unsupported Firestore reference type.")
        log(f"Firestore - deleted {ref_type}: Firestore/{self.path}")
        return True

    def _delete_document(self, doc_ref):
        """
        Deletes a document and all of its collections.
        """
        collections = doc_ref.collections()
        for collection in collections:
            self._delete_collection(collection)
        doc_ref.delete()

    def _delete_collection(self, col_ref):
        """
        Deletes a collection and all of its documents.
        """
        docs = col_ref.stream()
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(self._delete_document, doc.reference) for doc in docs
            ]
            concurrent.futures.wait(futures)

    def _ref_type(self, doc_ref):
        is_doc_ref = isinstance(doc_ref, self.firestore.DocumentReference)
        is_coll_ref = isinstance(doc_ref, self.firestore.CollectionReference)
        return "document" if is_doc_ref else "collection" if is_coll_ref else None

    def ls(self, path=None) -> list[str]:
        """
        List all documents in a collection or all collections in a document.

        Args:
        - path (str): Path to the Firestore document or collection from base path

        Returns:
        - List of documents or collections
        """
        if path is not None:
            self.path = self.path + "/" + path
        ref = self.get()
        if ref is None:
            output = [col.id for col in self.client.collections()]
            log(f"Firestore - collections listed.")
            return output
        ref_type = self._ref_type(ref)
        if ref_type == "document":
            output = [doc.id for doc in ref.collections()]
        elif ref_type == "collection":
            output = [doc.id for doc in ref.stream()]
        else:
            raise ValueError("Unsupported Firestore reference type.")
        log(f"Firestore - collections listed.")
        return output

    def exists(self):
        """
        Check if a document or collection exists in Firestore.
        """
        ref = self.get()
        ref_type = self._ref_type(ref)
        if ref_type == "document":
            exists = ref.get().exists
        elif ref_type == "collection":
            exists = bool(list(ref.stream()))
        else:
            raise ValueError("Unsupported Firestore reference type.")
        return exists


if __name__ == "__main__":
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1, 2, 3],
    }
    import pandas as pd

    data = pd.DataFrame(data)
    collection_name = "test_collection"
    Firestore(f"{collection_name}/test_document1").write(data)
    Firestore(f"{collection_name}/test_document2").write(data)
    Firestore(f"{collection_name}/test_document3").write(data)
    print(Firestore(collection_name).ls())
    print(Firestore().ls())
    output = Firestore(collection_name).read(apply_schema=True)
    print(output)
    Firestore(collection_name).delete()
