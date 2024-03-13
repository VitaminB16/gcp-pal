import os
import json
import concurrent.futures
from google.cloud import firestore

from gcp_tools.utils import log


class Firestore:
    """
    Class for operating Firestore
    """

    def __init__(self, path=None, project=None):
        """
        Args:
        - path (str): Path to the Firestore document or collection
        - project (str): Project ID

        Examples:
        - Firestore("bucket/path").read() -> Read from Firestore "bucket/path"
        - Firestore("gs://project/bucket/path").read() -> Read from Firestore "bucket/path"
        - Firestore("gs://project/bucket/path").write(data) -> Write to Firestore "bucket/path"
        - Firestore("gs://project/bucket/path").delete() -> Delete from Firestore "bucket/path"
        """
        self.project = project or os.getenv("PROJECT")
        self.client = firestore.Client(self.project)
        self.path = path

    def _parse_path(self, method="get"):
        """
        Parse the path into project, bucket, and path.
        This allows Firestore to be used in the same way as GCS.
        """
        if "gs://" in self.path:
            # gs://project/bucket/path -> project, bucket, path
            # path -> collection/document/../collection/document
            self.path = self.path.replace("gs://", "")
            self.bucket, self.path = self.path.split("/", 1)
        path_elements = self.path.split("/")
        return path_elements

    def get_ref(self, method=None):
        path_elements = self._parse_path(method)
        doc_ref = self.client.collection(path_elements.pop(0))
        ref_type = "document"
        while len(path_elements) > 0:
            doc_ref = getattr(doc_ref, ref_type)(path_elements.pop(0))
            ref_type = "document" if ref_type == "collection" else "collection"
        return doc_ref

    def read(self, allow_empty=False, apply_schema=False, schema=None):
        """
        Read from Firestore
        Args:
        - allow_empty (bool): If True, return an empty DataFrame if the document is empty
        - apply_schema (bool): If True, apply the schema from FIRESTORE_SCHEMAS. Also converts the output to a DataFrame.
        """
        doc_ref = self.get_ref(method="get")
        output = doc_ref.get().to_dict()
        dtypes = {}
        if output is None and allow_empty:
            output = {}
        if set(output.keys()) in [{"data"}, {"data", "dtypes"}]:
            dtypes = output.get("dtypes", dtypes)
            output = output.get("data", output)
        if apply_schema:
            from pandas import DataFrame

            df = DataFrame(output)
            from gcp_tools.utils import enforce_schema

            output = enforce_schema(df, schema=schema, dtypes=dtypes)
        log(f"Read from Firestore: {self.path}")
        return output

    def write(self, data, columns=None):
        """
        Write to Firestore
        Args:
        - data (DataFrame or dict): Data to write to Firestore
        - columns (list): Columns to write from the DataFrame
        Output:
        - True if successful
        """
        doc_ref = self.get_ref(method="set")
        dtypes = None
        if isinstance(data, DataFrame):
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
            output = {"data": data}
            if dtypes is not None:
                output["dtypes"] = dtypes
            doc_ref.set(output)
        log(f"Written to Firestore: {self.path}")
        return True

    def delete(self):
        """
        Recursively deletes documents and collections from Firestore.
        """
        ref = self.get_ref()
        ref_type = self._ref_type(ref)
        if ref_type == "document":
            self._delete_document(ref)
        elif ref_type == "collection":
            self._delete_collection(ref)
        else:
            raise ValueError("Unsupported Firestore reference type.")
        print(f"Deleted {ref_type} from Firestore: {self.path}")
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
        is_doc_ref = isinstance(doc_ref, firestore.DocumentReference)
        is_coll_ref = isinstance(doc_ref, firestore.CollectionReference)
        return "document" if is_doc_ref else "collection" if is_coll_ref else None
