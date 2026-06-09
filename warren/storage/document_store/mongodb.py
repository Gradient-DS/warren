from typing import Union

import uuid
from collections.abc import AsyncGenerator

from basics.base import Base
from bson import ObjectId
from pymongo import AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from document_processing.distributed.warren.storage.document_store.interface import (
    DocumentAlreadyExistsError,
    DocumentNotFoundError,
    DocumentStoreInterface,
    IndexSpec,
)


# Type alias for MongoDB document IDs (either ObjectId or string)
DocId = Union[ObjectId, str]  # noqa: UP007  # runtime type alias


class MongoDBDocumentStore(Base, DocumentStoreInterface):
    """
    Async MongoDB implementation of DocumentStoreInterface.

    Uses pymongo's AsyncMongoClient for non-blocking I/O. Provides document
    storage using MongoDB as the backend. Supports custom document ID fields
    and automatic index creation.

    Requires calling ``await setup()`` after construction to create indexes.
    The factory function handles this automatically.
    """

    def __init__(
        self,
        client: AsyncMongoClient,
        *,
        database_name: str,
        collection_name: str,
        doc_id_field: str = "_id",
        fields_to_index: list[IndexSpec] | None = None,
        unique_indexes: list[IndexSpec] | None = None,
        name: str | None = None,
    ) -> None:
        """
        Initialize the MongoDB document store.

        :param client: Async MongoDB client instance.
        :param database_name: Name of the database to use.
        :param collection_name: Name of the collection to use.
        :param doc_id_field: Field name to use as document ID.
            Defaults to "_id".
        :param fields_to_index: List of index specifications. Each can be a single
            field name (str) or a tuple of field names for composite indexes.
        :param unique_indexes: List of index specifications that should be unique.
            Each can be a single field name (str) or a tuple of field names.
        :param name: Optional name for logging purposes.
        """
        super().__init__(pybase_logger_name=name)

        self._client = client
        self._database_name = database_name
        self._collection_name = collection_name
        self._doc_id_field = doc_id_field
        self._fields_to_index = fields_to_index if fields_to_index is not None else []
        self._unique_indexes = unique_indexes if unique_indexes is not None else []
        # Collection reference is sync in pymongo async — no I/O, just a reference.
        self._collection: AsyncCollection = self._ensure_collection()

    async def setup(self) -> None:
        """
        Create indexes on configured fields.

        Must be called after construction before using the store.
        The factory function handles this automatically.
        """
        await self._create_indexes()

    async def insert(
        self,
        doc: dict,
        overwrite_existing: bool = False,
    ) -> str:
        """
        Insert a document into the document store.

        :param doc: Document to insert.
        :param overwrite_existing: If True, replace existing document with
            same unique key. If unique_indexes are configured, uses the first
            unique index as the upsert key. Otherwise, uses the doc_id_field.

        :return: Document ID of the inserted document.

        :raises DocumentAlreadyExistsError: If overwrite_existing is False
            and a document with the same ID already exists.
        """
        doc_copy = doc.copy()
        self._ensure_doc_id(doc_copy)

        if overwrite_existing:
            if self._unique_indexes:
                await self._upsert_by_unique_index(doc_copy)
            else:
                query = {self._doc_id_field: doc_copy[self._doc_id_field]}
                await self._collection.replace_one(query, doc_copy, upsert=True)
        else:
            try:
                await self._collection.insert_one(doc_copy)
            except DuplicateKeyError as e:
                msg = (
                    f"Document with id '{self._normalize_doc_id(doc_copy[self._doc_id_field])}' "
                    "already exists"
                )
                raise DocumentAlreadyExistsError(msg) from e

        return self._normalize_doc_id(doc_copy[self._doc_id_field])

    async def update(
        self,
        doc_id: str,
        updates: dict,
    ) -> None:
        """
        Update a document with the given updates.

        :param doc_id: ID of the document to update.
        :param updates: Dictionary of field updates to apply.

        :raises ValueError: If doc_id is None, empty, or invalid format.
        :raises DocumentNotFoundError: If document with doc_id does not exist.
        """
        query = {self._doc_id_field: self._denormalize_doc_id(doc_id)}
        result = await self._collection.update_one(query, {"$set": updates})
        if result.matched_count == 0:
            msg = f"Document with id '{doc_id}' not found"
            raise DocumentNotFoundError(msg)

    async def exists(
        self,
        doc_id: str,
    ) -> bool:
        """
        Check if a document exists in the store.

        :param doc_id: ID of the document to check.

        :return: True if document exists, False otherwise.

        :raises ValueError: If doc_id is None, empty, or invalid format.
        """
        query = {self._doc_id_field: self._denormalize_doc_id(doc_id)}
        count = await self._collection.count_documents(query, limit=1)
        return count > 0

    async def delete(
        self,
        doc_id: str,
    ) -> bool:
        """
        Delete a document by its ID.

        :param doc_id: ID of the document to delete.

        :return: True if document existed and was deleted, False if
            document was not found.

        :raises ValueError: If doc_id is None, empty, or invalid format.
        """
        query = {self._doc_id_field: self._denormalize_doc_id(doc_id)}
        result = await self._collection.delete_one(query)
        return result.deleted_count > 0

    async def get_document(
        self,
        doc_id: str,
    ) -> dict:
        """
        Retrieve a document by its ID.

        :param doc_id: ID of the document to retrieve.

        :return: The document as a dictionary.

        :raises ValueError: If doc_id is None, empty, or invalid format.
        :raises DocumentNotFoundError: If document with doc_id does not exist.
        """
        query = {self._doc_id_field: self._denormalize_doc_id(doc_id)}
        doc = await self._collection.find_one(query)
        if doc is None:
            msg = f"Document with id '{doc_id}' not found"
            raise DocumentNotFoundError(msg)
        return self._prepare_doc_for_return(doc)

    async def query(
        self,
        params: dict,
    ) -> AsyncGenerator[dict, None]:
        """
        Query documents matching the given parameters.

        :param params: MongoDB query parameters.

        :return: Async generator yielding matching documents.
        """
        cursor = self._collection.find(params)
        async for doc in cursor:
            yield self._prepare_doc_for_return(doc)

    def get_document_type(self) -> str:
        """
        Returns the document type identifier for this store.

        :return: The collection name.
        """
        return self._collection_name

    def get_doc_id_field(self) -> str:
        """
        Returns the field name used for document IDs.

        :return: The configured doc_id_field.
        """
        return self._doc_id_field

    async def has_unique_index(self, index_spec: IndexSpec) -> bool:
        """
        Check if a unique index exists on the given field(s).

        :param index_spec: Single field name or tuple of field names for composite index.

        :return: True if a unique index exists on exactly these fields.
        """
        target_fields = (index_spec,) if isinstance(index_spec, str) else index_spec
        indexes = await self._collection.index_information()
        for index_info in indexes.values():
            index_keys = tuple(field for field, _ in index_info.get("key", []))
            if index_keys == target_fields and index_info.get("unique", False):
                return True
        return False

    def _ensure_collection(self) -> AsyncCollection:
        """Get or create the collection reference (no I/O)."""
        db = self._client[self._database_name]
        return db[self._collection_name]

    async def _create_indexes(self) -> None:
        """Create indexes on configured fields."""
        for index_spec in self._fields_to_index:
            keys = self._index_spec_to_keys(index_spec)
            is_unique = index_spec in self._unique_indexes
            await self._collection.create_index(keys, unique=is_unique)

        for index_spec in self._unique_indexes:
            if index_spec not in self._fields_to_index:
                keys = self._index_spec_to_keys(index_spec)
                await self._collection.create_index(keys, unique=True)

        if self._doc_id_field != "_id":
            await self._collection.create_index(self._doc_id_field, unique=True)

    def _index_spec_to_keys(self, index_spec: IndexSpec) -> list[tuple[str, int]]:
        """Convert an index specification to MongoDB index keys format."""
        if isinstance(index_spec, str):
            return [(index_spec, 1)]
        return [(field, 1) for field in index_spec]

    async def _upsert_by_unique_index(self, doc: dict) -> None:
        """
        Upsert a document using the first unique index as the query key.

        Uses find_one_and_replace with upsert=True to atomically:
        - Replace existing document if found (preserving its _id)
        - Insert new document if not found (auto-generating _id)

        Updates doc[doc_id_field] with the actual value from MongoDB after upsert.

        :param doc: Document to upsert (will be modified).

        :raises RuntimeError: If no unique indexes are configured.
        """
        if not self._unique_indexes:
            msg = "Cannot upsert by unique index: no unique_indexes configured"
            raise RuntimeError(msg)

        upsert_fields = self._unique_indexes[0]
        if isinstance(upsert_fields, str):
            upsert_fields = (upsert_fields,)

        query = {field: doc.get(field) for field in upsert_fields}

        # Remove _id to let MongoDB handle it (preserve on update, auto-generate on insert)
        # Note: custom doc_id_field (if any) is already set by _ensure_doc_id before this call
        doc.pop("_id", None)

        result = await self._collection.find_one_and_replace(
            query,
            doc,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

        # Update doc with the actual doc_id_field from MongoDB
        doc[self._doc_id_field] = result[self._doc_id_field]

    def _generate_doc_id(self) -> DocId:
        """Generate a new document ID."""
        if self._doc_id_field == "_id":
            return ObjectId()
        return str(uuid.uuid4())

    def _normalize_doc_id(self, doc_id: DocId) -> str:
        """
        Convert a document ID to its normalized string representation.

        :param doc_id: Document ID to normalize.

        :return: String representation of the document ID.
        """
        return str(doc_id)

    def _denormalize_doc_id(self, doc_id: DocId) -> DocId:
        """
        Validate and convert an ID to its native MongoDB format.

        :param doc_id: Document ID to denormalize (string or ObjectId).

        :return: Native MongoDB ID (ObjectId for _id field, string otherwise).

        :raises ValueError: If doc_id is None, empty, or invalid format.
        """
        if doc_id is None or doc_id == "":
            msg = "doc_id cannot be None or empty"
            raise ValueError(msg)

        if self._doc_id_field == "_id":
            if not ObjectId.is_valid(doc_id):
                msg = f"Invalid ObjectId format: '{doc_id}'"
                raise ValueError(msg)
            return ObjectId(doc_id)

        return doc_id

    def _ensure_doc_id(self, doc: dict) -> DocId:
        """
        Ensure document has an ID, generating one if needed.

        Modifies doc in place if ID is generated.

        :return: The document ID in its native format.
        """
        doc_id = doc.get(self._doc_id_field)
        if doc_id is not None:
            doc_id = self._denormalize_doc_id(doc_id)
        else:
            doc_id = self._generate_doc_id()

        doc[self._doc_id_field] = doc_id
        return doc_id

    def _prepare_doc_for_return(self, doc: dict) -> dict:
        """
        Prepare a document for return by cleaning up internal fields.

        Note: This method mutates the input dict.
        """
        if self._doc_id_field != "_id":
            doc.pop("_id", None)
        else:
            doc["_id"] = str(doc["_id"])
        return doc
