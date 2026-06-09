from typing import Protocol, Union

from collections.abc import AsyncGenerator

from warren.storage.exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)


# Type alias for index specification (single field or composite)
IndexSpec = Union[str, tuple[str, ...]]  # noqa: UP007  # runtime type alias


class DocumentNotFoundError(ResourceNotFoundError):
    pass


class DocumentAlreadyExistsError(ResourceAlreadyExistsError):
    pass


class DocumentStoreInterface(Protocol):
    """
    Async document store interface.

    All I/O methods are async — implementations must use non-blocking drivers
    (e.g., pymongo.AsyncMongoClient) to avoid blocking the event loop.

    Accessor methods (get_document_type, get_doc_id_field) are sync because
    they return stored config values with no I/O.
    """

    async def insert(self, doc: dict, overwrite_existing: bool = False) -> str:
        """
        Insert a document into the document store and return its ID.

        :param doc: Document to insert.
        :param overwrite_existing: Allow overwrite of existing document if True.

        :return: Document ID.

        :raises DocumentAlreadyExistsError: If overwrite_existing is False and
            a document with the same ID already exists.
        """
        ...

    async def update(self, doc_id: str, updates: dict) -> None:
        """
        Update document with document id with given updates.

        :param doc_id: Document ID.
        :param updates: Dictionary with updates.

        :raises DocumentNotFoundError: if document id does not exist.
        """
        ...

    async def exists(
        self,
        doc_id: str,
    ) -> bool:
        """
        Check if document id exists.

        :param doc_id: Document ID.

        :return: True if document id exists.
        """
        ...

    async def get_document(
        self,
        doc_id: str,
    ) -> dict:
        """
        Get document with document id.

        :param doc_id: Document ID.

        :return: Document dict.

        :raises DocumentNotFoundError: if document id does not exist.
        """
        ...

    def query(self, params: dict) -> AsyncGenerator[dict, None]:
        """
        Query document store.

        Returns an async generator — callers use ``async for doc in store.query(params)``.

        :param params: Query parameters.

        :return: Async generator of documents found.
        """
        ...

    async def delete(
        self,
        doc_id: str,
    ) -> bool:
        """
        Delete a document by its ID.

        :param doc_id: Document ID.

        :return: True if document existed and was deleted, False if
            document was not found.
        """
        ...

    def get_document_type(self) -> str:
        """
        Returns the document type identifier for this store.

        :return: Document type identifier (e.g., collection name).
        """
        ...

    def get_doc_id_field(self) -> str:
        """
        Returns the field name used for document IDs.

        :return: Field name containing document IDs.
        """
        ...

    async def has_unique_index(self, index_spec: IndexSpec) -> bool:
        """
        Check if a unique index exists on the given field(s).

        :param index_spec: Single field name or tuple of field names for composite index.

        :return: True if a unique index exists on exactly these fields.
        """
        ...
