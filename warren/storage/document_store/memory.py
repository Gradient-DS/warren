"""In-process ``DocumentStoreInterface`` implementation."""

import copy
import uuid
from collections.abc import AsyncGenerator

from basics.base import Base

from warren.storage.document_store.interface import (
    DocumentAlreadyExistsError,
    DocumentNotFoundError,
    DocumentStoreInterface,
    IndexSpec,
)


class MemoryDocumentStore(Base, DocumentStoreInterface):
    """Document store held in a dict. Single-process, gone at exit.

    Takes the same ``doc_id_field`` / ``unique_indexes`` keywords as
    ``MongoDBDocumentStore`` so either can sit under ``DefaultResultsStore``
    and ``RetryWorker``.

    Documents are deep-copied on the way in and on the way out. A database
    gives callers that isolation for free; a dict does not, and a worker
    mutating a document it just read must not change stored state.

    ``query`` supports flat equality only, which is everything the
    framework itself issues. A query operator raises instead of silently
    matching nothing.
    """

    def __init__(
        self,
        *,
        collection_name: str,
        doc_id_field: str = "doc_id",
        unique_indexes: list[IndexSpec] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._collection_name = collection_name
        self._doc_id_field = doc_id_field
        self._unique_indexes: list[tuple[str, ...]] = [
            (spec,) if isinstance(spec, str) else tuple(spec)
            for spec in (unique_indexes or [])
        ]
        self._docs: dict[str, dict] = {}

    async def insert(self, doc: dict, overwrite_existing: bool = False) -> str:
        new = copy.deepcopy(doc)
        if new.get(self._doc_id_field) is None:
            new[self._doc_id_field] = str(uuid.uuid4())
        new_id = self._require_id(new[self._doc_id_field])

        # Same rule as the Mongo store: with unique indexes configured the
        # first one is the upsert key, otherwise the doc id is.
        upsert_key = (
            self._unique_indexes[0] if self._unique_indexes else (self._doc_id_field,)
        )
        replaced = self._find_by_fields(upsert_key, new) if overwrite_existing else None

        clashes = self._clashing_ids(new) - {replaced}
        if clashes:
            msg = f"Document with id '{new_id}' already exists"
            raise DocumentAlreadyExistsError(msg)

        if replaced is not None:
            del self._docs[replaced]
        self._docs[new_id] = new
        return new_id

    async def update(self, doc_id: str, updates: dict) -> None:
        key = self._require_id(doc_id)
        if key not in self._docs:
            msg = f"Document with id '{doc_id}' not found"
            raise DocumentNotFoundError(msg)
        self._docs[key].update(copy.deepcopy(updates))

    async def exists(self, doc_id: str) -> bool:
        return self._require_id(doc_id) in self._docs

    async def get_document(self, doc_id: str) -> dict:
        key = self._require_id(doc_id)
        if key not in self._docs:
            msg = f"Document with id '{doc_id}' not found"
            raise DocumentNotFoundError(msg)
        return copy.deepcopy(self._docs[key])

    async def query(self, params: dict) -> AsyncGenerator[dict, None]:
        for field, value in params.items():
            if field.startswith("$") or isinstance(value, dict):
                msg = (
                    f"MemoryDocumentStore.query supports flat equality only, "
                    f"got {field!r}: {value!r}"
                )
                raise ValueError(msg)

        # Snapshot first: a caller may insert or delete while iterating.
        matches = [
            doc
            for doc in self._docs.values()
            if all(doc.get(field) == value for field, value in params.items())
        ]
        for doc in matches:
            yield copy.deepcopy(doc)

    async def delete(self, doc_id: str) -> bool:
        return self._docs.pop(self._require_id(doc_id), None) is not None

    def get_document_type(self) -> str:
        return self._collection_name

    def get_doc_id_field(self) -> str:
        return self._doc_id_field

    async def has_unique_index(self, index_spec: IndexSpec) -> bool:
        target = (index_spec,) if isinstance(index_spec, str) else tuple(index_spec)
        return target == (self._doc_id_field,) or target in self._unique_indexes

    def _require_id(self, doc_id: object) -> str:
        if doc_id is None or doc_id == "":
            msg = "doc_id cannot be None or empty"
            raise ValueError(msg)
        return str(doc_id)

    def _find_by_fields(self, fields: tuple[str, ...], doc: dict) -> str | None:
        for key, stored in self._docs.items():
            if all(stored.get(f) == doc.get(f) for f in fields):
                return key
        return None

    def _clashing_ids(self, doc: dict) -> set[str]:
        """Ids of stored documents that ``doc`` would collide with."""
        constraints = [(self._doc_id_field,), *self._unique_indexes]
        return {
            key
            for key, stored in self._docs.items()
            if any(
                all(stored.get(f) == doc.get(f) for f in fields)
                for fields in constraints
            )
        }
