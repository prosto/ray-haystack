import uuid
from typing import Any, Dict, List, Literal, Optional

import ray
import ray.actor
from haystack import default_from_dict, default_to_dict
from haystack.core.errors import DeserializationError, SerializationError
from haystack.dataclasses import Document
from haystack.document_stores.in_memory import InMemoryDocumentStore
from haystack.document_stores.types import DocumentStore, DuplicatePolicy

from ray_haystack.serialization.generic_wrapper import GenericWrapper


class RayInMemoryDocumentStore(InMemoryDocumentStore):
    def __init__(
        self,
        bm25_tokenization_regex: str = r"(?u)\b\w\w+\b",
        bm25_algorithm: Literal["BM25Okapi", "BM25L", "BM25Plus"] = "BM25L",
        bm25_parameters: Optional[Dict] = None,
        embedding_similarity_function: Literal["dot_product", "cosine"] = "dot_product",
        index: Optional[str] = None,
        *,
        actor_options: Optional[Dict] = None,
        actor_name: Optional[str] = None,
    ):
        if actor_name:
            self._actor_name = actor_name
            self._actor = ray.get_actor(actor_name)
        else:
            doc_store = InMemoryDocumentStore(
                bm25_tokenization_regex, bm25_algorithm, bm25_parameters, embedding_similarity_function, index
            )

            actor_options = actor_options or {}
            if "name" in actor_options:
                self._actor_name = actor_options["name"]
            else:
                self._actor_name = f"RayInMemoryDocumentStore_{uuid.uuid4()!s}"
                actor_options["name"] = self._actor_name

            self._actor = self.create_actor(doc_store, actor_options)

    def to_dict(self) -> Dict[str, Any]:
        return default_to_dict(self, actor_name=self._actor_name)

    @classmethod
    def create_actor(cls, doc_store: InMemoryDocumentStore, actor_options: Dict[str, Any]):
        doc_store_wrapper: GenericWrapper[InMemoryDocumentStore] = GenericWrapper(obj=doc_store)
        return ray.remote(_DocumentStoreActor).options(**actor_options).remote(doc_store_wrapper)  # type:ignore

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RayInMemoryDocumentStore":
        proxy = default_from_dict(cls, data)
        return proxy

    def count_documents(self) -> int:
        return ray.get(self._actor.count_documents.remote())

    def filter_documents(self, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        return ray.get(self._actor.filter_documents.remote(filters))

    def write_documents(self, documents: List[Document], policy: DuplicatePolicy = DuplicatePolicy.NONE) -> int:
        return ray.get(self._actor.write_documents.remote(documents, policy))

    def delete_documents(self, document_ids: List[str]) -> None:
        return ray.get(self._actor.delete_documents.remote(document_ids))

    def embedding_retrieval(
        self,
        query_embedding: List[float],
        filters: Optional[Dict[str, Any]] = None,
        top_k: int = 10,
        scale_score: bool = False,
        return_embedding: bool = False,
    ) -> List[Document]:
        return ray.get(
            self._actor.embedding_retrieval.remote(query_embedding, filters, top_k, scale_score, return_embedding)
        )

    def bm25_retrieval(
        self, query: str, filters: Optional[Dict[str, Any]] = None, top_k: int = 10, scale_score: bool = False
    ) -> List[Document]:
        return ray.get(self._actor.bm25_retrieval.remote(query, filters, top_k, scale_score))


class _DocumentStoreActor(DocumentStore):
    def __init__(self, wrapper: GenericWrapper[InMemoryDocumentStore]):
        self._doc_store = wrapper.get_obj()

    def to_dict(self) -> Dict[str, Any]:
        raise SerializationError("We are not supposed to serialize actor instance")

    @classmethod
    def from_dict(cls, _data: Dict[str, Any]) -> InMemoryDocumentStore:
        raise DeserializationError("We are not supposed to deserialize actor instance")

    def count_documents(self) -> int:
        return self._doc_store.count_documents()

    def filter_documents(self, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        return self._doc_store.filter_documents(filters)

    def write_documents(self, documents: List[Document], policy: DuplicatePolicy = DuplicatePolicy.NONE) -> int:
        return self._doc_store.write_documents(documents, policy)

    def delete_documents(self, document_ids: List[str]) -> None:
        return self._doc_store.delete_documents(document_ids)

    def embedding_retrieval(
        self,
        query_embedding: List[float],
        filters: Optional[Dict[str, Any]] = None,
        top_k: int = 10,
        scale_score: bool = False,
        return_embedding: bool = False,
    ) -> List[Document]:
        return self._doc_store.embedding_retrieval(query_embedding, filters, top_k, scale_score, return_embedding)

    def bm25_retrieval(
        self, query: str, filters: Optional[Dict[str, Any]] = None, top_k: int = 10, scale_score: bool = False
    ) -> List[Document]:
        return self._doc_store.bm25_retrieval(query, filters, top_k, scale_score)
