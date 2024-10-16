from typing import Any, Dict

from haystack import component, default_from_dict
from haystack.components.retrievers.in_memory import InMemoryBM25Retriever
from haystack.document_stores.types import FilterPolicy
from haystack.utils import deserialize_document_store_in_init_params_inplace


@component
class RayInMemoryBM25Retriever(InMemoryBM25Retriever):
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RayInMemoryBM25Retriever":
        init_params = data.get("init_parameters", {})

        if "filter_policy" in init_params:
            init_params["filter_policy"] = FilterPolicy.from_str(init_params["filter_policy"])

        deserialize_document_store_in_init_params_inplace(data)

        return default_from_dict(cls, data)
