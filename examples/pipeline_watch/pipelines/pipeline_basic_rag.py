from datasets import load_dataset
from haystack.components.builders.prompt_builder import PromptBuilder
from haystack.components.embedders import (
    SentenceTransformersDocumentEmbedder,
    SentenceTransformersTextEmbedder,
)
from haystack.components.generators import OpenAIGenerator
from haystack.dataclasses import Document

from ray_haystack.components import (
    RayInMemoryDocumentStore,
    RayInMemoryEmbeddingRetriever,
)
from ray_haystack.ray_pipeline import RayPipeline


def prepare_document_store(document_store: RayInMemoryDocumentStore):
    doc_embedder = SentenceTransformersDocumentEmbedder(model="sentence-transformers/all-MiniLM-L6-v2")
    doc_embedder.warm_up()

    dataset = load_dataset("bilgeyucel/seven-wonders", split="train")
    docs = [Document(content=doc["content"], meta=doc["meta"]) for doc in dataset]

    docs_with_embeddings = doc_embedder.run(docs)
    document_store.write_documents(docs_with_embeddings["documents"])


def default_pipeline_inputs():
    question = "What does Rhodes Statue look like?"
    return {"text_embedder": {"text": question}, "prompt_builder": {"question": question}}


def create_pipeline():
    template = """
    Given the following information, answer the question.

    Context:
    {% for document in documents %}
        {{ document.content }}
    {% endfor %}

    Question: {{question}}
    Answer:
    """

    document_store = RayInMemoryDocumentStore()

    prepare_document_store(document_store)

    basic_rag_pipeline = RayPipeline()

    basic_rag_pipeline.add_component(
        "text_embedder", SentenceTransformersTextEmbedder(model="sentence-transformers/all-MiniLM-L6-v2")
    )
    basic_rag_pipeline.add_component("retriever", RayInMemoryEmbeddingRetriever(document_store))
    basic_rag_pipeline.add_component("prompt_builder", PromptBuilder(template=template))
    basic_rag_pipeline.add_component("llm", OpenAIGenerator())

    # Now, connect the components to each other
    basic_rag_pipeline.connect("text_embedder.embedding", "retriever.query_embedding")
    basic_rag_pipeline.connect("retriever", "prompt_builder.documents")
    basic_rag_pipeline.connect("prompt_builder", "llm")

    return basic_rag_pipeline


if __name__ == "__main__":
    import os
    from getpass import getpass

    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = getpass("Enter OpenAI API key:")

    pipeline_inputs = default_pipeline_inputs()

    pipeline = create_pipeline()

    result = pipeline.run(pipeline_inputs)

    print(result["llm"]["replies"][0])  # noqa: T201
