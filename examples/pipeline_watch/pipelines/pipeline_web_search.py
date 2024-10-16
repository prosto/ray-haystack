from haystack.components.builders.prompt_builder import PromptBuilder
from haystack.components.generators import OpenAIGenerator
from haystack.components.routers import ConditionalRouter
from haystack.components.websearch.serper_dev import SerperDevWebSearch
from haystack.dataclasses import Document

from ray_haystack.ray_pipeline import RayPipeline

documents = [
    Document(
        content="""Munich, the vibrant capital of Bavaria in southern Germany, exudes a perfect blend of rich cultural
heritage and modern urban sophistication. Nestled along the banks of the Isar River, Munich is renowned
for its splendid architecture, including the iconic Neues Rathaus (New Town Hall) at Marienplatz and
the grandeur of Nymphenburg Palace. The city is a haven for art enthusiasts, with world-class museums like the
Alte Pinakothek housing masterpieces by renowned artists. Munich is also famous for its lively beer gardens, where
locals and tourists gather to enjoy the city's famed beers and traditional Bavarian cuisine. The city's annual
Oktoberfest celebration, the world's largest beer festival, attracts millions of visitors from around the globe.
Beyond its cultural and culinary delights, Munich offers picturesque parks like the English Garden, providing a
serene escape within the heart of the bustling metropolis. Visitors are charmed by Munich's warm hospitality,
making it a must-visit destination for travelers seeking a taste of both old-world charm and contemporary allure."""
    )
]


def default_pipeline_inputs():
    query = "How many people live in Munich?"
    return {"prompt_builder": {"query": query, "documents": documents}, "router": {"query": query}}


def create_pipeline():
    prompt_template = """
    Answer the following query given the documents.
    If the answer is not contained within the documents reply with 'no_answer'
    Query: {{query}}
    Documents:
    {% for document in documents %}
    {{document.content}}
    {% endfor %}
    """

    prompt_for_websearch = """
    Answer the following query given the documents retrieved from the web.
    Your answer should indicate that your answer was generated from websearch.

    Query: {{query}}
    Documents:
    {% for document in documents %}
    {{document.content}}
    {% endfor %}
    """

    routes = [
        {
            "condition": "{{'no_answer' in replies[0]}}",
            "output": "{{query}}",
            "output_name": "go_to_websearch",
            "output_type": str,
        },
        {
            "condition": "{{'no_answer' not in replies[0]}}",
            "output": "{{replies[0]}}",
            "output_name": "answer",
            "output_type": str,
        },
    ]

    pipeline = RayPipeline()
    pipeline.add_component("prompt_builder", PromptBuilder(template=prompt_template))
    pipeline.add_component("llm", OpenAIGenerator())
    pipeline.add_component("router", ConditionalRouter(routes))
    pipeline.add_component("websearch", SerperDevWebSearch())
    pipeline.add_component("prompt_builder_for_websearch", PromptBuilder(template=prompt_for_websearch))
    pipeline.add_component("llm_for_websearch", OpenAIGenerator())

    pipeline.connect("prompt_builder", "llm")
    pipeline.connect("llm.replies", "router.replies")
    pipeline.connect("router.go_to_websearch", "websearch.query")
    pipeline.connect("router.go_to_websearch", "prompt_builder_for_websearch.query")
    pipeline.connect("websearch.documents", "prompt_builder_for_websearch.documents")
    pipeline.connect("prompt_builder_for_websearch", "llm_for_websearch")

    return pipeline


if __name__ == "__main__":
    import os
    from getpass import getpass

    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = getpass("Enter OpenAI API key:")
    if "SERPERDEV_API_KEY" not in os.environ:
        os.environ["SERPERDEV_API_KEY"] = getpass("Enter Serper Api key: ")

    pipeline_inputs = default_pipeline_inputs()

    pipeline = create_pipeline()

    result = pipeline.run(pipeline_inputs)

    # Print the `replies` generated using the web searched Documents
    print(result["llm_for_websearch"]["replies"])  # noqa: T201
