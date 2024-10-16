import json
import logging
from typing import List, Optional, Type

import pydantic
from haystack import component
from haystack.components.builders import PromptBuilder
from haystack.components.converters import OutputAdapter
from haystack.components.generators import OpenAIGenerator
from pydantic import BaseModel, ValidationError

from ray_haystack.ray_pipeline import RayPipeline
from ray_haystack.serialization import worker_asset


def setup_logger():
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


logger = setup_logger()


class City(BaseModel):
    name: str
    country: str
    population: int


class CitiesData(BaseModel):
    cities: List[City]


@component
class OutputValidator:
    def __init__(self, pydantic_model: Type[pydantic.BaseModel]):
        self.pydantic_model = pydantic_model
        self.iteration_counter = 0

    @component.output_types(valid_replies=List[str], invalid_replies=Optional[List[str]], error_message=Optional[str])
    def run(self, replies: List[str]):
        self.iteration_counter += 1

        try:
            output_dict = json.loads(replies[0])
            self.pydantic_model.model_validate(output_dict)
            logger.info(
                f"OutputValidator at Iteration {self.iteration_counter}: "
                f"Valid JSON from LLM - No need for looping: {replies[0]}"
            )
            return {"valid_replies": replies}

        except (ValueError, ValidationError) as e:
            logger.info(
                f"OutputValidator at Iteration {self.iteration_counter}: Invalid JSON from LLM - Let's try again.\n"
                f"Output from LLM:\n {replies[0]} \n"
                f"Error from OutputValidator: {e}"
            )
            return {"invalid_replies": replies, "error_message": str(e)}


prompt_template = """
Create a JSON object from the information present in this passage: {{passage}}.
Only use information that is present in the passage. Follow this JSON schema, but only return the actual instances
without any additional schema definition:
{{schema}}
Make sure your response is a dict and not a list.
{% if invalid_replies and error_message %}
  You already created the following output in a previous attempt: {{invalid_replies}}
  However, this doesn't comply with the format requirements from above and triggered this Python exception:
    {{error_message}}
  Correct the output and try again. Just return the corrected output without any extra explanations.
{% endif %}
"""


@worker_asset
def parse_response(valid_reply: str):
    return json.loads(valid_reply)


pipeline = RayPipeline(max_runs_per_component=5)

pipeline.add_component("prompt_builder", PromptBuilder(template=prompt_template))
pipeline.add_component("llm", OpenAIGenerator(model="gpt-4o-mini"))
pipeline.add_component("output_validator", OutputValidator(pydantic_model=CitiesData))
pipeline.add_component(
    "answer_adapter",
    OutputAdapter(
        template="{{ valid_replies[0] | parse_response}}",
        output_type=str,
        custom_filters={"parse_response": parse_response},
    ),
)

pipeline.connect("prompt_builder", "llm")
pipeline.connect("llm", "output_validator")

pipeline.connect("output_validator.valid_replies", "answer_adapter")
pipeline.connect("output_validator.invalid_replies", "prompt_builder.invalid_replies")
pipeline.connect("output_validator.error_message", "prompt_builder.error_message")

if __name__ == "__main__":
    result = pipeline.run(
        {
            "prompt_builder": {
                "passage": "Berlin is the capital of Germany. It has a population of 3,850,809. Paris, France's \
                            capital, has 2.161 million residents. Lisbon is the capital and the largest city of \
                            Portugal with the population of 504,718.",
                "schema": json.dumps(CitiesData.model_json_schema(), indent=2),
            }
        }
    )

    logger.info(f'RESULT: {result["answer_adapter"]["output"]}')
