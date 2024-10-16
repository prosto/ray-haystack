import re
from typing import Any, List

from haystack.components.builders import ChatPromptBuilder
from haystack.components.converters import OutputAdapter
from haystack.components.generators.chat import OpenAIChatGenerator
from haystack.components.joiners import BranchJoiner
from haystack.components.routers import ConditionalRouter
from haystack.components.websearch import SerperDevWebSearch
from haystack.dataclasses import ChatMessage
from haystack.utils import Secret

from ray_haystack.ray_pipeline import RayPipeline
from ray_haystack.serialization.worker_asset import worker_asset

search_message_template = """
Given these web search results:

{% for doc in documents %}
    {{ doc.content }}
{% endfor %}

Be as brief as possible, max one sentence.
Answer the question: {{search_query}}
"""

react_message_template = """
Solve a question answering task with interleaving Thought, Action, Observation steps.

Thought reasons about the current situation

Action can be:
google_search - Searches Google for the exact concept/entity (given in square brackets) and returns the results for you to use
finish - Returns the final answer (given in square brackets) and finishes the task

Observation summarizes the Action outcome and helps in formulating the next
Thought in Thought, Action, Observation interleaving triplet of steps.

After each Observation, provide the next Thought and next Action.
Don't execute multiple steps even though you know the answer.
Only generate Thought and Action, never Observation, you'll get Observation from Action.
Follow the pattern in the examples below.

Examples:
###########################
Question: Which magazine was started first Arthur's Magazine or First for Women?
Thought: I need to search Arthur's Magazine and First for Women, and find which was started
first.
Action: google_search[When was 'Arthur's Magazine' started?]
Observation: Arthur's Magazine was an American literary periodical ˘
published in Philadelphia and founded in 1844. Edited by Timothy Shay Arthur, it featured work by
Edgar A. Poe, J.H. Ingraham, Sarah Josepha Hale, Thomas G. Spear, and others. In May 1846
it was merged into Godey's Lady's Book.
Thought: Arthur's Magazine was started in 1844. I need to search First for Women founding date next
Action: google_search[When was 'First for Women' magazine started?]
Observation: First for Women is a woman's magazine published by Bauer Media Group in the
USA. The magazine was started in 1989. It is based in Englewood Cliffs, New Jersey. In 2011
the circulation of the magazine was 1,310,696 copies.
Thought: First for Women was started in 1989. 1844 (Arthur's Magazine) ¡ 1989 (First for
Women), so Arthur's Magazine was started first.
Action: finish[Arthur's Magazine]
############################
Question: Which documentary is about Finnish rock groups, Adam Clayton Powell or The Saimaa Gesture?
Thought: I need to search Adam Clayton Powell and The Saimaa Gesture, and find which documentary is about Finnish rock groups.
Action: google_search[Adam Clayton Powell (film)]
Observation: Adam Clayton Powell is a 1989 American documentary film directed by Richard Kilberg. The film is about the rise and fall of influential African-American politician Adam Clayton Powell Jr.[3][4] It was later aired as part of the PBS series The American Experience.
Thought: Adam Clayton Powell (film) is a documentary about an African-American politician, not Finnish rock groups. So the documentary about Finnish rock groups must instead be The Saimaa Gesture.
Action: finish[The Saimaa Gesture]
############################

Let's start, the question is: {{query}}
"""  # noqa: E501

routes = [
    {
        "condition": "{{'search' in tool_id_and_param[0]}}",
        "output": "{{tool_id_and_param[1]}}",
        "output_name": "search",
        "output_type": str,
    },
    {
        "condition": "{{'finish' in tool_id_and_param[0]}}",
        "output": "{{tool_id_and_param[1]}}",
        "output_name": "finish",
        "output_type": str,
    },
]


@worker_asset
def find_last_action(messages: List[ChatMessage]):
    prompt: str = messages[-1].content
    lines = prompt.strip().split("\n")
    for line in reversed(lines):
        pattern = r"Action\s*\d*:\s*(\w+)\[(.*?)\]"

        match = re.search(pattern, line)
        if match:
            action_name = match.group(1)
            parameter = match.group(2)
            return [action_name, parameter]
    return [None, None]


@worker_asset
def concat_prompt(last_message: ChatMessage, current_prompt: List[ChatMessage], append: str):
    return [ChatMessage.from_user(current_prompt[-1].content + last_message.content + append)]


@worker_asset
def format_observation(messages: List[ChatMessage]):
    return [ChatMessage.from_assistant("Observation: " + messages[-1].content + "\n")]


@worker_asset
def identity(x: Any):
    return x


def default_pipeline_inputs():
    question = "Which tower is taller: Tower of Pisa or Eiffel Tower?"
    return {
        "react_loop_input": {"value": [ChatMessage.from_user(react_message_template)]},
        "react_prompt_builder": {"template_variables": {"query": question}},
        "search_prompt_builder": {"template": [ChatMessage.from_user(search_message_template)]},
    }


def create_pipeline():
    pipeline = RayPipeline()

    pipeline.add_component("react_loop_input", BranchJoiner(List[ChatMessage]))
    pipeline.add_component("react_prompt_builder", ChatPromptBuilder(variables=["query"]))
    pipeline.add_component(
        "thought_action_generator", OpenAIChatGenerator(model="gpt-4o", generation_kwargs={"temperature": 0})
    )

    # tools
    pipeline.add_component(
        "action_extractor",
        OutputAdapter(
            "{{messages | find_action}}",
            output_type=List[str],
            custom_filters={"find_action": find_last_action},
            unsafe=True,
        ),
    )

    pipeline.add_component(
        "prompt_concatenator_after_action",
        OutputAdapter(
            "{{replies[-1] | concat_prompt(current_prompt,'')}}",
            output_type=List[ChatMessage],
            custom_filters={"concat_prompt": concat_prompt},
            unsafe=True,
        ),
    )

    pipeline.add_component("action_router", ConditionalRouter(routes, unsafe=True))
    pipeline.add_component("search_tool", SerperDevWebSearch(api_key=Secret.from_env_var("SERPERDEV_API_KEY")))
    pipeline.add_component("search_prompt_builder", ChatPromptBuilder(variables=["documents", "search_query"]))
    pipeline.add_component(
        "observation_generator", OpenAIChatGenerator(model="gpt-4o", generation_kwargs={"temperature": 0})
    )
    pipeline.add_component(
        "final_output_formatter",
        OutputAdapter(
            "{{final_answer | format_final_answer}}",
            output_type=str,
            custom_filters={"format_final_answer": identity},
            unsafe=True,
        ),
    )

    pipeline.add_component(
        "observation_adapter",
        OutputAdapter(
            "{{search_replies | format_observation}}",
            output_type=List[ChatMessage],
            custom_filters={"format_observation": format_observation},
            unsafe=True,
        ),
    )

    pipeline.add_component(
        "prompt_concatenator_after_observation",
        OutputAdapter(
            "{{replies[-1] | concat_prompt(current_prompt, 'Thought:')}}",
            output_type=List[ChatMessage],
            custom_filters={"concat_prompt": concat_prompt},
            unsafe=True,
        ),
    )

    # main
    pipeline.connect("react_loop_input", "react_prompt_builder.template")
    pipeline.connect("react_prompt_builder.prompt", "thought_action_generator.messages")
    pipeline.connect("thought_action_generator.replies", "prompt_concatenator_after_action.replies")

    # tools
    pipeline.connect("react_prompt_builder.prompt", "prompt_concatenator_after_action.current_prompt")
    pipeline.connect("prompt_concatenator_after_action", "action_extractor.messages")

    pipeline.connect("action_extractor", "action_router")
    pipeline.connect("action_router.search", "search_tool.query")
    pipeline.connect("search_tool.documents", "search_prompt_builder.documents")
    pipeline.connect("action_router.search", "search_prompt_builder.search_query")
    pipeline.connect("search_prompt_builder.prompt", "observation_generator.messages")
    pipeline.connect("action_router.finish", "final_output_formatter")

    pipeline.connect("observation_generator.replies", "observation_adapter.search_replies")
    pipeline.connect("observation_adapter", "prompt_concatenator_after_observation.replies")
    pipeline.connect("prompt_concatenator_after_action", "prompt_concatenator_after_observation.current_prompt")

    pipeline.connect("prompt_concatenator_after_observation", "react_loop_input")

    return pipeline
