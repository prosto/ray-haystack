import re
from typing import Any, Dict, List, Optional

import pytest
from haystack import Document, component
from haystack.components.builders import AnswerBuilder, ChatPromptBuilder, PromptBuilder
from haystack.components.joiners import BranchJoiner, DocumentJoiner
from haystack.components.routers import ConditionalRouter
from haystack.core.errors import PipelineMaxComponentRuns, PipelineRuntimeError
from haystack.dataclasses import ChatMessage, GeneratedAnswer
from haystack.testing.factory import component_class

from ray_haystack import RayPipeline
from ray_haystack.components import RayInMemoryBM25Retriever, RayInMemoryDocumentStore

from .sample_components import (
    Accumulate,
    AddFixedValue,
    Double,
    FString,
    Greet,
    Hello,
    Parity,
    Remainder,
    Repeat,
    SelfLoop,
    StringListJoiner,
    Subtract,
    Sum,
    TextSplitter,
    Threshold,
)
from .utils import HashableDocument, run_pipeline


def test_has_no_components():
    pipeline = RayPipeline()
    inputs = {}
    expected_outputs = {}

    outputs = pipeline.run(inputs)

    assert outputs == expected_outputs


def test_pipeline_is_linear():
    pipeline = RayPipeline()
    pipeline.add_component("first_addition", AddFixedValue(add=2))
    pipeline.add_component("second_addition", AddFixedValue())
    pipeline.add_component("double", Double())
    pipeline.connect("first_addition", "double")
    pipeline.connect("double", "second_addition")

    outputs, _ = run_pipeline(pipeline, {"first_addition": {"value": 1}})

    assert outputs == {"second_addition": {"result": 7}}


def test_pipeline_that_has_an_infinite_loop():
    @component
    class FakeComponent:
        @component.output_types(a=int, b=int)
        def run(self, x: int, y: int = 1):
            return {"a": 1, "b": 1}

    pipe = RayPipeline(max_runs_per_component=2)
    pipe.add_component("first", FakeComponent())
    pipe.add_component("second", FakeComponent())
    pipe.connect("first.a", "second.x")
    pipe.connect("second.b", "first.y")

    with pytest.raises(PipelineMaxComponentRuns):
        pipe.run({"first": {"x": 1}})


def test_pipeline_complex():
    pipeline = RayPipeline(max_runs_per_component=2)
    pipeline.add_component("greet_first", Greet(message="Hello, the value is {value}."))
    pipeline.add_component("accumulate_1", Accumulate())
    pipeline.add_component("add_two", AddFixedValue(add=2))
    pipeline.add_component("parity", Parity())
    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("accumulate_2", Accumulate())

    pipeline.add_component("branch_joiner", BranchJoiner(type_=int))
    pipeline.add_component("below_10", Threshold(threshold=10))
    pipeline.add_component("double", Double())

    pipeline.add_component("greet_again", Greet(message="Hello again, now the value is {value}."))
    pipeline.add_component("sum", Sum())

    pipeline.add_component("greet_enumerator", Greet(message="Hello from enumerator, here the value became {value}."))
    pipeline.add_component("enumerate", Repeat(outputs=["first", "second"]))
    pipeline.add_component("add_three", AddFixedValue(add=3))

    pipeline.add_component("diff", Subtract())
    pipeline.add_component("greet_one_last_time", Greet(message="Bye bye! The value here is {value}!"))
    pipeline.add_component("replicate", Repeat(outputs=["first", "second"]))
    pipeline.add_component("add_five", AddFixedValue(add=5))
    pipeline.add_component("add_four", AddFixedValue(add=4))
    pipeline.add_component("accumulate_3", Accumulate())

    pipeline.connect("greet_first", "accumulate_1")
    pipeline.connect("accumulate_1", "add_two")
    pipeline.connect("add_two", "parity")

    pipeline.connect("parity.even", "greet_again")
    pipeline.connect("greet_again", "sum.values")
    pipeline.connect("sum", "diff.first_value")
    pipeline.connect("diff", "greet_one_last_time")
    pipeline.connect("greet_one_last_time", "replicate")
    pipeline.connect("replicate.first", "add_five.value")
    pipeline.connect("replicate.second", "add_four.value")
    pipeline.connect("add_four", "accumulate_3")

    pipeline.connect("parity.odd", "add_one.value")
    pipeline.connect("add_one", "branch_joiner.value")
    pipeline.connect("branch_joiner", "below_10")

    pipeline.connect("below_10.below", "double")
    pipeline.connect("double", "branch_joiner.value")

    pipeline.connect("below_10.above", "accumulate_2")
    pipeline.connect("accumulate_2", "diff.second_value")

    pipeline.connect("greet_enumerator", "enumerate")
    pipeline.connect("enumerate.second", "sum.values")

    pipeline.connect("enumerate.first", "add_three.value")
    pipeline.connect("add_three", "sum.values")

    outputs, _ = run_pipeline(pipeline, {"greet_first": {"value": 1}, "greet_enumerator": {"value": 1}})
    assert outputs == {"accumulate_3": {"value": -7}, "add_five": {"result": -6}}


def test_pipeline_that_has_a_single_component_with_a_default_input():
    @component
    class WithDefault:
        @component.output_types(b=int)
        def run(self, a: int, b: int = 2):
            return {"c": a + b}

    pipeline = RayPipeline()
    pipeline.add_component("with_defaults", WithDefault())

    outputs = pipeline.run({"with_defaults": {"a": 40, "b": 30}})
    assert outputs == {"with_defaults": {"c": 70}}

    outputs = pipeline.run({"with_defaults": {"a": 40}})
    assert outputs == {"with_defaults": {"c": 42}}


def test_pipeline_that_has_two_loops_of_identical_lengths():
    pipeline = RayPipeline(max_runs_per_component=10)
    pipeline.add_component("branch_joiner", BranchJoiner(type_=int))
    pipeline.add_component("remainder", Remainder(divisor=3))
    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("add_two", AddFixedValue(add=2))

    pipeline.connect("branch_joiner.value", "remainder.value")
    pipeline.connect("remainder.remainder_is_1", "add_two.value")
    pipeline.connect("remainder.remainder_is_2", "add_one.value")
    pipeline.connect("add_two", "branch_joiner.value")
    pipeline.connect("add_one", "branch_joiner.value")

    outputs, _ = run_pipeline(pipeline, {"branch_joiner": {"value": 0}})
    assert outputs == {"remainder": {"remainder_is_0": 0}}

    outputs, _ = run_pipeline(pipeline, {"branch_joiner": {"value": 3}})
    assert outputs == {"remainder": {"remainder_is_0": 3}}

    outputs, _ = run_pipeline(pipeline, {"branch_joiner": {"value": 4}})
    assert outputs == {"remainder": {"remainder_is_0": 6}}

    outputs, _ = run_pipeline(pipeline, {"branch_joiner": {"value": 5}})
    assert outputs == {"remainder": {"remainder_is_0": 6}}

    outputs, _ = run_pipeline(pipeline, {"branch_joiner": {"value": 6}})
    assert outputs == {"remainder": {"remainder_is_0": 6}}


def test_pipeline_that_has_two_loops_of_different_lengths():
    pipeline = RayPipeline(max_runs_per_component=10)
    pipeline.add_component("branch_joiner", BranchJoiner(type_=int))
    pipeline.add_component("remainder", Remainder(divisor=3))
    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("add_two_1", AddFixedValue(add=1))
    pipeline.add_component("add_two_2", AddFixedValue(add=1))

    pipeline.connect("branch_joiner.value", "remainder.value")
    pipeline.connect("remainder.remainder_is_1", "add_two_1.value")
    pipeline.connect("add_two_1", "add_two_2.value")
    pipeline.connect("add_two_2", "branch_joiner")
    pipeline.connect("remainder.remainder_is_2", "add_one.value")
    pipeline.connect("add_one", "branch_joiner")

    (outputs, run_order) = run_pipeline(pipeline, {"branch_joiner": {"value": 0}})
    assert outputs == {"remainder": {"remainder_is_0": 0}}
    assert run_order == ["branch_joiner", "remainder"]

    (outputs, run_order) = run_pipeline(pipeline, {"branch_joiner": {"value": 3}})
    assert outputs == {"remainder": {"remainder_is_0": 3}}
    assert run_order == ["branch_joiner", "remainder"]

    (outputs, run_order) = run_pipeline(pipeline, {"branch_joiner": {"value": 4}})
    assert outputs == {"remainder": {"remainder_is_0": 6}}
    assert run_order == [
        "branch_joiner",
        "remainder",
        "add_two_1",
        "add_two_2",
        "branch_joiner",
        "remainder",
    ]

    (outputs, run_order) = run_pipeline(pipeline, {"branch_joiner": {"value": 5}})
    assert outputs == {"remainder": {"remainder_is_0": 6}}
    assert run_order == ["branch_joiner", "remainder", "add_one", "branch_joiner", "remainder"]

    (outputs, run_order) = run_pipeline(pipeline, {"branch_joiner": {"value": 6}})
    assert outputs == {"remainder": {"remainder_is_0": 6}}
    assert run_order == ["branch_joiner", "remainder"]


def test_pipeline_that_has_a_single_loop_with_two_conditional_branches():
    accumulator = Accumulate()
    pipeline = RayPipeline(max_runs_per_component=10)

    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("branch_joiner", BranchJoiner(type_=int))
    pipeline.add_component("below_10", Threshold(threshold=10))
    pipeline.add_component("below_5", Threshold(threshold=5))
    pipeline.add_component("add_three", AddFixedValue(add=3))
    pipeline.add_component("accumulator", accumulator)
    pipeline.add_component("add_two", AddFixedValue(add=2))

    pipeline.connect("add_one.result", "branch_joiner")
    pipeline.connect("branch_joiner.value", "below_10.value")
    pipeline.connect("below_10.below", "accumulator.value")
    pipeline.connect("accumulator.value", "below_5.value")
    pipeline.connect("below_5.above", "add_three.value")
    pipeline.connect("below_5.below", "branch_joiner")
    pipeline.connect("add_three.result", "branch_joiner")
    pipeline.connect("below_10.above", "add_two.value")

    (outputs, run_order) = run_pipeline(pipeline, {"add_one": {"value": 3}})
    assert outputs == {"add_two": {"result": 13}}
    assert run_order == [
        "add_one",
        "branch_joiner",
        "below_10",
        "accumulator",
        "below_5",
        "branch_joiner",
        "below_10",
        "accumulator",
        "below_5",
        "add_three",
        "branch_joiner",
        "below_10",
        "add_two",
    ]


def test_pipeline_that_has_a_component_with_dynamic_inputs_defined_in_init():
    pipeline = RayPipeline()
    pipeline.add_component("hello", Hello())
    pipeline.add_component("fstring", FString(template="This is the greeting: {greeting}!", variables=["greeting"]))
    pipeline.add_component("splitter", TextSplitter())
    pipeline.connect("hello.output", "fstring.greeting")
    pipeline.connect("fstring.string", "splitter.sentence")

    outputs = pipeline.run({"hello": {"word": "Alice"}})
    assert outputs == {"splitter": {"output": ["This", "is", "the", "greeting:", "Hello,", "Alice!!"]}}

    outputs = pipeline.run({"hello": {"word": "Alice"}, "fstring": {"template": "Received: {greeting}"}})
    assert outputs == {"splitter": {"output": ["Received:", "Hello,", "Alice!"]}}


def test_pipeline_that_has_two_branches_that_dont_merge():
    pipeline = RayPipeline()
    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("parity", Parity())
    pipeline.add_component("add_ten", AddFixedValue(add=10))
    pipeline.add_component("double", Double())
    pipeline.add_component("add_three", AddFixedValue(add=3))

    pipeline.connect("add_one.result", "parity.value")
    pipeline.connect("parity.even", "add_ten.value")
    pipeline.connect("parity.odd", "double.value")
    pipeline.connect("add_ten.result", "add_three.value")

    outputs, run_order = run_pipeline(pipeline, {"add_one": {"value": 1}})
    assert outputs == {"add_three": {"result": 15}}
    assert run_order == ["add_one", "parity", "add_ten", "add_three"]

    outputs, run_order = run_pipeline(pipeline, {"add_one": {"value": 2}})
    assert outputs == {"double": {"value": 6}}
    assert run_order == ["add_one", "parity", "double"]


def test_pipeline_that_has_three_branches_that_dont_merge():
    pipeline = RayPipeline()
    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("repeat", Repeat(outputs=["first", "second"]))
    pipeline.add_component("add_ten", AddFixedValue(add=10))
    pipeline.add_component("double", Double())
    pipeline.add_component("add_three", AddFixedValue(add=3))
    pipeline.add_component("add_one_again", AddFixedValue(add=1))

    pipeline.connect("add_one.result", "repeat.value")
    pipeline.connect("repeat.first", "add_ten.value")
    pipeline.connect("repeat.second", "double.value")
    pipeline.connect("repeat.second", "add_three.value")
    pipeline.connect("add_three.result", "add_one_again.value")

    outputs = pipeline.run({"add_one": {"value": 1}})
    assert outputs == {"add_one_again": {"result": 6}, "add_ten": {"result": 12}, "double": {"value": 4}}


def test_pipeline_that_has_two_branches_that_merge():
    pipeline = RayPipeline()
    pipeline.add_component("first_addition", AddFixedValue(add=2))
    pipeline.add_component("second_addition", AddFixedValue(add=2))
    pipeline.add_component("third_addition", AddFixedValue(add=2))
    pipeline.add_component("diff", Subtract())
    pipeline.add_component("fourth_addition", AddFixedValue(add=1))

    pipeline.connect("first_addition.result", "second_addition.value")
    pipeline.connect("second_addition.result", "diff.first_value")
    pipeline.connect("third_addition.result", "diff.second_value")
    pipeline.connect("diff", "fourth_addition.value")

    outputs = pipeline.run({"first_addition": {"value": 1}, "third_addition": {"value": 1}})
    assert outputs == {"fourth_addition": {"result": 3}}


def test_pipeline_that_has_different_combinations_of_branches_that_merge_and_do_not_merge():
    pipeline = RayPipeline()
    pipeline.add_component("add_one", AddFixedValue())
    pipeline.add_component("parity", Parity())
    pipeline.add_component("add_ten", AddFixedValue(add=10))
    pipeline.add_component("double", Double())
    pipeline.add_component("add_four", AddFixedValue(add=4))
    pipeline.add_component("add_two", AddFixedValue())
    pipeline.add_component("add_two_as_well", AddFixedValue())
    pipeline.add_component("diff", Subtract())

    pipeline.connect("add_one.result", "parity.value")
    pipeline.connect("parity.even", "add_four.value")
    pipeline.connect("parity.odd", "double.value")
    pipeline.connect("add_ten.result", "diff.first_value")
    pipeline.connect("double.value", "diff.second_value")
    pipeline.connect("parity.odd", "add_ten.value")
    pipeline.connect("add_four.result", "add_two.value")
    pipeline.connect("add_four.result", "add_two_as_well.value")

    outputs = pipeline.run({"add_one": {"value": 1}, "add_two": {"add": 2}, "add_two_as_well": {"add": 2}})
    assert outputs == {"add_two": {"result": 8}, "add_two_as_well": {"result": 8}}

    outputs = pipeline.run({"add_one": {"value": 2}, "add_two": {"add": 2}, "add_two_as_well": {"add": 2}})
    assert outputs == {"diff": {"difference": 7}}


def test_pipeline_that_has_two_branches_one_of_which_loops_back():
    pipeline = RayPipeline(max_runs_per_component=10)
    pipeline.add_component("add_zero", AddFixedValue(add=0))
    pipeline.add_component("branch_joiner", BranchJoiner(type_=int))
    pipeline.add_component("sum", Sum())
    pipeline.add_component("below_10", Threshold(threshold=10))
    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("counter", Accumulate())
    pipeline.add_component("add_two", AddFixedValue(add=2))

    pipeline.connect("add_zero", "branch_joiner.value")
    pipeline.connect("branch_joiner", "below_10.value")
    pipeline.connect("below_10.below", "add_one.value")
    pipeline.connect("add_one.result", "counter.value")
    pipeline.connect("counter.value", "branch_joiner.value")
    pipeline.connect("below_10.above", "add_two.value")
    pipeline.connect("add_two.result", "sum.values")

    outputs, run_order = run_pipeline(pipeline, {"add_zero": {"value": 8}, "sum": {"values": 2}})
    assert outputs == {"sum": {"total": 23}}
    assert run_order == [
        "add_zero",
        "branch_joiner",
        "below_10",
        "add_one",
        "counter",
        "branch_joiner",
        "below_10",
        "add_one",
        "counter",
        "branch_joiner",
        "below_10",
        "add_two",
        "sum",
    ]


def test_pipeline_that_has_a_component_with_mutable_input():
    @component
    class InputMangler:
        @component.output_types(mangled_list=List[str])
        def run(self, input_list: List[str]):
            input_list.append("extra_item")
            return {"mangled_list": input_list}

    pipe = RayPipeline()
    pipe.add_component("mangler1", InputMangler())
    pipe.add_component("mangler2", InputMangler())
    pipe.add_component("concat1", StringListJoiner())
    pipe.add_component("concat2", StringListJoiner())
    pipe.connect("mangler1", "concat1")
    pipe.connect("mangler2", "concat2")

    input_list = ["foo", "bar"]

    outputs = pipe.run({"mangler1": {"input_list": input_list}, "mangler2": {"input_list": input_list}})
    assert outputs == {
        "concat1": {"output": ["foo", "bar", "extra_item"]},
        "concat2": {"output": ["foo", "bar", "extra_item"]},
    }


def test_pipeline_that_has_a_component_with_mutable_output_sent_to_multiple_inputs():
    @component
    class PassThroughPromptBuilder:
        # This is a pass-through component that returns the same input
        @component.output_types(prompt=List[ChatMessage])
        def run(self, prompt_source: List[ChatMessage]):
            return {"prompt": prompt_source}

    @component
    class MessageMerger:
        @component.output_types(merged_message=str)
        def run(self, messages: List[ChatMessage], metadata: Optional[dict] = None):
            return {"merged_message": "\n".join(t.content for t in messages)}

    @component
    class FakeGenerator:
        # This component is a fake generator that always returns the same message
        @component.output_types(replies=List[ChatMessage])
        def run(self, messages: List[ChatMessage]):
            return {"replies": [ChatMessage.from_assistant("Fake message")]}

    prompt_builder = PassThroughPromptBuilder()
    llm = FakeGenerator()
    mm1 = MessageMerger()
    mm2 = MessageMerger()

    pipe = RayPipeline()
    pipe.add_component("prompt_builder", prompt_builder)
    pipe.add_component("llm", llm)
    pipe.add_component("mm1", mm1)
    pipe.add_component("mm2", mm2)

    pipe.connect("prompt_builder.prompt", "llm.messages")
    pipe.connect("prompt_builder.prompt", "mm1")
    pipe.connect("llm.replies", "mm2")

    messages = [
        ChatMessage.from_system("Always respond in English even if some input data is in other languages."),
        ChatMessage.from_user("Tell me about Berlin"),
    ]
    params = {"metadata": {"metadata_key": "metadata_value", "meta2": "value2"}}

    outputs = pipe.run({"mm1": params, "mm2": params, "prompt_builder": {"prompt_source": messages}})
    assert outputs == {
        "mm1": {
            "merged_message": "Always respond "
            "in English even "
            "if some input "
            "data is in other "
            "languages.\n"
            "Tell me about "
            "Berlin"
        },
        "mm2": {"merged_message": "Fake message"},
    }


def test_pipeline_that_has_a_greedy_and_variadic_component_after_a_component_with_default_input():
    """
    This test verifies that `Pipeline.run()` executes the components in the correct order when
    there's a greedy Component with variadic input right before a Component with at least one default input.
    """
    # Use a "singleton" memory store available in distributed Ray environment
    document_store = RayInMemoryDocumentStore()
    document_store.write_documents([Document(content="This is a simple document")])

    pipeline = RayPipeline()
    template = "Given this documents: {{ documents|join(', ', attribute='content') }} Answer this question: {{ query }}"
    pipeline.add_component("retriever", RayInMemoryBM25Retriever(document_store=document_store))
    pipeline.add_component("prompt_builder", PromptBuilder(template=template))
    pipeline.add_component("branch_joiner", BranchJoiner(List[Document]))

    pipeline.connect("retriever", "branch_joiner")
    pipeline.connect("branch_joiner", "prompt_builder.documents")

    outputs, run_order = run_pipeline(pipeline, {"query": "This is my question"})
    assert outputs == {
        "prompt_builder": {
            "prompt": "Given this "
            "documents: "
            "This is a "
            "simple "
            "document "
            "Answer this "
            "question: "
            "This is my "
            "question"
        }
    }

    assert run_order == ["retriever", "branch_joiner", "prompt_builder"]


def test_pipeline_that_has_a_component_that_doesnt_return_a_dictionary():
    BrokenComponent = component_class(  # noqa: N806
        "BrokenComponent",
        input_types={"a": int},
        output_types={"b": int},
        output=1,  # type:ignore
    )

    pipe = RayPipeline(max_runs_per_component=10)
    pipe.add_component("comp", BrokenComponent())

    with pytest.raises(PipelineRuntimeError):
        pipe.run({"comp": {"a": 1}})


def test_pipeline_that_has_components_added_in_a_different_order_from_the_order_of_execution():
    """
    We enqueue the Components in internal `to_run` data structure at the start of `Pipeline.run()` using the order
    they are added in the Pipeline with `Pipeline.add_component()`.
    If a Component A with defaults is added before a Component B that has no defaults, but in the Pipeline
    logic A must be executed after B it could run instead before.

    This test verifies that the order of execution is correct.
    """
    docs = [Document(content="Rome is the capital of Italy"), Document(content="Paris is the capital of France")]
    doc_store = RayInMemoryDocumentStore()
    doc_store.write_documents(docs)
    template = (
        "Given the following information, answer the question.\n"
        "Context:\n"
        "{% for document in documents %}"
        "    {{ document.content }}\n"
        "{% endfor %}"
        "Question: {{ query }}"
    )

    pipe = RayPipeline()

    # The order of this addition is important for the test
    # Do not edit them.
    pipe.add_component("prompt_builder", PromptBuilder(template=template))
    pipe.add_component("retriever", RayInMemoryBM25Retriever(document_store=doc_store))
    pipe.connect("retriever", "prompt_builder.documents")

    query = "What is the capital of France?"

    outputs, run_order = run_pipeline(pipe, {"prompt_builder": {"query": query}, "retriever": {"query": query}})

    assert outputs == {
        "prompt_builder": {
            "prompt": "Given the "
            "following "
            "information, "
            "answer the "
            "question.\n"
            "Context:\n"
            "    Paris is "
            "the capital "
            "of France\n"
            "    Rome is "
            "the capital "
            "of Italy\n"
            "Question: "
            "What is the "
            "capital of "
            "France?"
        }
    }
    assert run_order == ["retriever", "prompt_builder"]


def test_pipeline_that_has_a_component_with_only_default_inputs():
    FakeGenerator = component_class(  # noqa: N806
        "FakeGenerator", input_types={"prompt": str}, output_types={"replies": List[str]}, output={"replies": ["Paris"]}
    )
    docs = [Document(content="Rome is the capital of Italy"), Document(content="Paris is the capital of France")]
    doc_store = RayInMemoryDocumentStore()
    doc_store.write_documents(docs)
    template = (
        "Given the following information, answer the question.\n"
        "Context:\n"
        "{% for document in documents %}"
        "    {{ document.content }}\n"
        "{% endfor %}"
        "Question: {{ query }}"
    )

    pipe = RayPipeline()

    pipe.add_component("retriever", RayInMemoryBM25Retriever(document_store=doc_store))
    pipe.add_component("prompt_builder", PromptBuilder(template=template))
    pipe.add_component("generator", FakeGenerator())
    pipe.add_component("answer_builder", AnswerBuilder())

    pipe.connect("retriever", "prompt_builder.documents")
    pipe.connect("prompt_builder.prompt", "generator.prompt")
    pipe.connect("generator.replies", "answer_builder.replies")
    pipe.connect("retriever.documents", "answer_builder.documents")

    outputs, run_order = run_pipeline(pipe, {"query": "What is the capital of France?"})

    assert outputs == {
        "answer_builder": {
            "answers": [
                GeneratedAnswer(
                    data="Paris",
                    query="What is the capital of France?",
                    documents=[
                        Document(
                            id="413dccdf51a54cca75b7ed2eddac04e6e58560bd2f0caf4106a3efc023fe3651",
                            content="Paris is the capital of France",
                            score=1.600237583702734,
                        ),
                        Document(
                            id="a4a874fc2ef75015da7924d709fbdd2430e46a8e94add6e0f26cd32c1c03435d",
                            content="Rome is the capital of Italy",
                            score=1.2536639934227616,
                        ),
                    ],
                    meta={},
                )
            ]
        }
    }
    assert run_order == ["retriever", "prompt_builder", "generator", "answer_builder"]


def test_pipeline_that_has_a_component_with_only_default_inputs_as_first_to_run():
    """
    This tests verifies that a Pipeline doesn't get stuck running in a loop if
    it has all the following characterics:
    - The first Component has all defaults for its inputs
    - The first Component receives one input from the user
    - The first Component receives one input from a loop in the Pipeline
    - The second Component has at least one default input
    """

    @component
    class FakeGenerator:
        def __init__(self):
            self.called = False

        # This component is a fake generator that always returns the same message
        @component.output_types(replies=List[str])
        def run(self, prompt: str, generation_kwargs: Optional[Dict[str, Any]] = None):
            if self.called:
                return {"replies": ["Rome"]}

            self.called = True
            return {"replies": ["Paris"]}

    template = (
        "Answer the following question.\n"
        "{% if previous_replies %}\n"
        "Previously you replied incorrectly this:\n"
        "{% for reply in previous_replies %}\n"
        " - {{ reply }}\n"
        "{% endfor %}\n"
        "{% endif %}\n"
        "Question: {{ query }}"
    )
    router = ConditionalRouter(
        routes=[
            {
                "condition": "{{ replies == ['Rome'] }}",
                "output": "{{ replies }}",
                "output_name": "correct_replies",
                "output_type": List[int],
            },
            {
                "condition": "{{ replies == ['Paris'] }}",
                "output": "{{ replies }}",
                "output_name": "incorrect_replies",
                "output_type": List[int],
            },
        ]
    )

    pipe = RayPipeline()

    pipe.add_component("prompt_builder", PromptBuilder(template=template))
    pipe.add_component("generator", FakeGenerator())
    pipe.add_component("router", router)

    pipe.connect("prompt_builder.prompt", "generator.prompt")
    pipe.connect("generator.replies", "router.replies")
    pipe.connect("router.incorrect_replies", "prompt_builder.previous_replies")

    outputs, run_order = run_pipeline(pipe, {"prompt_builder": {"query": "What is the capital of Italy?"}})

    assert outputs == {"router": {"correct_replies": ["Rome"]}}
    assert run_order == ["prompt_builder", "generator", "router", "prompt_builder", "generator", "router"]


def test_pipeline_that_has_a_single_component_that_send_one_of_outputs_to_itself():
    pipeline = RayPipeline(max_runs_per_component=10)
    pipeline.add_component("self_loop", SelfLoop())
    pipeline.connect("self_loop.current_value", "self_loop.values")

    outputs, run_order = run_pipeline(pipeline, {"self_loop": {"values": 5}})

    assert outputs == {"self_loop": {"final_result": 0}}
    assert run_order == ["self_loop", "self_loop", "self_loop", "self_loop", "self_loop"]


def test_pipeline_that_has_a_component_that_sends_one_of_its_outputs_to_itself():
    pipeline = RayPipeline(max_runs_per_component=10)
    pipeline.add_component("add_1", AddFixedValue())
    pipeline.add_component("self_loop", SelfLoop())
    pipeline.add_component("add_2", AddFixedValue())
    pipeline.connect("add_1", "self_loop.values")
    pipeline.connect("self_loop.current_value", "self_loop.values")
    pipeline.connect("self_loop.final_result", "add_2.value")

    outputs, run_order = run_pipeline(pipeline, {"add_1": {"value": 5}})

    assert outputs == {"add_2": {"result": 1}}
    assert run_order == [
        "add_1",
        "self_loop",
        "self_loop",
        "self_loop",
        "self_loop",
        "self_loop",
        "self_loop",
        "add_2",
    ]


def test_pipeline_that_has_multiple_branches_that_merge_into_a_component_with_a_single_variadic_input():
    pipeline = RayPipeline()
    pipeline.add_component("add_one", AddFixedValue())
    pipeline.add_component("parity", Remainder(divisor=2))
    pipeline.add_component("add_ten", AddFixedValue(add=10))
    pipeline.add_component("double", Double())
    pipeline.add_component("add_four", AddFixedValue(add=4))
    pipeline.add_component("add_one_again", AddFixedValue())
    pipeline.add_component("sum", Sum())

    pipeline.connect("add_one.result", "parity.value")
    pipeline.connect("parity.remainder_is_0", "add_ten.value")
    pipeline.connect("parity.remainder_is_1", "double.value")
    pipeline.connect("add_one.result", "sum.values")
    pipeline.connect("add_ten.result", "sum.values")
    pipeline.connect("double.value", "sum.values")
    pipeline.connect("parity.remainder_is_1", "add_four.value")
    pipeline.connect("add_four.result", "add_one_again.value")
    pipeline.connect("add_one_again.result", "sum.values")

    outputs, run_order = run_pipeline(pipeline, {"add_one": {"value": 1}})
    assert outputs == {"sum": {"total": 14}}
    assert run_order == ["add_one", "parity", "add_ten", "sum"]

    outputs, run_order = run_pipeline(pipeline, {"add_one": {"value": 2}})
    assert outputs == {"sum": {"total": 17}}
    # assert run_order == ["add_one", "parity", "double", "add_four", "add_one_again", "sum"]


def test_pipeline_that_has_multiple_branches_of_different_lengths_that_merge_into_a_component_with_a_single_variadic_input():
    pipeline = RayPipeline()
    pipeline.add_component("first_addition", AddFixedValue(add=2))
    pipeline.add_component("second_addition", AddFixedValue(add=2))
    pipeline.add_component("third_addition", AddFixedValue(add=2))
    pipeline.add_component("sum", Sum())
    pipeline.add_component("fourth_addition", AddFixedValue(add=1))

    pipeline.connect("first_addition.result", "second_addition.value")
    pipeline.connect("first_addition.result", "sum.values")
    pipeline.connect("second_addition.result", "sum.values")
    pipeline.connect("third_addition.result", "sum.values")
    pipeline.connect("sum.total", "fourth_addition.value")

    outputs, _ = run_pipeline(pipeline, {"first_addition": {"value": 1}, "third_addition": {"value": 1}})
    assert outputs == {"fourth_addition": {"result": 12}}


def test_pipeline_that_is_linear_and_returns_intermediate_outputs():
    pipeline = RayPipeline()
    pipeline.add_component("first_addition", AddFixedValue(add=2))
    pipeline.add_component("second_addition", AddFixedValue())
    pipeline.add_component("double", Double())
    pipeline.connect("first_addition", "double")
    pipeline.connect("double", "second_addition")

    outputs, run_order = run_pipeline(
        pipeline, {"first_addition": {"value": 1}}, include_outputs_from={"second_addition", "double", "first_addition"}
    )
    assert outputs == {
        "double": {"value": 6},
        "first_addition": {"result": 3},
        "second_addition": {"result": 7},
    }
    assert run_order == ["first_addition", "double", "second_addition"]

    outputs, run_order = run_pipeline(pipeline, {"first_addition": {"value": 1}}, include_outputs_from={"double"})
    assert outputs == {"double": {"value": 6}, "second_addition": {"result": 7}}
    assert run_order == ["first_addition", "double", "second_addition"]


def test_pipeline_that_has_a_loop_and_returns_intermediate_outputs_from_it():
    pipeline = RayPipeline(max_runs_per_component=10)
    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("branch_joiner", BranchJoiner(type_=int))
    pipeline.add_component("below_10", Threshold(threshold=10))
    pipeline.add_component("below_5", Threshold(threshold=5))
    pipeline.add_component("add_three", AddFixedValue(add=3))
    pipeline.add_component("accumulator", Accumulate())
    pipeline.add_component("add_two", AddFixedValue(add=2))

    pipeline.connect("add_one.result", "branch_joiner")
    pipeline.connect("branch_joiner.value", "below_10.value")
    pipeline.connect("below_10.below", "accumulator.value")
    pipeline.connect("accumulator.value", "below_5.value")
    pipeline.connect("below_5.above", "add_three.value")
    pipeline.connect("below_5.below", "branch_joiner")
    pipeline.connect("add_three.result", "branch_joiner")
    pipeline.connect("below_10.above", "add_two.value")

    outputs, run_order = run_pipeline(
        pipeline,
        {"add_one": {"value": 3}},
        include_outputs_from={
            "add_two",
            "add_one",
            "branch_joiner",
            "below_10",
            "accumulator",
            "below_5",
            "add_three",
        },
    )
    assert outputs == {
        "add_two": {"result": 13},
        "add_one": {"result": 4},
        "branch_joiner": {"value": 11},
        "below_10": {"above": 11},
        "accumulator": {"value": 8},
        "below_5": {"above": 8},
        "add_three": {"result": 11},
    }
    assert run_order == [
        "add_one",
        "branch_joiner",
        "below_10",
        "accumulator",
        "below_5",
        "branch_joiner",
        "below_10",
        "accumulator",
        "below_5",
        "add_three",
        "branch_joiner",
        "below_10",
        "add_two",
    ]


def test_pipeline_that_is_linear_and_returns_intermediate_outputs_from_multiple_sockets():
    @component
    class DoubleWithOriginal:
        """
        Doubles the input value and returns the original value as well.
        """

        @component.output_types(value=int, original=int)
        def run(self, value: int):
            return {"value": value * 2, "original": value}

    pipeline = RayPipeline()
    pipeline.add_component("first_addition", AddFixedValue(add=2))
    pipeline.add_component("second_addition", AddFixedValue())
    pipeline.add_component("double", DoubleWithOriginal())
    pipeline.connect("first_addition", "double")
    pipeline.connect("double.value", "second_addition")

    outputs, run_order = run_pipeline(
        pipeline, {"first_addition": {"value": 1}}, include_outputs_from={"second_addition", "double", "first_addition"}
    )
    assert outputs == {
        "double": {"original": 3, "value": 6},
        "first_addition": {"result": 3},
        "second_addition": {"result": 7},
    }
    assert run_order == ["first_addition", "double", "second_addition"]

    outputs, run_order = run_pipeline(pipeline, {"first_addition": {"value": 1}}, include_outputs_from={"double"})
    assert outputs == {"double": {"original": 3, "value": 6}, "second_addition": {"result": 7}}
    assert run_order == ["first_addition", "double", "second_addition"]


def test_pipeline_that_has_a_component_with_default_inputs_that_doesnt_receive_anything_from_its_sender():
    routes = [
        {"condition": "{{'reisen' in sentence}}", "output": "German", "output_name": "language_1", "output_type": str},
        {"condition": "{{'viajar' in sentence}}", "output": "Spanish", "output_name": "language_2", "output_type": str},
    ]
    router = ConditionalRouter(routes)

    pipeline = RayPipeline()
    pipeline.add_component("router", router)
    pipeline.add_component("pb", PromptBuilder(template="Ok, I know, that's {{language}}"))
    pipeline.connect("router.language_2", "pb.language")

    outputs, run_order = run_pipeline(pipeline, {"router": {"sentence": "Wir mussen reisen"}})
    assert outputs == {"router": {"language_1": "German"}}
    assert run_order == ["router"]

    outputs, run_order = run_pipeline(pipeline, {"router": {"sentence": "Yo tengo que viajar"}})
    assert outputs == {"pb": {"prompt": "Ok, I know, that's Spanish"}}
    assert run_order == ["router", "pb"]


def test_pipeline_that_has_a_component_with_default_inputs_that_doesnt_receive_anything_from_its_sender_but_receives_input_from_user():
    prompt = PromptBuilder(
        template="""Please generate an SQL query. The query should answer the following Question: {{ question }};
            If the question cannot be answered given the provided table and columns, return 'no_answer'
            The query is to be answered for the table is called 'absenteeism' with the following
            Columns: {{ columns }};
            Answer:"""
    )

    @component
    class FakeGenerator:
        @component.output_types(replies=List[str])
        def run(self, prompt: str):
            if "a question with no_answer" in prompt:
                return {"replies": ["There's simply no_answer to this question"]}
            return {"replies": ["Some SQL query"]}

    @component
    class FakeSQLQuerier:
        @component.output_types(results=str)
        def run(self, query: str):
            return {"results": "This is the query result", "query": query}

    llm = FakeGenerator()
    sql_querier = FakeSQLQuerier()

    routes = [
        {
            "condition": "{{'no_answer' not in replies[0]}}",
            "output": "{{replies[0]}}",
            "output_name": "sql",
            "output_type": str,
        },
        {
            "condition": "{{'no_answer' in replies[0]}}",
            "output": "{{question}}",
            "output_name": "go_to_fallback",
            "output_type": str,
        },
    ]

    router = ConditionalRouter(routes)

    fallback_prompt = PromptBuilder(
        template="""User entered a query that cannot be answered with the given table.
                    The query was: {{ question }} and the table had columns: {{ columns }}.
                    Let the user know why the question cannot be answered"""
    )
    fallback_llm = FakeGenerator()

    pipeline = RayPipeline()
    pipeline.add_component("prompt", prompt)
    pipeline.add_component("llm", llm)
    pipeline.add_component("router", router)
    pipeline.add_component("fallback_prompt", fallback_prompt)
    pipeline.add_component("fallback_llm", fallback_llm)
    pipeline.add_component("sql_querier", sql_querier)

    pipeline.connect("prompt", "llm")
    pipeline.connect("llm.replies", "router.replies")
    pipeline.connect("router.sql", "sql_querier.query")
    pipeline.connect("router.go_to_fallback", "fallback_prompt.question")
    pipeline.connect("fallback_prompt", "fallback_llm")

    columns = "Age, Absenteeism_time_in_hours, Days, Disciplinary_failure"

    # First Run
    outputs, run_order = run_pipeline(
        pipeline,
        {
            "prompt": {"question": "This is a question with no_answer", "columns": columns},
            "router": {"question": "This is a question with no_answer"},
        },
    )
    assert outputs == {"fallback_llm": {"replies": ["There's simply no_answer to this question"]}}
    assert run_order == ["prompt", "llm", "router", "fallback_prompt", "fallback_llm"]

    # Second Run
    outputs, run_order = run_pipeline(
        pipeline,
        {
            "prompt": {"question": "This is a question that has an answer", "columns": columns},
            "router": {"question": "This is a question that has an answer"},
        },
    )
    assert outputs == {"sql_querier": {"results": "This is the query result", "query": "Some SQL query"}}
    assert run_order == ["prompt", "llm", "router", "sql_querier"]


def test_pipeline_that_has_a_loop_and_a_component_with_default_inputs_that_doesnt_receive_anything_from_its_sender_but_receives_input_from_user():
    template = """
    You are an experienced and accurate Turkish CX speacialist that classifies customer comments into pre-defined categories below:\n
    Negative experience labels:
    - Late delivery
    - Rotten/spoilt item
    - Bad Courier behavior

    Positive experience labels:
    - Good courier behavior
    - Thanks & appreciation
    - Love message to courier
    - Fast delivery
    - Quality of products

    Create a JSON object as a response. The fields are: 'positive_experience', 'negative_experience'.
    Assign at least one of the pre-defined labels to the given customer comment under positive and negative experience fields.
    If the comment has a positive experience, list the label under 'positive_experience' field.
    If the comments has a negative_experience, list it under the 'negative_experience' field.
    Here is the comment:\n{{ comment }}\n. Just return the category names in the list. If there aren't any, return an empty list.

    {% if invalid_replies and error_message %}
    You already created the following output in a previous attempt: {{ invalid_replies }}
    However, this doesn't comply with the format requirements from above and triggered this Python exception: {{ error_message }}
    Correct the output and try again. Just return the corrected output without any extra explanations.
    {% endif %}
    """
    prompt_builder = PromptBuilder(template=template)

    @component
    class FakeOutputValidator:
        @component.output_types(
            valid_replies=List[str], invalid_replies=Optional[List[str]], error_message=Optional[str]
        )
        def run(self, replies: List[str]):
            if not getattr(self, "called", False):
                self.called = True
                return {"invalid_replies": ["This is an invalid reply"], "error_message": "this is an error message"}
            return {"valid_replies": replies}

    @component
    class FakeGenerator:
        @component.output_types(replies=List[str])
        def run(self, prompt: str):
            return {"replies": ["This is a valid reply"]}

    llm = FakeGenerator()
    validator = FakeOutputValidator()

    pipeline = RayPipeline()
    pipeline.add_component("prompt_builder", prompt_builder)

    pipeline.add_component("llm", llm)
    pipeline.add_component("output_validator", validator)

    pipeline.connect("prompt_builder.prompt", "llm.prompt")
    pipeline.connect("llm.replies", "output_validator.replies")
    pipeline.connect("output_validator.invalid_replies", "prompt_builder.invalid_replies")

    pipeline.connect("output_validator.error_message", "prompt_builder.error_message")

    comment = "I loved the quality of the meal but the courier was rude"

    outputs, run_order = run_pipeline(pipeline, {"prompt_builder": {"template_variables": {"comment": comment}}})
    assert outputs == {"output_validator": {"valid_replies": ["This is a valid reply"]}}
    assert run_order == [
        "prompt_builder",
        "llm",
        "output_validator",
        "prompt_builder",
        "llm",
        "output_validator",
    ]


def test_pipeline_that_has_multiple_components_with_only_default_inputs_and_are_added_in_a_different_order_from_the_order_of_execution():
    prompt_builder1 = PromptBuilder(
        template="""
    You are a spellchecking system. Check the given query and fill in the corrected query.

    Question: {{question}}
    Corrected question:
    """
    )
    prompt_builder2 = PromptBuilder(
        template="""
    According to these documents:

    {% for doc in documents %}
    {{ doc.content }}
    {% endfor %}

    Answer the given question: {{question}}
    Answer:
    """
    )
    prompt_builder3 = PromptBuilder(
        template="""
    {% for ans in replies %}
    {{ ans }}
    {% endfor %}
    """
    )

    @component
    class FakeRetriever:
        @component.output_types(documents=List[Document])
        def run(
            self,
            query: str,
            filters: Optional[Dict[str, Any]] = None,
            top_k: Optional[int] = None,
            scale_score: Optional[bool] = None,
        ):
            return {"documents": [Document(content="This is a document")]}

    @component
    class FakeRanker:
        @component.output_types(documents=List[Document])
        def run(
            self,
            query: str,
            documents: List[Document],
            top_k: Optional[int] = None,
            scale_score: Optional[bool] = None,
            calibration_factor: Optional[float] = None,
            score_threshold: Optional[float] = None,
        ):
            return {"documents": documents}

    @component
    class FakeGenerator:
        @component.output_types(replies=List[str], meta=Dict[str, Any])
        def run(self, prompt: str, generation_kwargs: Optional[Dict[str, Any]] = None):
            return {"replies": ["This is a reply"], "meta": {"meta_key": "meta_value"}}

    pipeline = RayPipeline()
    pipeline.add_component(name="retriever", instance=FakeRetriever())
    pipeline.add_component(name="ranker", instance=FakeRanker())
    pipeline.add_component(name="prompt_builder2", instance=prompt_builder2)
    pipeline.add_component(name="prompt_builder1", instance=prompt_builder1)
    pipeline.add_component(name="prompt_builder3", instance=prompt_builder3)
    pipeline.add_component(name="llm", instance=FakeGenerator())
    pipeline.add_component(name="spellchecker", instance=FakeGenerator())

    pipeline.connect("prompt_builder1", "spellchecker")
    pipeline.connect("spellchecker.replies", "prompt_builder3")
    pipeline.connect("prompt_builder3", "retriever.query")
    pipeline.connect("prompt_builder3", "ranker.query")
    pipeline.connect("retriever.documents", "ranker.documents")
    pipeline.connect("ranker.documents", "prompt_builder2.documents")
    pipeline.connect("prompt_builder3", "prompt_builder2.question")
    pipeline.connect("prompt_builder2", "llm")

    outputs, run_order = run_pipeline(pipeline, {"prompt_builder1": {"question": "Wha i Acromegaly?"}})
    assert outputs == {
        "llm": {"replies": ["This is a reply"], "meta": {"meta_key": "meta_value"}},
        "spellchecker": {"meta": {"meta_key": "meta_value"}},
    }
    assert run_order == [
        "prompt_builder1",
        "spellchecker",
        "prompt_builder3",
        "retriever",
        "ranker",
        "prompt_builder2",
        "llm",
    ]


def test_pipeline_that_is_linear_with_conditional_branching_and_multiple_joins():
    pipeline = RayPipeline()

    @component
    class FakeRouter:
        @component.output_types(LEGIT=str, INJECTION=str)
        def run(self, query: str):
            if "injection" in query:
                return {"INJECTION": query}
            return {"LEGIT": query}

    @component
    class FakeEmbedder:
        @component.output_types(embeddings=List[float])
        def run(self, text: str):
            return {"embeddings": [1.0, 2.0, 3.0]}

    @component
    class FakeRanker:
        @component.output_types(documents=List[Document])
        def run(self, query: str, documents: List[Document]):
            return {"documents": documents}

    @component
    class FakeRetriever:
        @component.output_types(documents=List[Document])
        def run(self, query: str):
            if "injection" in query:
                return {"documents": []}
            return {"documents": [HashableDocument(content="This is a document")]}

    @component
    class FakeEmbeddingRetriever:
        @component.output_types(documents=List[Document])
        def run(self, query_embedding: List[float]):
            return {"documents": [HashableDocument(content="This is another document")]}

    pipeline.add_component(name="router", instance=FakeRouter())
    pipeline.add_component(name="text_embedder", instance=FakeEmbedder())
    pipeline.add_component(name="retriever", instance=FakeEmbeddingRetriever())
    pipeline.add_component(name="emptyretriever", instance=FakeRetriever())
    pipeline.add_component(name="joinerfinal", instance=DocumentJoiner())
    pipeline.add_component(name="joinerhybrid", instance=DocumentJoiner())
    pipeline.add_component(name="ranker", instance=FakeRanker())
    pipeline.add_component(name="bm25retriever", instance=FakeRetriever())

    pipeline.connect("router.INJECTION", "emptyretriever.query")
    pipeline.connect("router.LEGIT", "text_embedder.text")
    pipeline.connect("text_embedder", "retriever.query_embedding")
    pipeline.connect("router.LEGIT", "ranker.query")
    pipeline.connect("router.LEGIT", "bm25retriever.query")
    pipeline.connect("bm25retriever", "joinerhybrid.documents")
    pipeline.connect("retriever", "joinerhybrid.documents")
    pipeline.connect("joinerhybrid.documents", "ranker.documents")
    pipeline.connect("ranker", "joinerfinal.documents")
    pipeline.connect("emptyretriever", "joinerfinal.documents")

    # First Run
    outputs, _ = run_pipeline(pipeline, {"router": {"query": "I'm a legit question"}})
    assert set(outputs["joinerfinal"]["documents"]) == {
        HashableDocument(content="This is a document"),
        HashableDocument(content="This is another document"),
    }

    # Second Run
    outputs, run_order = run_pipeline(pipeline, {"router": {"query": "I'm a nasty prompt injection"}})
    assert outputs == {"joinerfinal": {"documents": []}}
    assert run_order == ["router", "emptyretriever", "joinerfinal"]


def test_is_a_simple_agent():
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
    Follow the pattern in the example below.

    Example:
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

    Let's start, the question is: {{query}}

    Thought:
    """

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

    @component
    class FakeThoughtActionOpenAIChatGenerator:
        run_counter = 0

        @component.output_types(replies=List[ChatMessage])
        def run(self, messages: List[ChatMessage], generation_kwargs: Optional[Dict[str, Any]] = None):
            if self.run_counter == 0:
                self.run_counter += 1
                return {
                    "replies": [
                        ChatMessage.from_assistant(
                            "thinking\n Action: google_search[What is taller, Eiffel Tower or Leaning Tower of Pisa]\n"
                        )
                    ]
                }

            return {"replies": [ChatMessage.from_assistant("thinking\n Action: finish[Eiffel Tower]\n")]}

    @component
    class FakeConclusionOpenAIChatGenerator:
        @component.output_types(replies=List[ChatMessage])
        def run(self, messages: List[ChatMessage], generation_kwargs: Optional[Dict[str, Any]] = None):
            return {"replies": [ChatMessage.from_assistant("Tower of Pisa is 55 meters tall\n")]}

    @component
    class FakeSerperDevWebSearch:
        @component.output_types(documents=List[Document])
        def run(self, query: str):
            return {
                "documents": [
                    Document(content="Eiffel Tower is 300 meters tall"),
                    Document(content="Tower of Pisa is 55 meters tall"),
                ]
            }

    # main part
    pipeline = RayPipeline()
    pipeline.add_component("main_input", BranchJoiner(List[ChatMessage]))
    pipeline.add_component("prompt_builder", ChatPromptBuilder(variables=["query"]))
    pipeline.add_component("llm", FakeThoughtActionOpenAIChatGenerator())

    @component
    class ToolExtractor:
        @component.output_types(output=List[str])
        def run(self, messages: List[ChatMessage]):
            prompt: str = messages[-1].content
            lines = prompt.strip().split("\n")
            for line in reversed(lines):
                pattern = r"Action:\s*(\w+)\[(.*?)\]"

                match = re.search(pattern, line)
                if match:
                    action_name = match.group(1)
                    parameter = match.group(2)
                    return {"output": [action_name, parameter]}
            return {"output": [None, None]}

    pipeline.add_component("tool_extractor", ToolExtractor())

    @component
    class PromptConcatenator:
        def __init__(self, suffix: str = ""):
            self._suffix = suffix

        @component.output_types(output=List[ChatMessage])
        def run(self, replies: List[ChatMessage], current_prompt: List[ChatMessage]):
            content = current_prompt[-1].content + replies[-1].content + self._suffix
            return {"output": [ChatMessage.from_user(content)]}

    @component
    class SearchOutputAdapter:
        @component.output_types(output=List[ChatMessage])
        def run(self, replies: List[ChatMessage]):
            content = f"Observation: {replies[-1].content}\n"
            return {"output": [ChatMessage.from_assistant(content)]}

    pipeline.add_component("prompt_concatenator_after_action", PromptConcatenator())

    pipeline.add_component("router", ConditionalRouter(routes))
    pipeline.add_component("router_search", FakeSerperDevWebSearch())
    pipeline.add_component("search_prompt_builder", ChatPromptBuilder(variables=["documents", "search_query"]))
    pipeline.add_component("search_llm", FakeConclusionOpenAIChatGenerator())

    pipeline.add_component("search_output_adapter", SearchOutputAdapter())
    pipeline.add_component("prompt_concatenator_after_observation", PromptConcatenator(suffix="\nThought: "))

    # main
    pipeline.connect("main_input", "prompt_builder.template")
    pipeline.connect("prompt_builder.prompt", "llm.messages")
    pipeline.connect("llm.replies", "prompt_concatenator_after_action.replies")

    # tools
    pipeline.connect("prompt_builder.prompt", "prompt_concatenator_after_action.current_prompt")
    pipeline.connect("prompt_concatenator_after_action", "tool_extractor.messages")

    pipeline.connect("tool_extractor", "router")
    pipeline.connect("router.search", "router_search.query")
    pipeline.connect("router_search.documents", "search_prompt_builder.documents")
    pipeline.connect("router.search", "search_prompt_builder.search_query")
    pipeline.connect("search_prompt_builder.prompt", "search_llm.messages")

    pipeline.connect("search_llm.replies", "search_output_adapter.replies")
    pipeline.connect("search_output_adapter", "prompt_concatenator_after_observation.replies")
    pipeline.connect("prompt_concatenator_after_action", "prompt_concatenator_after_observation.current_prompt")
    pipeline.connect("prompt_concatenator_after_observation", "main_input")

    search_message = [ChatMessage.from_user(search_message_template)]
    messages = [ChatMessage.from_user(react_message_template)]
    question = "which tower is taller: eiffel tower or tower of pisa?"

    outputs, run_order = run_pipeline(
        pipeline,
        {
            "main_input": {"value": messages},
            "prompt_builder": {"query": question},
            "search_prompt_builder": {"template": search_message},
        },
    )
    assert outputs == {"router": {"finish": "Eiffel Tower"}}
    assert run_order == [
        "main_input",
        "prompt_builder",
        "llm",
        "prompt_concatenator_after_action",
        "tool_extractor",
        "router",
        "router_search",
        "search_prompt_builder",
        "search_llm",
        "search_output_adapter",
        "prompt_concatenator_after_observation",
        "main_input",
        "prompt_builder",
        "llm",
        "prompt_concatenator_after_action",
        "tool_extractor",
        "router",
    ]


def test_has_a_variadic_component_that_receives_partial_inputs():
    @component
    class ConditionalDocumentCreator:
        def __init__(self, content: str):
            self.content = content

        @component.output_types(documents=List[HashableDocument], noop=None)
        def run(self, create_document: bool = False):
            if create_document:
                return {"documents": [HashableDocument(id=self.content, content=self.content)]}
            return {"noop": None}

    pipeline = RayPipeline()
    pipeline.add_component("first_creator", ConditionalDocumentCreator(content="First document"))
    pipeline.add_component("second_creator", ConditionalDocumentCreator(content="Second document"))
    pipeline.add_component("third_creator", ConditionalDocumentCreator(content="Third document"))
    pipeline.add_component("documents_joiner", DocumentJoiner())

    pipeline.connect("first_creator.documents", "documents_joiner.documents")
    pipeline.connect("second_creator.documents", "documents_joiner.documents")
    pipeline.connect("third_creator.documents", "documents_joiner.documents")

    # First Run
    outputs, _ = run_pipeline(
        pipeline, {"first_creator": {"create_document": True}, "third_creator": {"create_document": True}}
    )
    assert outputs["second_creator"] == {"noop": None}
    assert set(outputs["documents_joiner"]["documents"]) == {
        HashableDocument(id="First document", content="First document"),
        HashableDocument(id="Third document", content="Third document"),
    }

    # Second Run
    outputs, _ = run_pipeline(
        pipeline, {"first_creator": {"create_document": True}, "second_creator": {"create_document": True}}
    )
    assert outputs["third_creator"] == {"noop": None}
    assert set(outputs["documents_joiner"]["documents"]) == {
        HashableDocument(id="First document", content="First document"),
        HashableDocument(id="Second document", content="Second document"),
    }
