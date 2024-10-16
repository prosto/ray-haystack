from ray_haystack import RayPipeline

from .sample_components import AddFixedValue, Parity, Sum, Threshold
from .utils import run_pipeline


def test_pipeline_with_two_branches_and_two_variadic_components():
    pipeline = RayPipeline(max_runs_per_component=3)
    pipeline.add_component("comp_1", AddFixedValue(1))
    pipeline.add_component("parity_1", Parity())
    pipeline.add_component("comp_1_1", AddFixedValue(1))
    pipeline.add_component("comp_2", AddFixedValue(1))
    pipeline.add_component("threshold", Threshold(5))
    pipeline.add_component("sum_1", Sum())
    pipeline.add_component("sum_2", Sum())

    pipeline.connect("comp_1", "comp_2.value")
    pipeline.connect("comp_1", "parity_1.value")

    pipeline.connect("comp_1_1", "sum_2")
    pipeline.connect("parity_1.odd", "sum_2")
    pipeline.connect("parity_1.even", "comp_1_1.add")

    pipeline.connect("comp_2", "threshold.value")
    pipeline.connect("threshold.below", "comp_2.add")
    pipeline.connect("threshold.above", "sum_1")
    pipeline.connect("sum_2", "sum_1")

    outputs, _ = run_pipeline(pipeline, {"comp_1": {"value": 1}, "comp_1_1": {"value": 1}})
    assert outputs == {"sum_1": {"total": 8}}

    outputs, _ = run_pipeline(pipeline, {"comp_1": {"value": 2}, "comp_1_1": {"value": 1}})
    assert outputs == {"sum_1": {"total": 10}}


def test_pipeline_with_loop_connected_to_an_input_with_default_value():
    pipeline = RayPipeline(max_runs_per_component=3)
    pipeline.add_component("comp_0", AddFixedValue(1))
    pipeline.add_component("comp_1", AddFixedValue(1))
    pipeline.add_component("comp_2", AddFixedValue(1))
    pipeline.add_component("comp_3", Threshold(5))
    pipeline.add_component("comp_4", AddFixedValue())

    pipeline.connect("comp_0", "comp_1.value")
    pipeline.connect("comp_1", "comp_2.value")
    pipeline.connect("comp_2", "comp_3.value")
    pipeline.connect("comp_3.below", "comp_1.add")
    pipeline.connect("comp_3.above", "comp_4.value")

    outputs, _ = run_pipeline(
        pipeline,
        {
            "comp_0": {"value": 1},
        },
    )
    assert outputs == {"comp_4": {"result": 8}}


def test_component_which_will_run_once_no_blocking_input_dependencies_left():
    pipeline = RayPipeline(max_runs_per_component=3)
    pipeline.add_component("comp_1_1", AddFixedValue(1))
    pipeline.add_component("comp_1_2", AddFixedValue(1))
    pipeline.add_component("comp_2_1", Sum())
    pipeline.add_component("comp_2_2", AddFixedValue(1))
    pipeline.add_component("comp_3", Threshold(5))
    pipeline.add_component("comp_4", AddFixedValue())

    pipeline.connect("comp_1_1", "comp_2_1")
    pipeline.connect("comp_1_2", "comp_2_2.value")
    pipeline.connect("comp_2_2", "comp_3.value")
    pipeline.connect("comp_3.below", "comp_2_2.add")
    pipeline.connect("comp_3.above", "comp_4.value")
    pipeline.connect("comp_3.above", "comp_2_1")

    outputs, _ = run_pipeline(
        pipeline,
        {
            "comp_1_1": {"value": 1},
            "comp_1_2": {"value": 2},
        },
    )
    assert outputs == {"comp_2_1": {"total": 9}, "comp_4": {"result": 8}}
