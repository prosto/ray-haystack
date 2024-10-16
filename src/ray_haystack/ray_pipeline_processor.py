import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Union

import ray
from haystack.core.errors import PipelineMaxComponentRuns, PipelineRuntimeError
from ray.actor import ActorHandle
from ray.util.queue import Queue

from ray_haystack.graph import ComponentNode, RayPipelineGraph
from ray_haystack.ray_pipeline_events import (
    ComponentEndEvent,
    ComponentStartEvent,
    PipelineEndEvent,
    PipelineStartEvent,
)
from ray_haystack.ray_pipeline_settings import RayPipelineSettings


def setup_logger():
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("haystack.ray.processor")


@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self):
        self.ready_event = asyncio.Event()

    def send(self, clear=False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait=True):
        if should_wait:
            await self.ready_event.wait()


@ray.remote
class RayPipelineProcessor:
    def __init__(
        self,
        graph: RayPipelineGraph,
        max_runs_per_component: int,
        ray_settings: RayPipelineSettings,
        component_actors: Dict[str, ActorHandle],
        events_queue: Queue,
        pipeline_inputs: Dict[str, Any],
        include_outputs_from: Set[str],
    ):
        self._graph = graph
        self._max_runs_per_component = max_runs_per_component
        self._ray_settings = ray_settings
        self._component_actors = component_actors
        self._events_queue = events_queue
        self._pipeline_inputs = pipeline_inputs
        self._include_outputs_from = include_outputs_from

        self._component_data: Dict[str, _ComponentData] = {}
        self._inputs: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._outputs: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._back_edges: List[Tuple[ComponentNode, ComponentNode]] = []
        self._visits: Dict[str, int] = defaultdict(int)

        self._unfinished: List[Any] = []
        self._runnable_nodes: Set[ComponentNode] = set()
        self._running_nodes: Set[ComponentNode] = set()

        self.logger = setup_logger()

    def run_pipeline(self) -> Dict[str, Any]:
        # Find all back-edges (in case there are any loops in the pipeline)
        self._back_edges = self._graph.find_back_edges()

        # Find dependencies for each node in the graph
        (self._dependencies, self._dependents) = self._graph.compute_dependencies()

        # Initialize inputs for each component by assigning either pipeline inputs or defaults where applicable
        # Each component will also get its initial status depending on available input values, e.g. "runnable" status
        # makes component eligible for scheduling
        for node in self._graph._nodes.values():
            self._component_data[node.get_name()] = _ComponentData(node)

            self._assign_initial_inputs(node)

            self._assign_default_values(node)

            self._resolve_component_status_from_inputs(node)

        self._events_queue.put_nowait(
            PipelineStartEvent(
                data={
                    "pipeline_inputs": self._pipeline_inputs,
                    "runnable_nodes": [node.get_name() for node in self._runnable_nodes],
                }
            )
        )

        # Initially try to run components which are runnable and have no dependencies
        self._schedule_runnable_components()

        while self._unfinished:
            # Once finished remove node from the running list and keep remaining in `self._unfinished``
            finished, self._unfinished = ray.wait(self._unfinished, num_returns=1)

            finished_node_name, outputs = ray.get(finished[0])
            finished_node = self._graph.get_node(finished_node_name)

            self._update_node_status(finished_node, "finished")

            self.logger.debug(f"Finished running component {finished_node_name}, outputs {outputs}")

            if not isinstance(outputs, dict):
                raise PipelineRuntimeError(
                    f"Component '{finished_node_name}' didn't return a dictionary. "
                    "Components must always return dictionaries: check the the documentation."
                )

            self._events_queue.put_nowait(
                ComponentEndEvent(
                    data={
                        "name": finished_node_name,
                        "output": outputs,
                        "iteration": self._visits[finished_node_name],
                    }
                )
            )

            # Increase 'visits' count - track number of times component has ran
            self._visits[finished_node_name] += 1

            # Update component outputs with most recent invocation results
            self._outputs[finished_node_name] = outputs

            # Update downstream component input values with recent invocation results from 'finished_node'
            self._assign_inputs_from_outputs(finished_node, outputs)

            self._find_components_which_will_receive_missing_inputs(finished_node)

            # Schedule components which are ready to run
            self._schedule_runnable_components(finished_node)

        pipeline_output = self._build_pipeline_outputs()

        self._events_queue.put_nowait(
            PipelineEndEvent(
                data={
                    "output": pipeline_output,
                    "include_outputs_from": self._include_outputs_from,
                }
            )
        )

        return pipeline_output

    def _assign_inputs_from_outputs(self, from_node: ComponentNode, outputs: Dict[str, Any], skip_runnable=False):
        for to_node in from_node.downstream:
            component_data = self._component_data[to_node.get_name()]

            # In case `to_node` has been waiting to run with existing inputs we should not update its inputs as MISSING
            # and allow it to run as soon as all dependencies resolved
            if skip_runnable and component_data.has_status("runnable"):  # TODO: See if should be moved below 158
                continue

            is_a_cycle = self._is_back_edge(from_node, to_node)

            for out_name, in_name in to_node._node_in_args[from_node].items():
                connection_value = outputs.get(out_name, _InputValue.MISSING)
                input_value = component_data.get_input_value(in_name)

                input_value.update_value(
                    value=connection_value,
                    from_node=from_node,
                    out_name=out_name,
                )

                if is_a_cycle and input_value.is_variadic:
                    input_value.fill_in_missing_variadic_inputs()

                self.logger.debug(
                    f"Assign connection value {from_node}:{out_name} -> {to_node}:{in_name} = {connection_value}"
                )

            self._resolve_component_status_from_inputs(to_node)

    def _resolve_component_status_from_inputs(self, node: ComponentNode):
        component_data = self._component_data[node.get_name()]
        component_status: ComponentNodeStatus = "runnable"

        if not component_data.all_inputs_are_ready():
            component_status = "waiting_for_inputs"
        elif not component_data.has_enough_inputs_to_run():
            component_status = "missing_inputs"

        self._update_node_status(node, component_status)

    def _schedule_runnable_components(self, finished_node: Optional[ComponentNode] = None):
        sender_name = finished_node.get_name() if finished_node else None
        runnable_nodes = set(self._runnable_nodes)
        all_running = self._runnable_nodes.union(self._running_nodes)

        for runnable in runnable_nodes:
            node_name = runnable.get_name()
            component_actor = self._component_actors[node_name]
            component_data = self._component_data[node_name]

            node_dependencies = self._dependencies[runnable]
            runnable_dependencies = (all_running - {runnable}) & node_dependencies

            if runnable_dependencies:
                self.logger.debug(f"* Skip running '{node_name}', waiting for {runnable_dependencies} to complete.")
                continue

            if self._visits[node_name] > self._max_runs_per_component:
                msg = f"Maximum run count {self._max_runs_per_component} reached for component '{node_name}'"
                raise PipelineMaxComponentRuns(msg)

            component_inputs = component_data.build_component_inputs()
            self.logger.debug(f"* Schedule '{node_name}' (from '{sender_name}'), with inputs: {component_inputs}")

            # Start execution of component (`run`) with given inputs. It is a non-blocking call
            scheduled_run = component_actor.run_component.remote(component_inputs)  # type:ignore
            self._unfinished.append(scheduled_run)

            self._update_node_status(runnable, "running")

            self._events_queue.put_nowait(
                ComponentStartEvent(
                    data={
                        "name": node_name,
                        "sender_name": sender_name,
                        "input": component_inputs,
                        "iteration": self._visits[node_name],
                    }
                )
            )

            # After scheduling component execution we will make sure input flags are reset
            component_data.reset_inputs()  # TODO: See if its redundant considering call below

            self._reset_inputs(runnable)

    def _update_node_status(self, node: ComponentNode, status: "ComponentNodeStatus"):
        component_data = self._component_data[node.get_name()]
        current_status = component_data._status
        component_data.update_status(status)

        self._runnable_nodes.discard(node)
        self._running_nodes.discard(node)

        if status == "runnable":
            self._runnable_nodes.add(node)

        if status == "running":
            self._running_nodes.add(node)

        self.logger.debug(f"Updated component '{node}' status from '{current_status}' to '{status}'.")

    def _assign_initial_inputs(self, to_node: ComponentNode):
        component_data = self._component_data[to_node.get_name()]
        component_pipeline_inputs = self._pipeline_inputs.get(to_node.get_name(), {})

        for input_name in to_node.get_all_input_names():
            input_value = _InputValue(node=to_node, input_name=input_name)
            component_data._input_values[input_name] = input_value

            if input_name in component_pipeline_inputs:
                input_value.set_initial_value(value=component_pipeline_inputs[input_name])
            elif input_value.has_default and not input_value.is_connected:
                input_value.set_initial_value(value=to_node.get_default_value(input_name))

    def _assign_default_values(self, to_node: ComponentNode):
        component_data = self._component_data[to_node.get_name()]

        for from_node, out_name, in_name in to_node.get_node_in_args():
            input_value = component_data.get_input_value(in_name)
            if not input_value.can_run and self._is_back_edge(from_node, to_node):
                if input_value.is_variadic:
                    input_value.update_with_missing(from_node=from_node, out_name=out_name)
                elif input_value.is_absent() and input_value.has_default:
                    input_value.update_with_default(from_node=from_node, out_name=out_name)

    def _find_components_which_will_receive_missing_inputs(self, from_node: ComponentNode):
        visited_nodes: Set[ComponentNode] = {from_node}
        current_path: Set[ComponentNode] = set()  # Tracks nodes in the current path to detect cycles.

        def traverse_downstream(node: ComponentNode):
            current_path.add(node)

            for next_to_visit in node.downstream:
                component_data = self._component_data[next_to_visit.get_name()]

                if next_to_visit in current_path:
                    # If node is in the current path, we found a cycle, skip.
                    continue

                if next_to_visit not in visited_nodes and component_data.has_status("missing_inputs"):
                    # Assign MISSING inputs (like component returned an empty dictionary) for all downstream connections
                    self._assign_inputs_from_outputs(from_node=next_to_visit, outputs={}, skip_runnable=True)
                    # Explore next downstream components which will receive missing inputs
                    traverse_downstream(next_to_visit)

            visited_nodes.add(node)
            current_path.remove(node)

        traverse_downstream(from_node)

    def _reset_inputs(self, node: ComponentNode):
        component_data = self._component_data[node.get_name()]
        for from_node, _, in_name in node.get_node_in_args():
            input_value = component_data.get_input_value(in_name)
            # If upstream component (a dependency) which provides connected input will run in future
            # We should reset the input value so that next time the `node` runs we review its
            # runnable state based on fresh inputs from its dependencies
            if self._component_will_probably_run(from_node):
                input_value.reset_input()

    def _component_will_probably_run(self, node: ComponentNode) -> bool:
        all_which_might_run = self._runnable_nodes.union(self._running_nodes)
        runnable_dependencies = (all_which_might_run - {node}) & self._dependencies[node]

        return len(runnable_dependencies) > 0

    def _build_pipeline_outputs(self):
        result = {}

        for name, output in self._outputs.items():
            if name in self._include_outputs_from:
                result[name] = output
            else:
                consumed_keys = self._graph.get_node(name).get_output_names()
                unconsumed_output = {key: value for key, value in output.items() if key not in consumed_keys}
                if unconsumed_output:
                    result[name] = unconsumed_output

        return result

    def _is_back_edge(self, from_node: ComponentNode, to_node: ComponentNode) -> bool:
        return any(from_node == edge_from and to_node == edge_to for edge_from, edge_to in self._back_edges)


# A sentinel object to detect if a connection value has not been provided.
# It is not the same as 'None'
class _MissingValue:
    def __repr__(self):
        return "MISSING"


ComponentNodeStatus = Literal[
    "initial",  # component have not yet started (initial state when pipeline starts execution)
    "running",  # component is running (`run` method is called)
    "runnable",  # component will run soon (a state before being scheduled)
    "finished",  # component finished execution and provided outputs
    "waiting_for_inputs",  # component waits for some connections to provide values (either MISSING or actual value)
    "missing_inputs",  # component has got all values from connected inputs but some of them are MISSING, will not run
]


class _ComponentData:
    def __init__(self, node: ComponentNode):
        self._node = node
        self._name = node.get_name()
        self._status: ComponentNodeStatus = "initial"
        self._input_values: Dict[str, _InputValue] = defaultdict()

    def build_component_inputs(self):
        input_data = {}

        for input_name, input_value in self._input_values.items():
            if input_value.is_variadic:
                non_missing_values = [value for value in input_value.value if value is not _InputValue.MISSING]
                if len(non_missing_values) > 0:
                    input_data[input_name] = non_missing_values
            elif input_value.value is not _InputValue.MISSING:
                input_data[input_name] = input_value.value

        return input_data

    def has_status(self, *statuses: ComponentNodeStatus):
        return self._status in statuses

    def update_status(self, status: ComponentNodeStatus):
        self._status = status

    def get_input_value(self, name: str) -> "_InputValue":
        return self._input_values[name]

    def reset_inputs(self):
        for input_value in self._input_values.values():
            input_value.reset()

    def all_inputs_are_ready(self) -> bool:
        return all(value.is_ready for value in self._input_values.values())

    def has_enough_inputs_to_run(self):
        return all(value.can_run for value in self._input_values.values())


@dataclass
class _InputValue:
    MISSING = _MissingValue()

    node: ComponentNode
    input_name: str

    is_ready: bool = False
    can_run: bool = False
    value: Union[Any, List[Any]] = None
    initial_value: Union[Any, List[Any]] = None

    def __post_init__(self):
        self.is_variadic = self.node.is_variadic(self.input_name)
        self.is_greedy = self.node.is_greedy(self.input_name)
        self.is_connected = self.node.is_connected(self.input_name)
        self.has_default = self.node.has_default_value(self.input_name)
        self.senders = self.node.get_senders(self.input_name)
        self._values_per_connection = defaultdict(lambda: defaultdict())

        if self.is_variadic:
            self.initial_value = []

        self.logger = setup_logger()

    def is_absent(self) -> bool:
        return self.value is None

    def set_initial_value(self, value: Union[Any, List[Any]], is_ready=True, can_run=True):
        self.value = self.initial_value = value
        self.is_ready = is_ready
        self.can_run = can_run

    def update_value(
        self,
        value: Any,
        from_node: ComponentNode,
        out_name: str,
    ):
        # Keep track of exactly which connection has provided the value
        self._values_per_connection[from_node][out_name] = value

        if self.is_variadic:
            # For variadic inputs collect all input values from each connection
            # If there is initial value (pipeline input) it is added to the list
            self.value = self.initial_value + [
                value
                for value_per_output in self._values_per_connection.values()
                for value in value_per_output.values()
            ]
        else:
            self.value = value

        # Once value is updated lets update corresponding flags
        self._update_flags()

    def update_with_missing(self, from_node: ComponentNode, out_name: str):
        self.update_value(
            value=_InputValue.MISSING,
            from_node=from_node,
            out_name=out_name,
        )

    def fill_in_missing_variadic_inputs(self):
        if self.is_variadic:
            for from_node, out_name, _ in self.node.get_node_in_args():
                if from_node not in self._values_per_connection:
                    self.update_with_missing(from_node, out_name)

    def update_with_default(self, from_node: ComponentNode, out_name: str):
        self.update_value(
            value=self.node.get_default_value(self.input_name),
            from_node=from_node,
            out_name=out_name,
        )

    def reset_input(self):
        if self.is_variadic:
            self.value = []
            self.initial_value = []
            self._values_per_connection = defaultdict(lambda: defaultdict())

        self.is_ready = False
        self.can_run = False

    def reset(self):
        if self.is_variadic:
            self.value = []
            self.initial_value = []
            self._values_per_connection = defaultdict(lambda: defaultdict())
            self.is_ready = False
            self.can_run = False

    def _update_flags(self):
        if self.is_variadic:
            non_missing_values = [input_value for input_value in self.value if input_value is not _InputValue.MISSING]
            has_non_missing_value = len(non_missing_values) > 0

            if self.is_greedy:
                # We are ready to run greedy inputs as soon as at least one non-missing value is available
                enough_values_to_run = has_non_missing_value and len(self.value) > 0
                self.is_ready = self.can_run = enough_values_to_run
            else:
                self.is_ready = len(self.value) >= len(self.senders)
                self.can_run = has_non_missing_value
        else:
            self.is_ready = True
            self.can_run = self.value is not _InputValue.MISSING
