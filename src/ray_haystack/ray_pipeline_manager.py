import logging
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional, Set

import ray
import ray.actor
from haystack.core.component import Component
from ray.actor import ActorHandle
from ray.util.queue import Queue

from ray_haystack.component_actor import ComponentActor
from ray_haystack.graph import ComponentNode, RayPipelineGraph
from ray_haystack.ray_pipeline_events import PipelineEvent, PipelineEventType
from ray_haystack.ray_pipeline_processor import RayPipelineProcessor
from ray_haystack.ray_pipeline_settings import (
    RayPipelineSettings,
    RayPipelineSettingsWrapper,
)

logger = logging.getLogger(__name__)


@dataclass
class ActorDescriptor:
    actor_handle: ActorHandle
    component_name: str
    actor_name: Optional[str] = None
    is_detached: bool = False


@dataclass
class PipelineProcessorResult:
    pipeline_output_ref: Any
    events_queue: Queue

    def get_pipeline_output(self) -> Dict[str, Any]:
        """
        A blocking call to wait for the processor to finish pipeline execution

        Returns:
            Pipeline outputs
        """
        return ray.get(self.pipeline_output_ref)

    def consume_events(self) -> List[PipelineEvent]:
        number_of_events = self.events_queue.size()
        return self.events_queue.get_nowait_batch(number_of_events)

    def pipeline_events_sync(self) -> Generator[PipelineEvent, None, None]:
        while pipeline_event := self.events_queue.get(block=True):
            yield pipeline_event

            if pipeline_event.type == PipelineEventType.PIPELINE_END:
                break


class RayPipelineManager:
    def __init__(self, max_runs_per_component: int, ray_settings: Optional[RayPipelineSettings] = None):
        self.max_runs_per_component = max_runs_per_component
        self.ray_settings_wrapper = RayPipelineSettingsWrapper(ray_settings or {})
        self.graph = RayPipelineGraph()

        self.detached_actors: List[ActorDescriptor] = []

    def add_node(self, name: str, instance: Component):
        self.graph.add_node(ComponentNode(name, instance))

    def add_edge(self, sender_component_name: str, receiver_component_name: str, mapping: Dict[str, str]):
        self.graph.add_edge(
            sender_component_name,
            receiver_component_name,
            mapping,
        )

    def kill_detached(self):
        for descriptor in self.detached_actors:
            logger.info(f"Killing detached actor with name '{descriptor.actor_name}'")
            ray.kill(descriptor.actor_handle)

    def start_pipeline_execution(
        self,
        pipeline_inputs: Dict[str, Dict[str, Any]],
        include_outputs_from: Set[str],
        ray_settings: Optional[RayPipelineSettings] = None,
    ) -> PipelineProcessorResult:
        ray_settings_wrapper = RayPipelineSettingsWrapper(ray_settings) if ray_settings else self.ray_settings_wrapper

        # Create Queue actor handling pipeline events
        events_queue_options: Dict[str, Any] = {
            **ray_settings_wrapper.get_events_queue_settings().get("actor_options", {})
        }
        events_queue = Queue(actor_options=events_queue_options)

        # Create actor for each component in pipeline
        component_actors: Dict[str, ActorDescriptor] = {
            node_name: self._create_component_actor(node, ray_settings_wrapper)
            for node_name, node in self.graph._nodes.items()
        }

        # Keep track of detached actors (those which keep running when pipeline execution finishes)
        self.detached_actors.extend([desc for desc in component_actors.values() if desc.is_detached])

        # Create actor for processing pipeline execution
        processor_settings = ray_settings_wrapper.get_processor_settings()
        processor_actor_options = processor_settings.get("actor_options", {})

        processor_actor: ActorHandle = RayPipelineProcessor.options(**processor_actor_options).remote(  # type:ignore
            graph=self.graph,
            max_runs_per_component=self.max_runs_per_component,
            ray_settings=ray_settings,
            component_actors={name: desc.actor_handle for name, desc in component_actors.items()},
            events_queue=events_queue,
            pipeline_inputs=pipeline_inputs,
            include_outputs_from=include_outputs_from,
        )

        # Run pipeline and get reference to pipeline outputs (non-blocking)
        pipeline_output_ref = processor_actor.run_pipeline.remote()

        return PipelineProcessorResult(
            pipeline_output_ref=pipeline_output_ref,
            events_queue=events_queue,
        )

    def _create_component_actor(
        self, node: ComponentNode, ray_settings_wrapper: RayPipelineSettingsWrapper
    ) -> ActorDescriptor:
        component_name = node.get_name()
        common_component_settings = ray_settings_wrapper.component_settings
        component_settings = ray_settings_wrapper.get_component_settings(component_name)
        actor_options = component_settings.get("actor_options", {})

        # If actor name is not given in component specific options and `use_component_name_for_actor` is True
        # set actor name same as `component_name`
        if common_component_settings.get("use_component_name_for_actor", False) and "name" not in actor_options:
            actor_options["name"] = component_name

        actor_handle: ActorHandle = ComponentActor.options(**actor_options).remote(  # type:ignore
            node.get_name(),
            node.get_component_wrapper(),
            middleware=component_settings.get("middleware"),
        )

        return ActorDescriptor(
            actor_handle=actor_handle,
            component_name=component_name,
            actor_name=actor_options.get("name", None),
            is_detached=actor_options.get("lifetime") == "detached",
        )
