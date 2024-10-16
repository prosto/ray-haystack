from typing import Any, Dict, List, Optional, Set

from haystack.core.component import Component, InputSocket, OutputSocket
from haystack.core.pipeline.base import PipelineBase
from haystack.core.pipeline.draw import _to_mermaid_text
from haystack.core.pipeline.utils import parse_connect_string

from ray_haystack.ray_pipeline_manager import (
    PipelineProcessorResult,
    RayPipelineManager,
)
from ray_haystack.ray_pipeline_settings import RayPipelineSettings


class RayPipeline(PipelineBase):
    """
    Asynchronous version of Haystack Pipeline orchestration engine using Ray.

    Orchestrates component execution according to component connections in the graph, possibly running components
    in parallel.
    """

    def __init__(
        self,
        metadata: Optional[Dict[str, Any]] = None,
        max_runs_per_component: int = 100,
    ):
        super().__init__(
            metadata=metadata,
            max_runs_per_component=max_runs_per_component,
        )

        self._pipeline_manager = RayPipelineManager(
            max_runs_per_component=max_runs_per_component,
            ray_settings=self.metadata.get("ray", {}),
        )

    def connect(self, sender: str, receiver: str) -> "PipelineBase":
        pipe = super().connect(sender, receiver)

        sender_component_name, _ = parse_connect_string(sender)
        receiver_component_name, _ = parse_connect_string(receiver)

        for _, receiver_name, connection in self.graph.edges(nbunch=sender_component_name, data=True):
            if receiver_name == receiver_component_name:
                sender_socket: OutputSocket = connection["from_socket"]
                receiver_socket: InputSocket = connection["to_socket"]
                self._pipeline_manager.add_edge(
                    sender_component_name,
                    receiver_component_name,
                    {sender_socket.name: receiver_socket.name},
                )

        return pipe

    def add_component(self, name: str, instance: Component) -> None:
        super().add_component(name, instance)

        self._pipeline_manager.add_node(name, instance)

    def run_nowait(
        self,
        data: Dict[str, Any],
        include_outputs_from: Optional[Set[str]] = None,
        *,
        ray_settings: Optional[RayPipelineSettings] = None,
    ) -> PipelineProcessorResult:
        # normalize `data`
        data = self._prepare_component_input_data(data)

        # Raise if input is malformed in some way
        self._validate_input(data)

        # Initialize the inputs state
        components_inputs: Dict[str, Dict[str, Any]] = self._init_inputs_state(data)

        # Run distributed pipeline using Ray and return reference to results
        processor_result = self._pipeline_manager.start_pipeline_execution(
            pipeline_inputs=components_inputs,
            include_outputs_from=include_outputs_from or set(),
            ray_settings=ray_settings,
        )

        return processor_result

    def run(
        self,
        data: Dict[str, Any],
        include_outputs_from: Optional[Set[str]] = None,
        *,
        ray_settings: Optional[RayPipelineSettings] = None,
    ) -> Dict[str, Any]:
        processor_result = self.run_nowait(data, include_outputs_from, ray_settings=ray_settings)

        # Wait until pipeline outputs are ready and return
        return processor_result.get_pipeline_output()

    def cleanup(self):
        self._pipeline_manager.kill_detached()

    def to_mermaid_text(self) -> str:
        return _to_mermaid_text(self.graph)

    def get_components(self) -> Dict[str, Component]:
        return dict(self.graph.nodes(data="instance"))

    def get_component_names(self) -> List[str]:
        return list(self.get_components().keys())
