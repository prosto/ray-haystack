from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from haystack.core.component import Component, InputSocket

from ray_haystack.serialization import ComponentWrapper

ArgMapping = Dict[str, str]


class ComponentNode:
    def __init__(self, name: str, component: Component):
        self._name = name
        self._wrapper = ComponentWrapper(component=component)  # !!! Use keyword argument "component"

        self._input_sockets: Dict[str, InputSocket] = component.__haystack_input__._sockets_dict

        self._node_in_args: Dict[ComponentNode, ArgMapping] = defaultdict(dict)
        self._node_out_args: Dict[ComponentNode, ArgMapping] = defaultdict(dict)

        self.upstream: Set[ComponentNode] = set()
        self.downstream: Set[ComponentNode] = set()

    def get_name(self):
        return self._name

    def add_node_in_args(
        self,
        from_node: "ComponentNode",
        arg_mapping: ArgMapping,
    ):
        self._node_in_args[from_node].update(arg_mapping)

    def add_node_out_args(
        self,
        to_node: "ComponentNode",
        arg_mapping: ArgMapping,
    ):
        self._node_out_args[to_node].update(arg_mapping)

    def get_component_wrapper(self):
        return self._wrapper

    def get_component(self):
        return self._wrapper.get_component()

    def get_input_sockets(self) -> Dict[str, InputSocket]:
        return self._input_sockets

    def get_input_socket(self, name: str) -> InputSocket:
        (matching_socket,) = (socket for socket in self._input_sockets.values() if socket.name == name)
        return matching_socket

    def get_all_input_names(self) -> List[str]:
        return [socket.name for socket in self.get_input_sockets().values()]

    def get_connected_input_names(self) -> List[str]:
        mapped_keys = {key for mapping in self._node_in_args.values() for key in mapping.values()}
        return list(mapped_keys)

    def get_output_names(self) -> List[str]:
        mapped_keys = [key for mapping in self._node_out_args.values() for key in mapping.keys()]
        return mapped_keys

    def is_connected(self, input_name: str) -> bool:
        return input_name in self.get_connected_input_names()

    def has_default_value(self, input_name: str) -> bool:
        return not self.get_input_socket(input_name).is_mandatory

    def get_default_value(self, input_name: str) -> Optional[Any]:
        socket = self.get_input_socket(input_name)
        return socket.default_value if not socket.is_mandatory else None

    def is_variadic(self, input_name: str) -> bool:
        return self.get_input_socket(input_name).is_variadic

    def is_greedy(self, input_name: str) -> bool:
        return self.get_input_socket(input_name).is_greedy

    def get_senders(self, input_name: str) -> List["ComponentNode"]:
        return [node for node, mapping in self._node_in_args.items() if input_name in mapping.values()]

    def get_node_in_args(self, input_name: Optional[str] = None):
        return [
            (from_node, out_name, in_name)
            for from_node, mapping in self._node_in_args.items()
            for out_name, in_name in mapping.items()
            if input_name is None or (input_name is not None and in_name == input_name)
        ]

    def in_degree_is_zero(self) -> bool:
        return not self.upstream

    def __repr__(self):
        return f"ComponentNode({self.get_name()})"
