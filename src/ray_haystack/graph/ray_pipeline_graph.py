from collections import defaultdict
from typing import Dict, List, Set, Tuple, Union

from ray_haystack.graph.component_node import ComponentNode

BackEdges = List[Tuple[ComponentNode, ComponentNode]]


class RayPipelineGraph:
    def __init__(self):
        self._nodes: Dict[str, ComponentNode] = {}

    def add_node(self, node: ComponentNode):
        name = node.get_name()
        if name not in self._nodes:
            self._nodes[name] = node

    def get_node(self, name: str) -> ComponentNode:
        return self._nodes[name]

    def add_edge(
        self,
        from_node: Union[ComponentNode, str],
        to_node: Union[ComponentNode, str],
        arg_mapping: Dict[str, str],
    ):
        if isinstance(from_node, str):
            from_node = self._nodes[from_node]

        if isinstance(to_node, str):
            to_node = self._nodes[to_node]

        self.add_node(from_node)
        self.add_node(to_node)

        to_node.upstream.add(from_node)
        from_node.downstream.add(to_node)

        to_node.add_node_in_args(from_node, arg_mapping)
        from_node.add_node_out_args(to_node, arg_mapping)

    def _find_back_edges(self, node: ComponentNode, visit_status: Dict[ComponentNode, str], back_edges: BackEdges):
        visit_status[node] = "gray"

        for neighbor in node.downstream:
            if visit_status[neighbor] == "white":
                self._find_back_edges(neighbor, visit_status, back_edges)
            elif visit_status[neighbor] == "gray":
                back_edges.append((node, neighbor))

        visit_status[node] = "black"

    def find_back_edges(self) -> BackEdges:
        visit_status = {node: "white" for node in self._nodes.values()}
        back_edges: BackEdges = []

        # Run DFS on all unvisited nodes
        for node in self._nodes.values():
            if visit_status[node] == "white":
                self._find_back_edges(node, visit_status, back_edges)

        return back_edges

    def compute_dependencies(
        self,
    ) -> Tuple[Dict[ComponentNode, Set[ComponentNode]], Dict[ComponentNode, Set[ComponentNode]]]:
        dependents: Dict[ComponentNode, Set[ComponentNode]] = defaultdict(set)
        dependencies: Dict[ComponentNode, Set[ComponentNode]] = defaultdict(set)

        # Use a memoization dictionary to store previously computed dependencies
        memo: Dict[ComponentNode, Set[ComponentNode]] = {}

        def dfs(node: ComponentNode, path: Set[ComponentNode]) -> Set[ComponentNode]:
            """Depth-First Search to find all predecessors of a node."""
            if node in memo:
                return memo[node]

            if node in path:
                return set()

            path.add(node)
            predecessors: Set[ComponentNode] = set()

            for neighbor in node.upstream:
                predecessors.add(neighbor)
                predecessors.update(dfs(neighbor, path))

            path.remove(node)
            memo[node] = predecessors
            return predecessors

        for node in self._nodes.values():
            dfs(node, set())

        # TODO: Find a more efficient way to reconcile dependencies wth cycles
        for node, node_deps in memo.items():
            node_deps.discard(node)  # Remove self-reference
            refined_deps = set()

            for dep in node_deps:
                refined_deps.add(dep)
                refined_deps.update(memo[dep])
                dependents[dep].add(node)

            refined_deps.discard(node)  # Remove self-reference
            dependencies[node] = refined_deps

        return dependencies, dependents
