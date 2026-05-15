from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable


MODULUS = 1_000_000_007


class Reduction(str, Enum):
    SINGLE = "single"
    SUM = "sum"


@dataclass(frozen=True)
class NodeSpec:
    id: str
    operation: str
    operands: tuple[int, ...] = ()
    dependencies: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphSpec:
    name: str
    nodes: tuple[NodeSpec, ...]
    expected_answer: int
    answer_nodes: tuple[str, ...]
    reduction: Reduction
    modulus: int = MODULUS

    def answer_from_outputs(self, outputs: dict[str, str | int]) -> int:
        if self.reduction == Reduction.SINGLE:
            return int(outputs[self.answer_nodes[0]]) % self.modulus

        total = 0
        for node in self.answer_nodes:
            total = add_mod(total, int(outputs[node]), self.modulus)
        return total

    def dependencies_by_node(self) -> dict[str, tuple[str, ...]]:
        return {node.id: node.dependencies for node in self.nodes}

    def dependents_by_node(self) -> dict[str, list[str]]:
        dependents = {node.id: [] for node in self.nodes}
        for node in self.nodes:
            for dependency in node.dependencies:
                dependents[dependency].append(node.id)
        return dependents

    def roots(self) -> list[str]:
        return [node.id for node in self.nodes if not node.dependencies]


def build_graph(
    shape: str,
    *,
    size: int = 100,
    width: int = 10,
    depth: int = 10,
    branching: int = 3,
    modulus: int = MODULUS,
) -> GraphSpec:
    if shape == "chain":
        return chain(size, modulus=modulus)
    if shape == "wide":
        return wide(size, modulus=modulus)
    if shape == "fan_in":
        return fan_in(size, modulus=modulus)
    if shape == "layered":
        return layered(width=width, depth=depth, modulus=modulus)
    if shape == "tree":
        return tree(depth=depth, branching=branching, modulus=modulus)

    raise ValueError(
        "shape must be one of: chain, wide, fan_in, layered, tree"
    )


def chain(length: int, *, modulus: int = MODULUS) -> GraphSpec:
    require_positive(length, "chain length")

    nodes: list[NodeSpec] = [NodeSpec("chain_0", "const", (1,))]
    expected = 1

    for index in range(1, length):
        node = chain_node(index)
        dependency = chain_node(index - 1)
        nodes.append(NodeSpec(node, "add", (index,), (dependency,)))
        expected = add_mod(expected, index, modulus)

    return GraphSpec(
        name=f"chain/{length}",
        nodes=tuple(nodes),
        expected_answer=expected,
        answer_nodes=(chain_node(length - 1),),
        reduction=Reduction.SINGLE,
        modulus=modulus,
    )


def wide(width: int, *, modulus: int = MODULUS) -> GraphSpec:
    require_positive(width, "wide width")

    nodes = []
    answer_nodes = []
    expected = 0

    for index in range(width):
        node = wide_node(index)
        value = leaf_value(index, modulus)
        nodes.append(NodeSpec(node, "const", (value,)))
        answer_nodes.append(node)
        expected = add_mod(expected, value, modulus)

    return GraphSpec(
        name=f"wide/{width}",
        nodes=tuple(nodes),
        expected_answer=expected,
        answer_nodes=tuple(answer_nodes),
        reduction=Reduction.SUM,
        modulus=modulus,
    )


def fan_in(width: int, *, modulus: int = MODULUS) -> GraphSpec:
    require_positive(width, "fan-in width")

    nodes = []
    dependencies = []
    expected = 0

    for index in range(width):
        node = source_node(index)
        value = leaf_value(index, modulus)
        nodes.append(NodeSpec(node, "const", (value,)))
        dependencies.append(node)
        expected = add_mod(expected, value, modulus)

    nodes.append(NodeSpec("reducer", "add", (), tuple(dependencies)))

    return GraphSpec(
        name=f"fan_in/{width}",
        nodes=tuple(nodes),
        expected_answer=expected,
        answer_nodes=("reducer",),
        reduction=Reduction.SINGLE,
        modulus=modulus,
    )


def layered(width: int, depth: int, *, modulus: int = MODULUS) -> GraphSpec:
    require_positive(width, "layered width")
    require_positive(depth, "layered depth")

    nodes = []
    previous_values = []

    for index in range(width):
        node = layered_node(0, index)
        value = leaf_value(index, modulus)
        nodes.append(NodeSpec(node, "const", (value,)))
        previous_values.append(value)

    for layer in range(1, depth):
        previous_sum = sum_mod(previous_values, modulus)
        current_values = []

        for index in range(width):
            node = layered_node(layer, index)
            add = layer + index
            dependencies = tuple(layered_node(layer - 1, previous) for previous in range(width))
            nodes.append(NodeSpec(node, "add", (add,), dependencies))
            current_values.append(add_mod(previous_sum, add, modulus))

        previous_values = current_values

    answer_nodes = tuple(layered_node(depth - 1, index) for index in range(width))

    return GraphSpec(
        name=f"layered/{width}x{depth}",
        nodes=tuple(nodes),
        expected_answer=sum_mod(previous_values, modulus),
        answer_nodes=answer_nodes,
        reduction=Reduction.SUM,
        modulus=modulus,
    )


def tree(depth: int, branching: int, *, modulus: int = MODULUS) -> GraphSpec:
    require_positive(depth, "tree depth")
    require_positive(branching, "tree branching")

    nodes = []
    leaf_level = depth - 1
    level_values: dict[tuple[int, int], int] = {}

    for index in range(node_count_at_depth(leaf_level, branching)):
        node = tree_node(leaf_level, index)
        value = leaf_value(index, modulus)
        nodes.append(NodeSpec(node, "const", (value,)))
        level_values[(leaf_level, index)] = value

    for level in range(leaf_level - 1, -1, -1):
        for index in range(node_count_at_depth(level, branching)):
            node = tree_node(level, index)
            add = level + index
            dependencies = []
            value = add % modulus

            for child in range(branching):
                child_index = index * branching + child
                dependencies.append(tree_node(level + 1, child_index))
                value = add_mod(value, level_values[(level + 1, child_index)], modulus)

            nodes.append(NodeSpec(node, "add", (add,), tuple(dependencies)))
            level_values[(level, index)] = value

    return GraphSpec(
        name=f"tree/{depth}x{branching}",
        nodes=tuple(nodes),
        expected_answer=level_values[(0, 0)],
        answer_nodes=(tree_node(0, 0),),
        reduction=Reduction.SINGLE,
        modulus=modulus,
    )


def terminal_nodes(spec: GraphSpec) -> list[str]:
    dependents = spec.dependents_by_node()
    return [node for node, children in dependents.items() if not children]


def compute_node(node: NodeSpec, dependency_outputs: Iterable[int], modulus: int) -> int:
    values = [value % modulus for value in node.operands]
    values.extend(value % modulus for value in dependency_outputs)

    if node.operation in ("const", "constant"):
        return values[0] % modulus
    if node.operation in ("add", "sum"):
        return sum_mod(values, modulus)
    if node.operation in ("mul", "multiply", "product"):
        value = 1
        for item in values:
            value = (value * item) % modulus
        return value

    raise ValueError(f"unknown operation: {node.operation}")


def chain_node(index: int) -> str:
    return f"chain_{index}"


def wide_node(index: int) -> str:
    return f"wide_{index}"


def source_node(index: int) -> str:
    return f"source_{index}"


def layered_node(layer: int, index: int) -> str:
    return f"layer_{layer}_{index}"


def tree_node(level: int, index: int) -> str:
    return f"tree_{level}_{index}"


def leaf_value(index: int, modulus: int) -> int:
    return ((index + 1) * 17 + 11) % modulus


def node_count_at_depth(depth: int, branching: int) -> int:
    return branching**depth


def add_mod(left: int, right: int, modulus: int) -> int:
    return (left + right) % modulus


def sum_mod(values: Iterable[int], modulus: int) -> int:
    total = 0
    for value in values:
        total = add_mod(total, value, modulus)
    return total


def require_positive(value: int, name: str) -> None:
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
