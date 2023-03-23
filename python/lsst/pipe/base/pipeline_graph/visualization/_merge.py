# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import annotations

__all__ = (
    "MergedNodeKey",
    "merge_graph_input_trees",
    "merge_graph_output_trees",
    "merge_graph_intermediates",
)

import dataclasses
from collections import defaultdict
from typing import Any, Generic, Iterable, TypeVar

import networkx
import networkx.algorithms.dag
import networkx.algorithms.tree
from lsst.daf.butler import DimensionGraph

from .._abcs import NodeKey, NodeType
from ._options import NodeAttributeOptions

_P = TypeVar("_P")
_C = TypeVar("_C")


class MergedNodeKey(frozenset[NodeKey]):
    def __str__(self) -> str:
        members = [str(k) for k in self]
        members.sort(reverse=True)
        return ", ".join(members)

    @property
    def node_type(self) -> NodeType:
        return next(iter(self)).node_type


@dataclasses.dataclass(frozen=True)
class _MergeKey(Generic[_P, _C]):
    parents: frozenset[_P]
    dimensions: DimensionGraph | None
    storage_class_name: str | None
    task_class_name: str | None
    children: frozenset[_C]

    @classmethod
    def from_node_state(
        cls,
        state: dict[str, Any],
        parents: Iterable[_P],
        children: Iterable[_C],
        options: NodeAttributeOptions,
    ) -> _MergeKey[_P, _C]:
        return cls(
            parents=frozenset(parents),
            dimensions=state["dimensions"] if options.dimensions else None,
            storage_class_name=(state.get("storage_class_name") if options.storage_classes else None),
            task_class_name=(state.get("task_class_name") if options.task_classes else None),
            children=frozenset(children),
        )


_TreeGroupMergeKey = _MergeKey[NodeKey, "_TreeGroupMergeKey"]
_TreeApplyMergeKey = _MergeKey[NodeKey, "_TreeApplyMergeKey"]


def merge_graph_input_trees(xgraph: networkx.DiGraph, options: NodeAttributeOptions, depth: int) -> None:
    groups = _make_tree_merge_groups(xgraph, options, depth)
    _apply_tree_merges(xgraph, groups)


def merge_graph_output_trees(xgraph: networkx.DiGraph, options: NodeAttributeOptions, depth: int) -> None:
    groups = _make_tree_merge_groups(xgraph.reverse(copy=False), options, depth)
    _apply_tree_merges(xgraph, groups)


def merge_graph_intermediates(xgraph: networkx.DiGraph, options: NodeAttributeOptions) -> None:
    groups: dict[_MergeKey[NodeKey, NodeKey], set[NodeKey]] = defaultdict(set)
    for node, state in xgraph.nodes.items():
        merge_key = _MergeKey[NodeKey, NodeKey].from_node_state(
            state,
            xgraph.predecessors(node),
            xgraph.successors(node),
            options,
        )
        if merge_key.parents and merge_key.children:
            groups[merge_key].add(node)
    replacements: dict[NodeKey, MergedNodeKey] = {}
    for merge_key, members in groups.items():
        if len(members) < 2:
            continue
        new_node_key = MergedNodeKey(frozenset(members))
        xgraph.add_node(
            new_node_key,
            storage_class_name=merge_key.storage_class_name,
            task_class_name=merge_key.task_class_name,
            dimensions=merge_key.dimensions,
        )
        for parent in merge_key.parents:
            xgraph.add_edge(replacements.get(parent, parent), new_node_key)
        for child in merge_key.children:
            xgraph.add_edge(new_node_key, replacements.get(child, child))
        for member in members:
            replacements[member] = new_node_key
        xgraph.remove_nodes_from(members)


def _make_tree_merge_groups(
    xgraph: networkx.DiGraph,
    options: NodeAttributeOptions,
    depth: int,
) -> list[dict[_TreeGroupMergeKey, set[NodeKey]]]:
    # Our goal is to obtain mappings that groups trees of nodes by the
    # attributes in a _TreeMergeKey.  The nested dictionaries are the root of a
    # tree and the nodes under that root, recursively (but not including the
    # root).  We nest these mappings inside a list, which each mapping
    # corresponding to a different depth for the trees it represents.  We start
    # with a special empty dict for "0-depth trees", since that makes
    # result[depth] valid and hence off-by-one errors less likely.
    result: list[dict[_TreeGroupMergeKey, set[NodeKey]]] = [{}]
    if depth == 0:
        return result
    # We start with the nodes that have no predecessors in the graph.
    # Ignore for now the fact that the 'current_candidates' data structure
    # we process is actually a dict that associates each of those nodes
    # with an empty dict.  All of these initial nodes are valid trees,
    # since they're just single nodes.
    first_generation = next(networkx.algorithms.dag.topological_generations(xgraph))
    current_candidates: dict[NodeKey, dict[NodeKey, _TreeGroupMergeKey]] = dict.fromkeys(first_generation, {})
    # Set up an outer loop over tree depth; we'll construct a new set of
    # candidates at each iteration.
    while current_candidates:
        # As we go, we'll remember nodes that have just one predecessor, as
        # those predecessors might be the roots of slightly taller trees.
        # We store the successors and their merge keys under them.
        next_candidates: dict[NodeKey, dict[NodeKey, _TreeGroupMergeKey]] = defaultdict(dict)
        # We also want to track the nodes the level up that are not trees
        # because some node has both them and some other node as a
        # predecessor.
        nontrees: set[NodeKey] = set()
        # Make a dictionary for the results at this depth, then start the
        # inner iteration over candidates and (after the first iteration)
        # their children.
        result_for_depth: dict[_TreeGroupMergeKey, set[NodeKey]] = defaultdict(set)
        for node, children in current_candidates.items():
            # Make a _TreeMergeKey for this node and add it to the results for
            # this depth.  Two nodes with the same _TreeMergeKey are roots of
            # isomorphic trees that have the same predecessor(s), and can be
            # merged (with isomorphism defined as both both structure and
            # whatever comparisons are in 'options').
            merge_key = _TreeGroupMergeKey.from_node_state(
                xgraph.nodes[node], xgraph.successors(node), children.values(), options
            )
            result_for_depth[merge_key].add(node)
            if len(result) <= depth:
                # See if this node's successor might be the root of a
                # larger tree.
                if len(merge_key.parents) == 1:
                    (parent,) = merge_key.parents
                    next_candidates[parent][node] = dataclasses.replace(merge_key, parents=frozenset())
                else:
                    nontrees.update(merge_key.parents)
        # Append the results for this depth.
        result.append(result_for_depth)
        # Trim out candidates that aren't trees after all.
        for nontree_node in nontrees & next_candidates.keys():
            del next_candidates[nontree_node]
        current_candidates = next_candidates
    return result


def _apply_tree_merges(
    xgraph: networkx.DiGraph,
    groups: list[dict[_TreeGroupMergeKey, set[NodeKey]]],
) -> None:
    replacements: dict[NodeKey, MergedNodeKey] = {}
    for group in reversed(groups):
        new_group: dict[_TreeApplyMergeKey, set[NodeKey]] = defaultdict(set)
        for merge_key, members in group.items():
            if merge_key.parents & replacements.keys():
                replaced_parents = frozenset(replacements.get(p, p) for p in merge_key.parents)
                new_group[dataclasses.replace(merge_key, parents=replaced_parents)].update(members)
            else:
                new_group[merge_key].update(members)
        for merge_key, members in new_group.items():
            if len(members) < 2:
                continue
            new_node_key = MergedNodeKey(frozenset(members))
            new_edges: set[tuple[NodeKey | MergedNodeKey, NodeKey | MergedNodeKey]] = set()
            for member_key in members:
                replacements[member_key] = new_node_key
                new_edges.update(
                    (replacements.get(a, a), replacements.get(b, b)) for a, b in xgraph.in_edges(member_key)
                )
                new_edges.update(
                    (replacements.get(a, a), replacements.get(b, b)) for a, b in xgraph.out_edges(member_key)
                )
            xgraph.add_node(
                new_node_key,
                storage_class_name=merge_key.storage_class_name,
                task_class_name=merge_key.task_class_name,
                dimensions=merge_key.dimensions,
            )
            xgraph.add_edges_from(new_edges)
    xgraph.remove_nodes_from(replacements.keys())