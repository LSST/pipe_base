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

__all__ = ("Layout",)

import dataclasses
import itertools
from collections.abc import Iterator
from typing import Generic, TextIO, TypeVar

import networkx
import networkx.algorithms.components
import networkx.algorithms.dag
import networkx.algorithms.shortest_paths
import networkx.algorithms.traversal

_K = TypeVar("_K")


@dataclasses.dataclass
class LayoutRow(Generic[_K]):
    node: _K
    x: int
    terminating: list[tuple[int, _K | None]] = dataclasses.field(default_factory=list)
    continuing: list[tuple[int, _K, frozenset[_K]]] = dataclasses.field(default_factory=list)


class Layout(Generic[_K]):
    def __init__(self, graph: networkx.DiGraph):
        self._graph = graph
        self._todo_graph = graph.copy()
        self._active_columns: dict[int, set[_K]] = {}
        self._locations: dict[_K, int] = {}
        self.x_max = 0
        for component in list(networkx.algorithms.components.weakly_connected_components(graph)):
            self._add_connected_graph(graph.subgraph(component))
        assert not self._todo_graph, list(self._todo_graph)
        del self._todo_graph
        del self._active_columns

    def _add_unblocked_node(self, node: _K) -> int:
        assert self._todo_graph.in_degree(node) == 0, str(node)
        for active_column_x, active_column_endpoints in list(self._active_columns.items()):
            if node in active_column_endpoints:
                active_column_endpoints.remove(node)
                if not active_column_endpoints:
                    del self._active_columns[active_column_x]
        for node_x in itertools.count():
            if node_x not in self._active_columns:
                break
        outgoing = set(self._todo_graph.successors(node))
        self._locations[node] = node_x
        self.x_max = max(node_x, self.x_max)
        self._todo_graph.remove_node(node)
        if outgoing:
            self._active_columns[node_x] = outgoing
        return node_x

    def _add_active_unblocked(self, avoid: _K) -> None:
        while True:
            # First we immediately add any nodes that don't have outgoing
            # edges (since these won't occupy new columns for more than a
            # single row), and remember the rest that are unblocked.
            unblocked = set()
            for node in list(itertools.chain.from_iterable(self._active_columns.values())):
                if node != avoid and node in self._todo_graph and self._todo_graph.in_degree(node) == 0:
                    if self._todo_graph.out_degree(node) == 0:
                        self._add_unblocked_node(node)
                    else:
                        unblocked.add(node)
            if not unblocked:
                return
            # Add the unblocked nodes that do have outgoing edges, starting
            # with those that have the fewest outgoing edges, while recursing
            # to include the nodes those unblock.
            for node in sorted(unblocked, key=lambda n: self._todo_graph.out_degree(n), reverse=True):
                self._add_unblocked_node(node)

    def _add_connected_graph(self, xgraph: networkx.DiGraph) -> None:
        for node in networkx.algorithms.dag.dag_longest_path(xgraph):
            # Terminate all active edges that are now unblocked, continuing
            # until there are none unblocked other than node.
            self._add_active_unblocked(node)
            ancestors = list(networkx.algorithms.dag.ancestors(self._todo_graph, node))
            if ancestors:
                # Recurse to actually add this node and all of its remaining
                # ancestors to the graph.  Including this node in the subgraph
                # ensures that it's connected.
                ancestors.append(node)
                self._add_connected_graph(self._todo_graph.subgraph(ancestors))
            else:
                self._add_unblocked_node(node)
        # There can't be any remaining nodes in xgraph that had been blocked by
        # the last node, since that would have made them part of the longest
        # path.

    def print(self, stream: TextIO) -> None:
        for row in self:
            print(f"{' ' * row.x}●{' ' * (self.x_max - row.x)} {row.node}", file=stream)

    def __iter__(self) -> Iterator[LayoutRow]:
        active_edges: dict[_K, set[_K]] = {}
        for node, node_x in self._locations.items():
            row = LayoutRow(node, self.x_max - node_x)
            for origin, destinations in active_edges.items():
                if node in destinations:
                    row.terminating.append((self.x_max - self._locations[origin], origin))
                    destinations.remove(node)
                if destinations:
                    row.continuing.append(
                        (self.x_max - self._locations[origin], origin, frozenset(destinations))
                    )
            row.terminating.sort()
            row.continuing.sort()
            yield row
            active_edges[node] = set(self._graph.successors(node))
