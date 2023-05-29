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

__all__ = ("DatasetTypeNode",)

import dataclasses
from typing import TYPE_CHECKING, Any

import networkx
from lsst.daf.butler import DatasetRef, DatasetType, DimensionGraph, Registry, StorageClass
from lsst.daf.butler.registry import MissingDatasetTypeError

from ._exceptions import DuplicateOutputError
from ._nodes import NodeKey, NodeType

if TYPE_CHECKING:
    from ._edges import ReadEdge, WriteEdge


@dataclasses.dataclass(frozen=True, eq=False)
class DatasetTypeNode:
    """A node in a pipeline graph that represents a resolved dataset type.

    Notes
    -----
    A dataset type node represents a common definition of the dataset type
    across the entire graph - it is never a component, and the storage class is
    the registry dataset type's storage class or (if there isn't one) the one
    defined by the producing task.

    Dataset type nodes are intentionally not equality comparable, since there
    are many different (and useful) ways to compare these objects with no clear
    winner as the most obvious behavior.
    """

    dataset_type: DatasetType
    """Common definition of this dataset type for the graph.
    """

    is_initial_query_constraint: bool
    """Whether this dataset should be included as a constraint in the initial
    query for data IDs in QuantumGraph generation.

    This is only `True` for dataset types that are overall regular inputs, and
    only if none of those input connections had ``deferQueryConstraint=True``.
    """

    is_prerequisite: bool
    """Whether this dataset type is a prerequisite input that must exist in
    the Registry before graph creation.
    """

    @classmethod
    def _from_edges(
        cls, key: NodeKey, xgraph: networkx.MultiDiGraph, registry: Registry, previous: DatasetTypeNode | None
    ) -> DatasetTypeNode:
        """Construct a dataset type node from its edges.

        Parameters
        ----------
        key : `NodeKey`
            Named tuple that holds the dataset type and serves as the node
            object in the internal networkx graph.
        xgraph : `networkx.MultiDiGraph`
            The internal networkx graph.
        registry : `lsst.daf.butler.Registry`
            Registry client for the data repository.  Only used to get
            dataset type definitions and the dimension universe.
        previous : `DatasetTypeNode` or `None`
            Previous node for this dataset type.

        Returns
        -------
        node : `DatasetTypeNode`
            Node consistent with all edges pointing to it and the data
            repository.
        """
        try:
            dataset_type = registry.getDatasetType(key.name)
            is_registered = True
        except MissingDatasetTypeError:
            dataset_type = None
            is_registered = False
        if previous is not None and previous.dataset_type == dataset_type:
            # This node was already resolved (with exactly the same edges
            # contributing, since we clear resolutions when edges are added or
            # removed).  The only thing that might have changed was the
            # definition in the registry, and it didn't.
            return previous
        is_initial_query_constraint = True
        is_prerequisite: bool | None = None
        producer: str | None = None
        write_edge: WriteEdge
        for _, _, write_edge in xgraph.in_edges(key, data="instance"):  # will iterate zero or one time
            if producer is not None:
                raise DuplicateOutputError(
                    f"Dataset type {key.name!r} is produced by both {write_edge.task_label!r} "
                    f"and {producer!r}."
                )
            producer = write_edge.task_label
            dataset_type = write_edge._resolve_dataset_type(dataset_type, universe=registry.dimensions)
            is_prerequisite = False
            is_initial_query_constraint = False
        read_edge: ReadEdge
        consumers: list[str] = []
        read_edges = list(read_edge for _, _, read_edge in xgraph.out_edges(key, data="instance"))
        # Put edges that are not component datasets before any edges that are.
        read_edges.sort(key=lambda read_edge: read_edge.component is not None)
        for read_edge in read_edges:
            dataset_type, is_initial_query_constraint, is_prerequisite = read_edge._resolve_dataset_type(
                current=dataset_type,
                universe=registry.dimensions,
                is_initial_query_constraint=is_initial_query_constraint,
                is_prerequisite=is_prerequisite,
                is_registered=is_registered,
                producer=producer,
                consumers=consumers,
            )
            consumers.append(read_edge.task_label)
        assert dataset_type is not None, "Graph structure guarantees at least one edge."
        assert is_prerequisite is not None, "Having at least one edge guarantees is_prerequisite is known."
        return DatasetTypeNode(
            dataset_type=dataset_type,
            is_initial_query_constraint=is_initial_query_constraint,
            is_prerequisite=is_prerequisite,
        )

    @property
    def name(self) -> str:
        """Name of the dataset type.

        This is always the parent dataset type, never that of a component.
        """
        return self.dataset_type.name

    @property
    def key(self) -> NodeKey:
        """Key that identifies this dataset type in internal and exported
        networkx graphs.
        """
        return NodeKey(NodeType.DATASET_TYPE, self.dataset_type.name)

    @property
    def dimensions(self) -> DimensionGraph:
        """Dimensions of the dataset type."""
        return self.dataset_type.dimensions

    @property
    def storage_class_name(self) -> str:
        """String name of the storage class for this dataset type."""
        return self.dataset_type.storageClass_name

    @property
    def storage_class(self) -> StorageClass:
        """Storage class for this dataset type."""
        return self.dataset_type.storageClass

    def __repr__(self) -> str:
        return f"{self.name} ({self.storage_class_name}, {self.dimensions})"

    def generalize_ref(self, ref: DatasetRef) -> DatasetRef:
        """Convert a `~lsst.daf.butler.DatasetRef` with the dataset type
        associated with some task to one with the common dataset type defined
        by this node.

        Parameters
        ----------
        ref : `lsst.daf.butler.DatasetRef`
            Reference whose dataset type is convertible to this node's, either
            because it is a component with the node's dataset type as its
            parent, or because it has a compatible storage class.

        Returns
        -------
        ref : `lsst.daf.butler.DatasetRef`
            Reference with exactly this node's dataset type.
        """
        if ref.isComponent():
            ref = ref.makeCompositeRef()
        if ref.datasetType.storageClass_name != self.dataset_type.storageClass_name:
            return ref.overrideStorageClass(self.dataset_type.storageClass_name)
        return ref

    def _to_xgraph_state(self) -> dict[str, Any]:
        """Convert this node's attributes into a dictionary suitable for use
        in exported networkx graphs.
        """
        return {
            "dataset_type": self.dataset_type,
            "is_initial_query_constraint": self.is_initial_query_constraint,
            "is_prerequisite": self.is_prerequisite,
            "dimensions": self.dataset_type.dimensions,
            "storage_class_name": self.dataset_type.storageClass_name,
            "bipartite": NodeType.DATASET_TYPE.bipartite,
        }
