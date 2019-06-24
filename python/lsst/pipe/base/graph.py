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

"""Module defining quantum graph classes and related methods.

There could be different representations of the quantum graph depending
on the client needs. Presently this module contains graph implementation
which is based on requirements of command-line environment. In the future
we could add other implementations and methods to convert between those
representations.
"""

# "exported" names
__all__ = ["QuantumGraph", "QuantumGraphTaskNodes", "QuantumIterData"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from itertools import chain
from dataclasses import dataclass
from typing import List, FrozenSet, Mapping

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .pipeline import Pipeline, TaskDef
from .pipeTools import orderPipeline
from lsst.daf.butler import DataId, Quantum, DatasetRef, DatasetType

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


@dataclass
class QuantumIterData:
    """Helper class for iterating over quanta in a graph.

    The `QuantumGraph.traverse` method needs to return topologically ordered
    Quanta together with their dependencies. This class is used as a value
    for the iterator, it contains enumerated Quantum and its dependencies.
    """

    __slots__ = ["quantumId", "quantum", "taskDef", "dependencies"]

    quantumId: int
    """Index of this Quantum, a unique but arbitrary integer."""

    quantum: Quantum
    """Quantum corresponding to a graph node."""

    taskDef: TaskDef
    """Task class to be run on this quantum, and corresponding label and
    config.
    """

    dependencies: FrozenSet(int)
    """Possibly empty set of indices of dependencies for this Quantum.
    Dependnecies include other nodes in the graph; they do not reflect data
    already in butler (there are no graph nodes for those).
    """


@dataclass
class QuantumGraphTaskNodes:
    """QuantumGraphTaskNodes represents a bunch of nodes in an quantum graph
    corresponding to a single task.

    The node in quantum graph is represented by the `PipelineTask` and a
    single `~lsst.daf.butler.Quantum` instance. One possible representation
    of the graph is just a list of nodes without edges (edges can be deduced
    from nodes' quantum inputs and outputs if needed). That representation can
    be reduced to the list of PipelineTasks (or their corresponding TaskDefs)
    and the corresponding list of Quanta. This class is used in this reduced
    representation for a single task, and full `QuantumGraph` is a sequence of
    tinstances of this class for one or more tasks.

    Different frameworks may use different graph representation, this
    representation was based mostly on requirements of command-line
    executor which does not need explicit edges information.
    """

    taskDef: TaskDef
    """Task defintion for this set of nodes."""

    quanta: List[Quantum]
    """List of quanta corresponding to the task."""

    initInputs: Mapping[DatasetType, DatasetRef]
    """Datasets that must be loaded or created to construct this task."""

    initOutputs: Mapping[DatasetType, DatasetRef]
    """Datasets that may be written after constructing this task."""


class QuantumGraph(list):
    """QuantumGraph is a sequence of `QuantumGraphTaskNodes` objects.

    Typically the order of the tasks in the list will be the same as the
    order of tasks in a pipeline (obviously depends on the code which
    constructs graph).

    Parameters
    ----------
    iterable : iterable of `QuantumGraphTaskNodes`, optional
        Initial sequence of per-task nodes.
    """
    def __init__(self, iterable=None):
        list.__init__(self, iterable or [])

    def quanta(self):
        """Iterator over quanta in a graph.

        Quanta are returned in unspecified order.

        Yields
        ------
        taskDef : `TaskDef`
            Task definition for a Quantum.
        quantum : `~lsst.daf.butler.Quantum`
            Single quantum.
        """
        for taskNodes in self:
            taskDef = taskNodes.taskDef
            for quantum in taskNodes.quanta:
                yield taskDef, quantum

    def traverse(self):
        """Return topologically ordered Quanta and their dependencies.

        This method iterates over all Quanta in topological order, enumerating
        them during iteration. Returned `QuantumIterData` object contains
        Quantum instance, its ``quantumId`` and ``quantumId`` of all its
        prerequsites (Quanta that produce inputs for this Quantum):
        - the ``quantumId`` values are generated by an iteration of a
          QuantumGraph, and are not intrinsic to the QuantumGraph
        - during iteration, each ID will appear in quantumId before it ever
          appears in dependencies.

        Yields
        ------
        quantumData : `QuantumIterData`
        """

        def orderedTaskNodes(graph):
            """Return topologically ordered task nodes.

            Yields
            ------
            nodes : `QuantumGraphTaskNodes`
            """
            # Tasks in a graph are probably topologically sorted already but there
            # is no guarantee for that. Just re-construct Pipeline and order tasks
            # in a pipeline using existing method.
            nodesMap = {id(item.taskDef): item for item in graph}
            pipeline = orderPipeline(Pipeline(item.taskDef for item in graph))
            for taskDef in pipeline:
                yield nodesMap[id(taskDef)]

        index = 0
        outputs = {}  # maps (DatasetType.name, DataId) to its producing quantum index
        for nodes in orderedTaskNodes(self):
            for quantum in nodes.quanta:

                # Find quantum dependencies (must be in `outputs` already)
                prereq = []
                for dataRef in chain.from_iterable(quantum.predictedInputs.values()):
                    # if data exists in butler then `id` is not None
                    if dataRef.id is None:
                        key = (dataRef.datasetType.name, DataId(dataRef.dataId))
                        try:
                            prereq.append(outputs[key])
                        except KeyError:
                            # The Quantum that makes our inputs is not in the graph,
                            # this could happen if we run on a "split graph" which is
                            # usually just one quantum. Check for number of Quanta
                            # in a graph and ignore error if it's just one.
                            # TODO: This code has to be removed or replaced with
                            # something more generic
                            if not (len(self) == 1 and len(self[0].quanta) == 1):
                                raise

                # Update `outputs` with this quantum outputs
                for dataRef in chain.from_iterable(quantum.outputs.values()):
                    key = (dataRef.datasetType.name, DataId(dataRef.dataId))
                    outputs[key] = index

                yield QuantumIterData(index=index, quantum=quantum, taskDef=nodes.taskDef,
                                      dependencies=frozenset(prereq))
                index += 1
