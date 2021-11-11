# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "NoDimensionsTestConnections",
    "NoDimensionsTestConfig",
    "NoDimensionsTestTask",
)

from typing import Dict

from lsst.pex.config import Field
from lsst.pipe.base import (
    PipelineTask,
    PipelineTaskConfig,
    PipelineTaskConnections,
    Struct,
    connectionTypes,
)


class NoDimensionsTestConnections(PipelineTaskConnections, dimensions=set()):
    input = connectionTypes.Input(
        name="input", doc="some dict-y input data for testing", storageClass="StructuredDataDict"
    )
    output = connectionTypes.Output(
        name="output", doc="some dict-y output data for testing", storageClass="StructuredDataDict"
    )


class NoDimensionsTestConfig(PipelineTaskConfig, pipelineConnections=NoDimensionsTestConnections):
    key = Field(dtype=str, doc="String key for the dict entry the task sets.", default="one")
    value = Field(dtype=int, doc="Integer value for the dict entry the task sets.", default=1)


class NoDimensionsTestTask(PipelineTask):
    """A simple PipelineTask intended for tests that only need trivial
    relationships between tasks and datasets.

    The quanta and input and output datasets of this task have no dimensions,
    so they use trivial, empty data IDs and require little data repository prep
    work to be used.
    """

    ConfigClass = NoDimensionsTestConfig
    _DefaultName = "noDimensionsTest"

    def run(self, input: Dict[str, int]) -> Struct:
        """Run the task, adding the configured key-value pair to the input
        argument and returning it as the output.

        Parameters
        ----------
        input : `dict`
            Dictionary to update and return.

        Returns
        -------
        result : `lsst.pipe.base.Struct`
            Struct with a single ``output`` attribute.
        """
        output = input.copy()
        output[self.config.key] = self.config.value
        return Struct(output=output)