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

"""Utility to facilitate testing of pipelines consisting of multiple steps.
"""

__all__ = ["PipelineStepTester"]

import dataclasses
import unittest

from lsst.daf.butler import Butler, DatasetType
from lsst.pipe.base import Pipeline, PipelineDatasetTypes


@dataclasses.dataclass
class PipelineStepTester:
    """Utility class which facilitates testing of pipelines, optionally
    consisting of multiple steps.

    Two sets will be constructed by looping over the entire pipeline or all
    named subsets within the pipeline: `pure_inputs` and `all_outputs`.

    The `pure_inputs` set consists of all inputs which must be constructed and
    provided as an input into the pipeline (i.e., they will not be generated
    by the named pipeline).

    The `all_outputs` set consists of all dataset types which are generated by
    the named pipeline, either as intermediates or as final outputs.

    These sets will be checked against user-supplied sets to ensure that the
    named pipeline may still be run without raising a missing data error.

    Parameters
    ----------
    filename : `str`
        The full path to the pipeline YAML
    step_suffixes : `list` [`str`]
        A list, in the order of data reduction, of the step subsets to check
    initial_dataset_types : `list` [`tuple`]
        Dataset types which require initial registry by the butler
    expected_inputs : `set` [`str`]
        Dataset types expected as an input into the pipeline
    expected_outputs : `set` [`str`]
        Dataset types expected to be produced as an output by the pipeline
    """

    filename: str
    step_suffixes: list[str]
    initial_dataset_types: list[tuple[str, set[str], str, bool]]
    expected_inputs: set[str]
    expected_outputs: set[str]

    def register_dataset_types(self, butler: Butler) -> None:
        for name, dimensions, storageClass, isCalibration in self.initial_dataset_types:
            butler.registry.registerDatasetType(
                DatasetType(
                    name,
                    dimensions,
                    storageClass=storageClass,
                    isCalibration=isCalibration,
                    universe=butler.dimensions,
                )
            )

    def run(self, butler: Butler, test_case: unittest.TestCase) -> None:
        self.register_dataset_types(butler)

        all_outputs: dict[str, DatasetType] = dict()
        pure_inputs: dict[str, str] = dict()

        for suffix in self.step_suffixes:
            pipeline = Pipeline.from_uri(self.filename + suffix)
            dataset_types = PipelineDatasetTypes.fromPipeline(
                pipeline,
                registry=butler.registry,
                include_configs=False,
                include_packages=False,
            )

            pure_inputs.update({k: suffix for k in dataset_types.prerequisites.names})
            parent_inputs = {t.nameAndComponent()[0] for t in dataset_types.inputs}
            pure_inputs.update({k: suffix for k in parent_inputs - all_outputs.keys()})
            all_outputs.update(dataset_types.outputs.asMapping())
            all_outputs.update(dataset_types.intermediates.asMapping())

            for name in dataset_types.inputs.names & all_outputs.keys():
                test_case.assertTrue(
                    all_outputs[name].is_compatible_with(dataset_types.inputs[name]),
                    msg=(
                        f"dataset type {name} is defined as {dataset_types.inputs[name]} as an "
                        f"input, but {all_outputs[name]} as an output, and these are not compatible."
                    ),
                )

            for dataset_type in dataset_types.outputs | dataset_types.intermediates:
                if not dataset_type.isComponent():
                    butler.registry.registerDatasetType(dataset_type)

        if not pure_inputs.keys() <= self.expected_inputs:
            missing = [f"{k} ({pure_inputs[k]})" for k in pure_inputs.keys() - self.expected_inputs]
            raise AssertionError(f"Got unexpected pure_inputs: {missing}")

        if not all_outputs.keys() >= self.expected_outputs:
            missing = [k for k in self.expected_outputs - all_outputs.keys()]
            raise AssertionError(f"Missing expected_outputs: {missing}")
