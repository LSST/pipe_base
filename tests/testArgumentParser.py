#!/usr/bin/env python
# 
# LSST Data Management System
# Copyright 2008-2013 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import itertools
import os
import unittest

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.config as pexConfig
import lsst.pex.logging as pexLog
import lsst.pipe.base as pipeBase

ObsTestDir = eups.productDir("obs_test")
if not ObsTestDir:
    print "Warning: you must setup obs_test to run these tests"
else:
    DataPath = os.path.join(ObsTestDir, "data", "input")
LocalDataPath = os.path.join(eups.productDir("pipe_base"), "tests", "data")
#
# Context manager to intercept stdout/err
#  http://stackoverflow.com/questions/5136611/capture-stdout-from-a-script-in-python
#
# Use as:
#   with capture() as out:
#      print 'hi'
#
import contextlib
@contextlib.contextmanager
def capture():
    import sys
    from cStringIO import StringIO
    oldout,olderr = sys.stdout, sys.stderr
    try:
        out=[StringIO(), StringIO()]
        sys.stdout,sys.stderr = out
        yield out
    finally:
        sys.stdout,sys.stderr = oldout, olderr
        out[0] = out[0].getvalue()
        out[1] = out[1].getvalue()

class SubConfig(pexConfig.Config):
    intItem = pexConfig.Field(doc="sample int field", dtype=int, default=8)

class SampleConfig(pexConfig.Config):
    boolItem = pexConfig.Field(doc="sample bool field", dtype=bool, default=True)
    floatItem = pexConfig.Field(doc="sample float field", dtype=float, default=3.1)
    strItem = pexConfig.Field(doc="sample str field", dtype=str, default="strDefault")
    subItem = pexConfig.ConfigField(doc="sample subfield", dtype=SubConfig)
    
class ArgumentParserTestCase(unittest.TestCase):
    """A test case for ArgumentParser."""
    def setUp(self):
        self.ap = pipeBase.ArgumentParser(name="argumentParser")
        self.ap.add_id_argument("--id", "raw", "help text")
        self.ap.add_id_argument("--otherId", "raw", "more help")
        self.config = SampleConfig()
        os.environ.pop("PIPE_INPUT_ROOT", None)
        os.environ.pop("PIPE_CALIB_ROOT", None)
        os.environ.pop("PIPE_OUTPUT_ROOT", None)
    
    def tearDown(self):
        del self.ap
        del self.config
    
    def testBasicId(self):
        """Test --id basics"""
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath, "--id", "visit=1", "filter=g"],
        )
        self.assertEqual(len(namespace.id.idList), 1)
        self.assertEqual(len(namespace.id.refList), 1)
        
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath, "--id", "visit=22", "filter=g"],
        )
        self.assertEqual(len(namespace.id.idList), 1)
        self.assertEqual(len(namespace.id.refList), 0) # no data for this ID
        
    def testOtherId(self):
        """Test --other"""
        # By itself
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath, "--other", "visit=99"],
        )
        self.assertEqual(len(namespace.otherId.idList), 1)
        self.assertEqual(len(namespace.otherId.refList), 0)

        # And together
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath, "--id", "visit=1",
                    "--other", "visit=99"],
        )
        self.assertEqual(len(namespace.id.idList), 1)
        self.assertEqual(len(namespace.id.refList), 1)
        self.assertEqual(len(namespace.otherId.idList), 1)
        self.assertEqual(len(namespace.otherId.refList), 0) # no data for this ID

    def testIdCross(self):
        """Test --id cross product, including order"""
        visitList = (1, 2, 3)
        filterList = ("g", "r")
        namespace = self.ap.parse_args(
            config = self.config,
            args = [
                DataPath,
                "--id",
                "filter=%s" % ("^".join(filterList),),
                "visit=%s" % ("^".join(str(visit) for visit in visitList),),
            ]
        )
        self.assertEqual(len(namespace.id.idList), 6)
        predValList =  itertools.product(filterList, visitList)
        for id, predVal in itertools.izip(namespace.id.idList, predValList):
            idVal = tuple(id[key] for key in ("filter", "visit"))
            self.assertEqual(idVal, predVal)
        self.assertEqual(len(namespace.id.refList), 3) # only have data for three of these
    
    def testIdDuplicate(self):
        """Verify that each ID name can only appear once in a given ID argument"""
        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            args = [DataPath, "--id", "visit=1", "visit=2"],
        )
    
    def testConfigBasics(self):
        """Test --config"""
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath, "--config", "boolItem=False", "floatItem=-67.1",
                "strItem=overridden value", "subItem.intItem=5"],
        )
        self.assertEqual(namespace.config.boolItem, False)
        self.assertEqual(namespace.config.floatItem, -67.1)
        self.assertEqual(namespace.config.strItem, "overridden value")
        self.assertEqual(namespace.config.subItem.intItem, 5)

    def testConfigLeftToRight(self):
        """Verify that order of overriding config values is left to right"""
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath,
                "--config", "floatItem=-67.1", "strItem=overridden value",
                "--config", "strItem=final value"],
        )
        self.assertEqual(namespace.config.floatItem, -67.1)
        self.assertEqual(namespace.config.strItem, "final value")
    
    def testConfigWrongNames(self):
        """Verify that incorrect names for config fields are caught"""
        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            args = [DataPath, "--config", "missingItem=-67.1"],
        )
    
    def testShow(self):
        """Test --show"""
        with capture() as out:
            self.ap.parse_args(
                config = self.config,
                args = [DataPath, "--show", "config", "data", "tasks", "run"],
            )
        res = out[0]
        self.assertTrue("config.floatItem" in res)
        self.assertTrue("config.subItem" in res)
        self.assertTrue("config.boolItem" in res)
        self.assertTrue("config.strItem" in res)

        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            args = [DataPath, "--show", "config"],
        )

        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            args = [DataPath, "--show", "config=X"],
        )
        with capture() as out:
            self.ap.parse_args(self.config, [DataPath, "--show", "config=*strItem*", "run"])
        stdout = out[0]
        answer = stdout.rstrip().split('\n')
        self.assertEqual(len(answer), 1)         # only 1 config item matches
        self.assertTrue("strDefault" in answer[0])

        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            args = [DataPath, "--show", "badname", "run"],
        )
        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            args = [DataPath, "--show"], # no --show arguments
        )
        
    def testConfigFileBasics(self):
        """Test --configfile"""
        configFilePath = os.path.join(LocalDataPath, "argumentParserConfig.py")
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath, "--configfile", configFilePath],
        )
        self.assertEqual(namespace.config.floatItem, -9e9)
        self.assertEqual(namespace.config.strItem, "set in override file")
       
    def testConfigFileLeftRight(self):
        """verify that order of setting values is with a mix of config file and config is left to right"""
        configFilePath = os.path.join(LocalDataPath, "argumentParserConfig.py")
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath,
                "--config", "floatItem=5.5",
                "--configfile", configFilePath,
                "--config", "strItem=value from cmd line"],
        )
        self.assertEqual(namespace.config.floatItem, -9e9)
        self.assertEqual(namespace.config.strItem, "value from cmd line")

    def testConfigFileMissingFiles(self):
        """Verify that missing config override files are caught"""
        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            args = [DataPath, "--configfile", "missingFile"],
        )

    def testAtFile(self):
        """Test @file"""
        argPath = os.path.join(LocalDataPath, "args.txt")
        namespace = self.ap.parse_args(
            config = self.config,
            args = [DataPath, "@%s" % (argPath,)],
        )
        self.assertEqual(len(namespace.id.idList), 1)
        self.assertEqual(namespace.config.floatItem, 4.7)
        self.assertEqual(namespace.config.strItem, "new value")
    
    def testLogDest(self):
        """Test --logdest
        """
        logFile = "tests_argumentParser_testLog_temp.txt"
        self.ap.parse_args(
            config = self.config,
            args = [DataPath, "--logdest", logFile],
        )
        self.assertTrue(os.path.isfile(logFile))
        os.remove(logFile)
    
    def testLogLevel(self):
        """Test --loglevel"""
        for logLevel in ("debug", "Info", "WARN", "fatal"):
            intLevel = getattr(pexLog.Log, logLevel.upper())
            namespace = self.ap.parse_args(
                config = self.config,
                args = [DataPath, "--loglevel", logLevel],
            )
            self.assertEqual(namespace.log.getThreshold(), intLevel)

            namespace = self.ap.parse_args(
                config = self.config,
                args = [DataPath, "--loglevel", str(intLevel)],
            )
            self.assertEqual(namespace.log.getThreshold(), intLevel)
        
        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            args = [DataPath,
                "--loglevel", "INVALID_LEVEL",
            ],
        )
    
    def testTrace(self):
        """Test --trace
        
        I'm not sure how to verify that tracing is enabled; just see if it runs
        """
        self.ap.parse_args(
            config = self.config,
            args = [DataPath,
                "--trace", "afw.math=2", "meas.algorithms=3",
                "--trace", "something=5",
            ],
        )
        pexLog.Trace("something", 4, "You should see this message")
        pexLog.Trace("something", 6, "You should not see this message")
    
    def testPipeVars(self):
        """Test handling of $PIPE_x_ROOT environment variables, where x is INPUT, CALIB or OUTPUT
        """
        os.environ["PIPE_INPUT_ROOT"] = DataPath
        namespace = self.ap.parse_args(
            config = self.config,
            args = ["."],
        )
        self.assertEqual(namespace.input, os.path.abspath(DataPath))
        self.assertEqual(namespace.calib, None)
        self.assertEqual(namespace.output, None)

        os.environ["PIPE_CALIB_ROOT"] = DataPath
        namespace = self.ap.parse_args(
            config = self.config,
            args = ["."],
        )
        self.assertEqual(namespace.input, os.path.abspath(DataPath))
        self.assertEqual(namespace.calib, os.path.abspath(DataPath))
        self.assertEqual(namespace.output, None)
        
        os.environ.pop("PIPE_CALIB_ROOT", None)
        os.environ["PIPE_OUTPUT_ROOT"] = DataPath
        namespace = self.ap.parse_args(
            config = self.config,
            args = ["."],
        )
        self.assertEqual(namespace.input, os.path.abspath(DataPath))
        self.assertEqual(namespace.calib, None)
        self.assertEqual(namespace.output, os.path.abspath(DataPath))

    def testBareHelp(self):
        """Make sure bare help does not print an error message (ticket #3090)
        """
        for helpArg in ("-h", "--help"):
            try:
                self.ap.parse_args(
                    config = self.config,
                    args = [helpArg],
                )
                self.fail("should have raised SystemExit")
            except SystemExit, e:
                self.assertEqual(e.code, 0)

def suite():
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(ArgumentParserTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    if ObsTestDir is None:
        return
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
