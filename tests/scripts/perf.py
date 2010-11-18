#!/usr/bin/python

# This script uses test times frim a golden CTest log to detect performance regressions.
# To trigger an error the difference between test time and golden time must be
# test_time > relative_threshold * golden_time && test_time - golden_time > absolute threshold

import time
import sys
import re
import os
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-g", "--goldenFile", action="store", dest="golden_file",
                  help="File with golden perf numbers",
                  default="~/build/hypertable-test-files/LastTest.log.golden")
parser.add_option("-t", "--testFile", action="store", dest="test_file",
                  help="Output data file",
                  default="~/build/hypertable/Testing/Temporary/LastTest.log")
parser.add_option("-r", "--relativeThreshold", action="store", dest="relative_threshold",
                  help="Max allowable relative difference in test times", default=1.1)
parser.add_option("-a", "--absoluteThreshold", action="store", dest="absolute_threshold",
                  help="Max allowable absolute difference (s) in test times", default=2)



(options, args) = parser.parse_args()

test_file = os.path.expanduser(options.test_file)
golden_file = os.path.expanduser(options.golden_file)
relative_threshold = float(options.relative_threshold)
absolute_threshold = int(options.absolute_threshold)
print "================================================================================="
print "Golden file: ", golden_file
print "Test file: ", test_file
print "Relative Threshold: ", relative_threshold
print "Absolute Threshold: ", absolute_threshold, "s"

def parsePerfState(filename):
  hash = {}
  with open(filename, 'r') as file:
    while True:
      line = file.readline()
      if(len(line) == 0):
        return hash
      match = re.search('^(.*)(\stime\selapsed:\s*)(.*)\s*$', line)
      if (match):
        test_name = parseName(match.group(1))
        time = parseTime(match.group(3))
        hash[test_name] = time

def parseName(test_string):
  test = test_string.strip('"')
  return test

def parseTime(time_string):
  components = re.split(":", time_string)
  time = int(components[0])*60*60 + int(components[1])*60 + int(components[2])
  return time

def printReport(errors, warnings):
  global golden_file, test_file, threshold
  print "================================================================================="
  print "Errors: ", len(errors)
  for error in errors:
    print error
  print "================================================================================="
  print "Warnings: ", len(warnings)
  for warning in warnings:
    print warning
  print "================================================================================="

golden = parsePerfState(golden_file)
test = parsePerfState(test_file)
errors = []
warnings = []

for name, time in test.iteritems():
  if (name in golden):
    golden_time = golden[name]
    if (time > int(float(golden_time)*1.1) and time-golden_time > absolute_threshold):
      error = "Error:\t" + name +  "\tTest: " + str(time) + "s\tGolden: " + str(golden[name]) + "s"
      errors.append(error)
  else:
    warning = "Warning:\t" + name +  "\tTest: " + str(time) + "s\t not found in Golden file"
    warnings.append(warning)

printReport(errors, warnings)
