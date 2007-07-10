/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef HYPERTABLE_TESTHARNESS_H
#define HYPERTABLE_TESTHARNESS_H

#include <log4cpp/FileAppender.hh>
#include <log4cpp/Layout.hh>

#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <errno.h>
}

#include "Logger.h"

using namespace std;

namespace hypertable {

  /**
   * NoTimeLayout
   **/
  class  NoTimeLayout : public log4cpp::Layout {
  public:
    NoTimeLayout() { }
    virtual ~NoTimeLayout() { }
    virtual string format(const log4cpp::LoggingEvent& event) {
      std::ostringstream message;
      const std::string& priorityName = log4cpp::Priority::getPriorityName(event.priority);
      message << priorityName << " " << event.categoryName << " " << event.ndc << ": " << event.message << std::endl;
      return message.str();
    }
  };

  class TestHarness {
  public:
    TestHarness(const char *name) {

      mName = name;

      Logger::Initialize(name);

      // open temporary output file
      sprintf(mOutputFile, "%sXXXXXX", name);
      if (mktemp(mOutputFile) == 0) {
	LOG_VA_ERROR("mktemp(\"%s\") failed - %s", mOutputFile, strerror(errno));
	exit(1);
      }

      if ((mFd = open(mOutputFile, O_CREAT | O_TRUNC | O_WRONLY, 0644)) < 0) {
	LOG_VA_ERROR("open(%s) failed - %s", mOutputFile, strerror(errno));
	exit(1);
      }

      mLogStream.open(mOutputFile);

      Logger::logger->removeAllAppenders();
      mAppender = new log4cpp::FileAppender((string)name, mFd);
      mAppender->setLayout(new NoTimeLayout());
      Logger::logger->setAppender(mAppender);
    }
    ~TestHarness() {
      unlink(mOutputFile);
    }

    int GetLogFileDescriptor() { return mFd; }

    ostream &GetLogStream() { return mLogStream; }

    void ValidateAndExit(const char *goldenFile) {
      int exitVal = 0;
      mLogStream << flush;
      string command = (string)"diff " + mOutputFile + " " + goldenFile;
      if (system(command.c_str()))
	exitVal = 1;
      if (exitVal == 0)
	unlink(mOutputFile);
      else
	cerr << "Diff Error:  " << command << endl;
      exit(exitVal);
    }

    void RegenerateGoldenFile(const char *goldenFile) {
      string command = (string)"cp " + mOutputFile + " " + goldenFile;      
      system(command.c_str());
    }

    void ClearOutput() {
      if (!mAppender->reopen()) {
	LOG_VA_ERROR("Problem re-opening logging output file %s", mOutputFile);
	DisplayErrorAndExit();
      }
    }

    void DisplayErrorAndExit() {
      mLogStream << flush;
      cerr << "Error, see '" << mOutputFile << "'" << endl;
      /*
      string command = (string)"cat " + mOutputFile;
      system(command.c_str());
      unlink(mOutputFile);
      */
      exit(1);
    }

  private:
    char mOutputFile[128];
    log4cpp::FileAppender *mAppender;
    const char *mName;
    int mFd;
    ofstream mLogStream;
  };
  
}

#endif // HYPERTABLE_TESTHARNESS_H
