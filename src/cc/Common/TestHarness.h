/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

namespace Hypertable {

  /**
   * Log4cpp layout class that drops timestamp from log messages.
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

      m_name = name;

      Logger::initialize(name);

      // open temporary output file
      sprintf(m_output_file, "%s%d", name, getpid());

      if ((m_fd = open(m_output_file, O_CREAT | O_TRUNC | O_WRONLY, 0644)) < 0) {
	HT_ERRORF("open(%s) failed - %s", m_output_file, strerror(errno));
	exit(1);
      }

      m_log_stream.open(m_output_file);

      Logger::logger->removeAllAppenders();
      m_appender = new log4cpp::FileAppender((string)name, m_fd);
      m_appender->setLayout(new NoTimeLayout());
      Logger::logger->setAppender(m_appender);
    }
    ~TestHarness() {
      unlink(m_output_file);
    }

    int get_log_file_descriptor() { return m_fd; }

    ostream &get_log_stream() { return m_log_stream; }

    void validate_and_exit(const char *goldenFile) {
      int exitVal = 0;
      m_log_stream << flush;
      string command = (string)"diff " + m_output_file + " " + goldenFile;
      if (system(command.c_str()))
	exitVal = 1;
      if (exitVal == 0)
	unlink(m_output_file);
      else
	cerr << "Diff Error:  " << command << endl;
      exit(exitVal);
    }

    void regenerate_golden_file(const char *goldenFile) {
      string command = (string)"cp " + m_output_file + " " + goldenFile;      
      system(command.c_str());
    }

    void clear_output() {
      if (!m_appender->reopen()) {
	HT_ERRORF("Problem re-opening logging output file %s", m_output_file);
	display_error_and_exit();
      }
    }

    void display_error_and_exit() {
      m_log_stream << flush;
      cerr << "Error, see '" << m_output_file << "'" << endl;
      /*
      string command = (string)"cat " + m_output_file;
      system(command.c_str());
      unlink(m_output_file);
      */
      exit(1);
    }

  private:
    char m_output_file[128];
    log4cpp::FileAppender *m_appender;
    const char *m_name;
    int m_fd;
    ofstream m_log_stream;
  };
  
}

#endif // HYPERTABLE_TESTHARNESS_H
