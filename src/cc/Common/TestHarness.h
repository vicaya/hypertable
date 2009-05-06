/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
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


namespace Hypertable {

  class TestHarness {
  public:
    TestHarness(const char *name) : m_name(name), m_error(0) {
      Logger::initialize(name);

      // open temporary output file
      sprintf(m_output_file, "%s%d", name, getpid());

      if ((m_fd = open(m_output_file, O_CREAT | O_TRUNC | O_WRONLY, 0644))
          < 0) {
        HT_ERRORF("open(%s) failed - %s", m_output_file, strerror(errno));
        exit(1);
      }

      Logger::set_test_mode(name, m_fd);
    }
    ~TestHarness() {
      if (!m_error)
        unlink(m_output_file);
    }

    int get_log_file_descriptor() { return m_fd; }

    void validate_and_exit(const char *golden_file) {
      validate(golden_file);
      _exit(m_error);
    }
    
    int validate(const char *golden_file) {
      close(m_fd);
      String command = (String)"diff " + m_output_file + " " + golden_file;
      m_error = system(command.c_str());

      if (m_error)
        std::cerr << "Command: "<< command << "exited with "<< m_error
                  << ", see '" << m_output_file << "'" << std::endl;

      return m_error;
    }

    void regenerate_golden_file(const char *golden_file) {
      String command = (String)"mv " + m_output_file + " " + golden_file;
      system(command.c_str());
    }

    void clear_output() {
      if (!m_appender->reopen()) {
        HT_ERRORF("Problem re-opening logging output file %s", m_output_file);
        display_error_and_exit();
      }
    }

    void display_error_and_exit() {
      close(m_fd);
      std::cerr << "Error, see '" << m_output_file << "'" << std::endl;
      _exit(1);
    }

  private:
    char m_output_file[128];
    log4cpp::FileAppender *m_appender;
    const char *m_name;
    int m_fd;
    int m_error;
  };

} // namespace Hypertable

#endif // HYPERTABLE_TESTHARNESS_H
