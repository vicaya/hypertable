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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"

#include <iostream>
#include <log4cpp/Appender.hh>
#include <log4cpp/BasicLayout.hh>
#include <log4cpp/FileAppender.hh>
#include <log4cpp/Layout.hh>
#include <log4cpp/NDC.hh>
#include <log4cpp/Priority.hh>

#include "Logger.h"

using namespace Hypertable;
namespace Logging = log4cpp;

namespace {

  /**
   * NoTimeLayout
   **/
  class  NoTimeLayout : public Logging::Layout {
  public:
    NoTimeLayout() { }
    virtual ~NoTimeLayout() { }
    virtual String format(const Logging::LoggingEvent& event) {
      return Hypertable::format("%s %s %s: %s\n",
          Logging::Priority::getPriorityName(event.priority).c_str(),
          event.categoryName.c_str(), event.ndc.c_str(),
          event.message.c_str());
    }
  };

  /**
   * FlushableAppender appender with flush method
   */
  class FlushableAppender : public Logging::LayoutAppender {
  public:
    FlushableAppender(const String &name) : Logging::LayoutAppender(name) {}
    virtual ~FlushableAppender() {}
    virtual void flush() = 0;

    bool
    set_flush_per_log(bool choice) {
      bool old = m_flush_per_log;
      m_flush_per_log = choice;
      return old;
    }

  protected:
    bool m_flush_per_log;
  };

  /**
   * FlushableOstreamAppender appends LoggingEvents to ostreams.
   */
  class FlushableOstreamAppender : public FlushableAppender {
  public:
    FlushableOstreamAppender(const String& name, std::ostream &stream,
                             bool flush_per_log) :
                             FlushableAppender(name), m_stream(stream) {
      set_flush_per_log(flush_per_log);
    }
    virtual ~FlushableOstreamAppender() { close(); }

    virtual bool reopen() { return true; }
    virtual void close() { flush(); }
    virtual void flush() { m_stream.flush(); }

  protected:
    virtual void
    _append(const Logging::LoggingEvent& event) {
      m_stream << _getLayout().format(event);

      if (m_flush_per_log)
        m_stream.flush();
    }

  private:
    std::ostream &m_stream;
  };

  FlushableAppender *appender = 0;

} // local namespace

Logging::Category *Logger::logger = 0;
bool Logger::show_line_numbers = true;

void
Logger::initialize(const String &name, int priority, bool flush_per_log,
                   std::ostream &out) {
  appender = new FlushableOstreamAppender("default", out, flush_per_log);
  Logging::Layout* layout = new Logging::BasicLayout();
  appender->setLayout(layout);
  logger = &(Logging::Category::getInstance(name));
  logger->addAppender(appender);
  logger->setAdditivity(false);
  logger->setPriority(priority);
}

void
Logger::set_level(int priority) {
  logger->setPriority(priority);
}

void
Logger::suppress_line_numbers() {
  Logger::show_line_numbers = false;
}

void
Logger::set_test_mode(const String &name) {
  Logging::FileAppender *appender;
  Logger::show_line_numbers = false;
  logger->removeAllAppenders();
  appender = new Logging::FileAppender(name, 1);
  appender->setLayout(new NoTimeLayout());
  logger->setAppender(appender);
}

bool
Logger::set_flush_per_log(bool choice) {
  return appender->set_flush_per_log(choice);
}

void
Logger::flush() {
  appender->flush();
}
