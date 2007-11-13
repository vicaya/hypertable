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

#include "Logger.h"

#include <log4cpp/Appender.hh>
#include <log4cpp/BasicLayout.hh>
#include <log4cpp/FileAppender.hh>
#include <log4cpp/Layout.hh>
#include <log4cpp/NDC.hh>
#include <log4cpp/OstreamAppender.hh>
#include <log4cpp/Priority.hh>

using namespace Hypertable;

namespace {

  /**
   * NoTimeLayout
   **/
  class  NoTimeLayout : public log4cpp::Layout {
  public:
    NoTimeLayout() { }
    virtual ~NoTimeLayout() { }
    virtual std::string format(const log4cpp::LoggingEvent& event) {
      std::ostringstream message;
      const std::string& priorityName = log4cpp::Priority::getPriorityName(event.priority);
      message << priorityName << " " << event.categoryName << " " << event.ndc << ": " << event.message << std::endl;
      return message.str();
    }
  };

}


log4cpp::Category *Logger::logger = 0;
bool Logger::ms_show_line_numbers = true;


void Logger::initialize(const char *name, log4cpp::Priority::Value priority) {
  log4cpp::Appender* appender = 
    new log4cpp::OstreamAppender("default", &std::cout);
  log4cpp::Layout* layout = new log4cpp::BasicLayout();
  appender->setLayout(layout);
  logger = &(log4cpp::Category::getInstance(std::string(name)));
  logger->addAppender(appender);
  logger->setAdditivity(false);
  logger->setPriority(priority);
}


void Logger::set_level(log4cpp::Priority::Value priority) {
  logger->setPriority(priority);
}

void Logger::suppress_line_numbers() {
  ms_show_line_numbers = false;
}


void Logger::set_test_mode(const char *name) {
  log4cpp::FileAppender *appender;

  ms_show_line_numbers = false;

  logger->removeAllAppenders();
  appender = new log4cpp::FileAppender((std::string)name, 1);
  appender->setLayout(new NoTimeLayout());
  logger->setAppender(appender);
}
