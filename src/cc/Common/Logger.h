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
#ifndef HYPERTABLE_LOGGER_H
#define HYPERTABLE_LOGGER_H

#include <iostream>

#include <log4cpp/Category.hh>
#include <log4cpp/Priority.hh>

#define DUMP_CORE *((int *)0) = 1;

namespace Hypertable {
  class Logger {
  public:
    static log4cpp::Category *logger;
    static void initialize(const char *name, log4cpp::Priority::Value priority=log4cpp::Priority::DEBUG);
    static void set_level(log4cpp::Priority::Value priority);
    static void set_test_mode(const char *name);
    static void suppress_line_numbers();
    static bool ms_show_line_numbers;
  };
}

#ifndef DISABLE_LOG_ALL

#ifndef DISABLE_LOG_DEBUG

#define HT_LOG_ENTER \
  if (Logger::logger->isDebugEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->debug("(%s:%d) %s() ENTER", __FILE__, __LINE__, __func__); \
    else \
      Logger::logger->debug("%s() ENTER", __func__); \
    std::cout << std::flush; \
  } while(0)

#define HT_LOG_EXIT \
  if (Logger::logger->isDebugEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->debug("(%s:%d) %s() EXIT", __FILE__, __LINE__, __func__); \
    else \
      Logger::logger->debug("%s() EXIT", __func__); \
    std::cout << std::flush; \
  } while(0)

#define HT_DEBUG(msg) if (Logger::logger->isDebugEnabled()) do { \
  Logger::logger->debug("(%s:%d) " msg, __FILE__, __LINE__) \
} while (0)

#define HT_DEBUGF(msg, ...) \
  if (Logger::logger->isDebugEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->debug("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->debug(msg, __VA_ARGS__);  \
    std::cout << std::flush; \
  } while (0)
#else
#define HT_LOG_ENTER
#define HT_LOG_EXIT
#define HT_DEBUG(msg)
#define HT_DEBUGF(msg, ...)
#endif

#ifndef DISABLE_LOG_INFO
#define HT_INFO(msg) if (Logger::logger->isInfoEnabled()) do { \
  Logger::logger->info("(%s:%d) " msg, __FILE__, __LINE__); \
} while(0)

#define HT_INFOF(msg, ...) \
  if (Logger::logger->isInfoEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->info("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->info(msg, __VA_ARGS__);  \
  } while (0)
#else
#define HT_INFO(msg)
#define HT_INFOF(msg, ...)
#endif

#ifndef DISABLE_LOG_NOTICE
#define HT_NOTICE(msg) if (Logger::logger->isNoticeEnabled()) do { \
  Logger::logger->notice("(%s:%d) " msg, __FILE__, __LINE__); \
} while (0)
#define HT_NOTICEF(msg, ...) \
  if (Logger::logger->isNoticeEnabled()) { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->notice("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->notice(msg, __VA_ARGS__);  \
  } while (0)
#else
#define HT_NOTICE(msg)
#define HT_NOTICEF(msg, ...)
#endif

#ifndef DISABLE_LOG_WARN
#define HT_WARN(msg) if (Logger::logger->isWarnEnabled()) do { \
  Logger::logger->warn("(%s:%d) " msg, __FILE__, __LINE__); \
} while (0)

#define HT_WARNF(msg, ...) \
  if (Logger::logger->isWarnEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->warn("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->warn(msg, __VA_ARGS__);  \
    std::cout << std::flush; \
   } while (0)
#else
#define HT_WARN(msg)
#define HT_WARNF(msg, ...)
#endif

#ifndef DISABLE_LOG_ERROR
#define HT_ERROR(msg) if (Logger::logger->isErrorEnabled()) do { \
  Logger::logger->error("(%s:%d) " msg, __FILE__, __LINE__); \
} while (0)
#define HT_ERRORF(msg, ...) \
  if (Logger::logger->isErrorEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->error("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->error(msg, __VA_ARGS__);  \
    std::cout << std::flush; \
  } while (0)
#else
#define HT_ERROR(msg)
#define HT_ERRORF(msg, ...)
#endif

#ifndef DISABLE_LOG_CRIT
#define HT_CRIT(msg) if (Logger::logger->isCritEnabled()) do { \
  Logger::logger->crit("(%s:%d) " msg, __FILE__, __LINE__); \
} while (0)
#define HT_CRITF(msg, ...) \
  if (Logger::logger->isCritEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->crit("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->crit(msg, __VA_ARGS__);  \
    std::cout << std::flush; \
  } while (0)
#else
#define HT_CRIT(msg)
#define HT_CRITF(msg, ...)
#endif

#ifndef DISABLE_LOG_ALERT
#define HT_ALERT(msg) if (Logger::logger->isAlertEnabled()) do { \
  Logger::logger->alert("(%s:%d) " msg, __FILE__, __LINE__); \
} while (0)
#define HT_ALERTF(msg, ...) \
  if (Logger::logger->isAlertEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->alert("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->alert(msg, __VA_ARGS__);  \
    std::cout << std::flush; \
  } while (0)
#else
#define HT_ALERT(msg)
#define HT_ALERTF(msg, ...)
#endif

#ifndef DISABLE_LOG_EMERG
#define HT_EMERG(msg) if (Logger::logger->isEmergEnabled()) do { \
  Logger::logger->emerg("(%s:%d) " msg, __FILE__, __LINE__); \
} while (0)
#define HT_EMERGF(msg, ...) \
  if (Logger::logger->isEmergEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->emerg("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->emerg(msg, __VA_ARGS__);  \
    std::cout << std::flush; \
  } while (0)
#else
#define HT_EMERG(msg)
#define HT_EMERGF(msg, ...)
#endif

#ifndef DISABLE_LOG_FATAL
#define HT_FATAL(msg) if (Logger::logger->isFatalEnabled()) do { \
  Logger::logger->fatal("(%s:%d) " msg, __FILE__, __LINE__); \
  abort(); \
} while (0)
#define HT_FATALF(msg, ...) \
  if (Logger::logger->isFatalEnabled()) do { \
    if (Logger::ms_show_line_numbers) \
      Logger::logger->fatal("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__);  \
    else \
      Logger::logger->fatal(msg, __VA_ARGS__);  \
    std::cout << std::flush; \
    abort(); \
  } while (0)
#define HT_EXPECT(_e_, _code_) if (_e_); else \
  HT_FATAL("failed expectation: " #_e_)
#else
#define HT_FATAL(msg)
#define HT_FATALF(msg, ...)
#define HT_EXPECT(_e_, _code_) if (_e_); else throw \
  Exception(_code_, "failed expectation: " #_e_)
#endif

#else // HYPERTABLE_DISABLE_LOGGING

#define HT_DEBUG(msg)
#define HT_DEBUGF(msg, ...) 
#define HT_INFO(msg)
#define HT_INFOF(msg, ...)
#define HT_NOTICE(msg)
#define HT_NOTICEF(msg, ...)
#define HT_WARN(msg)
#define HT_WARNF(msg, ...)
#define HT_ERROR(msg)
#define HT_ERRORF(msg, ...)
#define HT_CRIT(msg)
#define HT_CRITF(msg, ...)
#define HT_ALERT(msg)
#define HT_ALERTF(msg, ...)
#define HT_EMERG(msg)
#define HT_EMERGF(msg, ...)
#define HT_FATAL(msg)
#define HT_FATALF(msg, ...)
#define HT_EXPECT(e)
#define HT_LOG_ENTER
#define HT_LOG_EXIT

#endif // HYPERTABLE_DISABLE_LOGGING


#endif // HYPERTABLE_LOGGER_H
