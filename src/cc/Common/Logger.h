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
#ifndef HYPERTABLE_LOGGER_H
#define HYPERTABLE_LOGGER_H

#include "Error.h"
#include "String.h"
#include <iostream>
#include "FixedStream.h"
#include <log4cpp/Category.hh>

namespace Hypertable { namespace Logger { 

  void initialize(const String &name, int level = log4cpp::Priority::DEBUG,
                  bool flush_per_log = true, std::ostream &out = std::cout);
  void set_level(int level);
  void set_test_mode(const String &name);
  void suppress_line_numbers();
  void flush();
  bool set_flush_per_log(bool);

  extern log4cpp::Category *logger;
  extern bool show_line_numbers;

}} // namespace Hypertable::Logger


// This should generate a core dump
#ifdef HT_USE_ABORT
#define HT_ABORT abort()
#else
#define HT_ABORT *((int *)0) = 1
#endif

// printf interface macro helper
#define HT_LOG(_enabled_, _cat_, msg) do { \
  if (Logger::logger->_enabled_()) { \
    if (Logger::show_line_numbers) \
      Logger::logger->_cat_("(%s:%d) %s", __FILE__, __LINE__, msg); \
    else \
      Logger::logger->_cat_("%s", msg); \
  } \
} while (0)

#define HT_LOGF(_enabled_, _cat_, msg, ...) do { \
  if (Logger::logger->_enabled_()) { \
    if (Logger::show_line_numbers) \
      Logger::logger->_cat_("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__); \
    else \
      Logger::logger->_cat_(msg, __VA_ARGS__);  \
  } \
} while (0)

// stream interface macro helpers
#define HT_OUT(_enabled_) do { if (Logger::logger->_enabled_()) { \
  char logbuf[1024]; \
  FixedOstream out(logbuf, sizeof(logbuf)); \
  if (Logger::show_line_numbers) \
    out <<"("<< __FILE__ <<':'<< __LINE__ <<") "; \
  out

#define HT_OUT2(_enabled_) do { if (Logger::logger->_enabled_()) { \
  char logbuf[1024]; \
  FixedOstream out(logbuf, sizeof(logbuf)); \
  out << __func__; \
  if (Logger::show_line_numbers) \
    out << " ("<< __FILE__ <<':'<< __LINE__ <<")"; \
  out <<": "

#define HT_END(_cat_) ""; Logger::logger->_cat_(out.str()); \
} /* if enabled */ } while (0)

#define HT_OUT_DISABLED do { if (0) {
#define HT_END_DISABLED }} while (0)

// Logging macros interface starts here
#ifndef DISABLE_LOG_ALL

#ifndef DISABLE_LOG_DEBUG

#define HT_LOG_ENTER do { \
  if (Logger::logger->isDebugEnabled()) {\
    if (Logger::show_line_numbers) \
      Logger::logger->debug("(%s:%d) %s() ENTER", __FILE__, __LINE__, __func__); \
    else \
      Logger::logger->debug("%s() ENTER", __func__); \
  } \
} while(0)

#define HT_LOG_EXIT do { \
  if (Logger::logger->isDebugEnabled()) { \
    if (Logger::show_line_numbers) \
      Logger::logger->debug("(%s:%d) %s() EXIT", __FILE__, __LINE__, __func__); \
    else \
      Logger::logger->debug("%s() EXIT", __func__); \
  } \
} while(0)

#define HT_TRACE

#define HT_DEBUG(msg) HT_LOG(isDebugEnabled, debug, msg)
#define HT_DEBUGF(msg, ...) HT_LOGF(isDebugEnabled, debug, msg, __VA_ARGS__)
#define HT_DEBUG_OUT HT_OUT2(isDebugEnabled)
#define HT_DEBUG_END HT_END(debug)
#else
#define HT_LOG_ENTER
#define HT_LOG_EXIT
#define HT_DEBUG(msg)
#define HT_DEBUGF(msg, ...)
#define HT_DEBUG_OUT HT_OUT_DISABLED
#define HT_DEBUG_END HT_END_DISABLED
#endif

#ifndef DISABLE_LOG_INFO
#define HT_INFO(msg) HT_LOG(isInfoEnabled, info, msg)
#define HT_INFOF(msg, ...) HT_LOGF(isInfoEnabled, info, msg, __VA_ARGS__)
#define HT_INFO_OUT HT_OUT(isInfoEnabled)
#define HT_INFO_END HT_END(info)
#else
#define HT_INFO(msg)
#define HT_INFOF(msg, ...)
#define HT_INFO_OUT HT_OUT_DISABLED
#define HT_INFO_END HT_END_DISABLED
#endif

#ifndef DISABLE_LOG_NOTICE
#define HT_NOTICE(msg) HT_LOG(isNoticeEnabled, notice, msg)
#define HT_NOTICEF(msg, ...) HT_LOGF(isNoticeEnabled, notice, msg, __VA_ARGS__)
#define HT_NOTICE_OUT HT_OUT(isNoticeEnabled)
#define HT_NOTICE_END HT_END(notice)
#else
#define HT_NOTICE(msg)
#define HT_NOTICEF(msg, ...)
#define HT_NOTICE_OUT HT_OUT_DISABLED
#define HT_NOTICE_END HT_END_DISABLED
#endif

#ifndef DISABLE_LOG_WARN
#define HT_WARN(msg) HT_LOG(isWarnEnabled, warn, msg)
#define HT_WARNF(msg, ...) HT_LOGF(isWarnEnabled, warn, msg, __VA_ARGS__)
#define HT_WARN_OUT HT_OUT2(isWarnEnabled)
#define HT_WARN_END HT_END(warn)
#else
#define HT_WARN(msg)
#define HT_WARNF(msg, ...)
#define HT_WARN_OUT HT_OUT_DISABLED
#define HT_WARN_END HT_END_DISABLED
#endif

#ifndef DISABLE_LOG_ERROR
#define HT_ERROR(msg) HT_LOG(isErrorEnabled, error, msg)
#define HT_ERRORF(msg, ...) HT_LOGF(isErrorEnabled, error, msg, __VA_ARGS__)
#define HT_ERROR_OUT HT_OUT2(isErrorEnabled)
#define HT_ERROR_END HT_END(error)
#else
#define HT_ERROR(msg)
#define HT_ERRORF(msg, ...)
#define HT_ERROR_OUT HT_OUT_DISABLED
#define HT_ERROR_END HT_END_DISABLED
#endif

#ifndef DISABLE_LOG_CRIT
#define HT_CRIT(msg) HT_LOG(isCritEnabled, crit, msg)
#define HT_CRITF(msg, ...) HT_LOGF(isCritEnabled, crit, msg, __VA_ARGS__)
#define HT_CRIT_OUT HT_OUT2(isCritEnabled)
#define HT_CRIT_END HT_END(crit)
#else
#define HT_CRIT(msg)
#define HT_CRITF(msg, ...)
#define HT_CRIT_OUT HT_OUT_DISABLED
#define HT_CRIT_END HT_END_DISABLED
#endif

#ifndef DISABLE_LOG_ALERT
#define HT_ALERT(msg) HT_LOG(isAlertEnabled, alert, msg)
#define HT_ALERTF(msg, ...) HT_LOGF(isAlertEnabled, alert, msg, __VA_ARGS__)
#define HT_ALERT_OUT HT_OUT2(isAlertEnabled)
#define HT_ALERT_END HT_END(alert)
#else
#define HT_ALERT(msg)
#define HT_ALERTF(msg, ...)
#define HT_ALERT_OUT HT_OUT_DISABLED
#define HT_ALERT_END HT_END_DISABLED
#endif

#ifndef DISABLE_LOG_EMERG
#define HT_EMERG(msg) HT_LOG(isEmergEnabled, emerg, msg)
#define HT_EMERGF(msg, ...) HT_LOGF(isEmergEnabled, emerg, msg, __VA_ARGS__)
#define HT_EMERG_OUT HT_OUT2(isEmergEnabled)
#define HT_EMERG_END HT_END(emerg)
#else
#define HT_EMERG(msg)
#define HT_EMERGF(msg, ...)
#define HT_EMERG_OUT HT_OUT_DISABLED
#define HT_EMERG_END HT_END_DISABLED
#endif

#ifndef DISABLE_LOG_FATAL
#define HT_FATAL(msg) do { \
  if (Logger::logger->isFatalEnabled()) { \
    if (Logger::show_line_numbers) \
      Logger::logger->fatal("(%s:%d) %s", __FILE__, __LINE__, msg); \
    else \
      Logger::logger->fatal("%s", msg); \
    HT_ABORT; \
  } \
} while (0)
#define HT_FATALF(msg, ...) do { \
  if (Logger::logger->isFatalEnabled()) { \
    if (Logger::show_line_numbers) \
      Logger::logger->fatal("(%s:%d) " msg, __FILE__, __LINE__, __VA_ARGS__); \
    else \
      Logger::logger->fatal(msg, __VA_ARGS__);  \
    HT_ABORT; \
  } \
} while (0)
#define HT_FATAL_OUT HT_OUT2(isFatalEnabled)
#define HT_FATAL_END ""; Logger::logger->fatal(out.str()); HT_ABORT; \
} /* if enabled */ } while (0)
#define HT_EXPECT(_e_, _code_) do { if (_e_); else { \
    if (_code_ == Error::FAILED_EXPECTATION) \
      HT_FATAL("failed expectation: " #_e_); \
    HT_THROW(_code_, "failed expectation: " #_e_); } \
} while (0)
#else
#define HT_FATAL(msg)
#define HT_FATALF(msg, ...)
#define HT_FATAL_OUT HT_OUT_DISABLED
#define HT_FATAL_END HT_END_DISABLED
#define HT_EXPECT(_e_, _code_) do { if (_e_); else throw \
  Exception(_code_, "failed expectation: " #_e_); \
} while (0)
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
#define HT_LOG_ENTER
#define HT_LOG_EXIT
#define HT_DEBUG_OUT HT_OUT_DISABLED
#define HT_DEBUG_END HT_END_DISABLED
#define HT_INFO_OUT HT_OUT_DISABLED
#define HT_INFO_END HT_END_DISABLED
#define HT_NOTICE_OUT HT_OUT_DISABLED
#define HT_NOTICE_END HT_END_DISABLED
#define HT_WARN_OUT HT_OUT_DISABLED
#define HT_WARN_END HT_END_DISABLED
#define HT_ERROR_OUT HT_OUT_DISABLED
#define HT_ERROR_END HT_END_DISABLED
#define HT_CRIT_OUT HT_OUT_DISABLED
#define HT_CRIT_END HT_END_DISABLED
#define HT_ALERT_OUT HT_OUT_DISABLED
#define HT_ALERT_END HT_END_DISABLED
#define HT_EMERG_OUT HT_OUT_DISABLED
#define HT_EMERG_END HT_END_DISABLED
#define HT_FATAL_OUT HT_OUT_DISABLED
#define HT_FATAL_END HT_END_DISABLED

#endif // HYPERTABLE_DISABLE_LOGGING

#endif // HYPERTABLE_LOGGER_H
