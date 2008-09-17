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
  using log4cpp::Priority;

  void initialize(const String &name, int level = Priority::DEBUG,
                  bool flush_per_log = true, std::ostream &out = std::cout);
  void set_level(int level);
  void set_test_mode(const String &name);
  void suppress_line_numbers();
  void flush();
  bool set_flush_per_log(bool);

  extern log4cpp::Category *logger;
  extern bool show_line_numbers;

}} // namespace Hypertable::Logger


#define HT_LOG_BUFSZ 1024

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
      Logger::logger->log(log4cpp::Priority::_cat_, Hypertable::format( \
          "(%s:%d) %s", __FILE__, __LINE__, msg)); \
    else \
      Logger::logger->log(log4cpp::Priority::_cat_, msg); \
  } \
} while (0)

#define HT_LOGF(_enabled_, _cat_, fmt, ...) do { \
  if (Logger::logger->_enabled_()) { \
    if (Logger::show_line_numbers) \
      Logger::logger->log(log4cpp::Priority::_cat_, Hypertable::format( \
          "(%s:%d) " fmt, __FILE__, __LINE__, __VA_ARGS__)); \
    else \
      Logger::logger->log(log4cpp::Priority::_cat_, Hypertable::format( \
          fmt, __VA_ARGS__));  \
  } \
} while (0)

// stream interface macro helpers
#define HT_LOG_BUF_SIZE 4096

#define HT_OUT(_enabled_, _l_) do { if (Logger::logger->_enabled_()) { \
  char logbuf[HT_LOG_BUF_SIZE]; \
  log4cpp::Priority::PriorityLevel _level_ = log4cpp::Priority::_l_; \
  FixedOstream _out_(logbuf, sizeof(logbuf)); \
  if (Logger::show_line_numbers) \
    _out_ <<"("<< __FILE__ <<':'<< __LINE__ <<") "; \
  _out_

#define HT_OUT2(_enabled_, _l_) do { if (Logger::logger->_enabled_()) { \
  char logbuf[HT_LOG_BUF_SIZE]; \
  log4cpp::Priority::PriorityLevel _level_ = log4cpp::Priority::_l_; \
  FixedOstream _out_(logbuf, sizeof(logbuf)); \
  _out_ << __func__; \
  if (Logger::show_line_numbers) \
    _out_ << " ("<< __FILE__ <<':'<< __LINE__ <<")"; \
  _out_ <<": "

#define HT_END ""; Logger::logger->log(_level_, _out_.str()); \
  if (_level_ == log4cpp::Priority::FATAL) HT_ABORT; \
} /* if enabled */ } while (0)

#define HT_OUT_DISABLED do { if (0) {

// helpers for printing a first n bytes
#define HT_HEAD_(_x_, _l_, _n_) \
  String((char *)(_x_), 0, (_n_) < (_l_) ? (_n_) : (_l_))

#define HT_HEAD(_x_, _n_) \
  String(_x_, 0, (_n_) < (_x_).length() ? (_n_) : (_x_).length())


// Logging macros interface starts here
#ifndef HT_DISABLE_LOG_ALL

#ifndef HT_DISABLE_LOG_DEBUG

#define HT_LOG_ENTER do { \
  if (Logger::logger->isDebugEnabled()) {\
    if (Logger::show_line_numbers) \
      Logger::logger->debug("(%s:%d) %s() ENTER", __FILE__, __LINE__, HT_FUNC);\
    else \
      Logger::logger->debug("%s() ENTER", HT_FUNC); \
  } \
} while(0)

#define HT_LOG_EXIT do { \
  if (Logger::logger->isDebugEnabled()) { \
    if (Logger::show_line_numbers) \
      Logger::logger->debug("(%s:%d) %s() EXIT", __FILE__, __LINE__, HT_FUNC); \
    else \
      Logger::logger->debug("%s() EXIT", HT_FUNC); \
  } \
} while(0)

#define HT_DEBUG(msg) HT_LOG(isDebugEnabled, DEBUG, msg)
#define HT_DEBUGF(msg, ...) HT_LOGF(isDebugEnabled, DEBUG, msg, __VA_ARGS__)
#define HT_DEBUG_OUT HT_OUT2(isDebugEnabled, DEBUG)
#else
#define HT_LOG_ENTER
#define HT_LOG_EXIT
#define HT_DEBUG(msg)
#define HT_DEBUGF(msg, ...)
#define HT_DEBUG_OUT HT_OUT_DISABLED
#endif

#ifndef HT_DISABLE_LOG_INFO
#define HT_INFO(msg) HT_LOG(isInfoEnabled, INFO, msg)
#define HT_INFOF(msg, ...) HT_LOGF(isInfoEnabled, INFO, msg, __VA_ARGS__)
#define HT_INFO_OUT HT_OUT(isInfoEnabled, INFO)
#else
#define HT_INFO(msg)
#define HT_INFOF(msg, ...)
#define HT_INFO_OUT HT_OUT_DISABLED
#endif

#ifndef HT_DISABLE_LOG_NOTICE
#define HT_NOTICE(msg) HT_LOG(isNoticeEnabled, NOTICE, msg)
#define HT_NOTICEF(msg, ...) HT_LOGF(isNoticeEnabled, NOTICE, msg, __VA_ARGS__)
#define HT_NOTICE_OUT HT_OUT(isNoticeEnabled, NOTICE)
#else
#define HT_NOTICE(msg)
#define HT_NOTICEF(msg, ...)
#define HT_NOTICE_OUT HT_OUT_DISABLED
#endif

#ifndef HT_DISABLE_LOG_WARN
#define HT_WARN(msg) HT_LOG(isWarnEnabled, WARN, msg)
#define HT_WARNF(msg, ...) HT_LOGF(isWarnEnabled, WARN, msg, __VA_ARGS__)
#define HT_WARN_OUT HT_OUT2(isWarnEnabled, WARN)
#else
#define HT_WARN(msg)
#define HT_WARNF(msg, ...)
#define HT_WARN_OUT HT_OUT_DISABLED
#endif

#ifndef HT_DISABLE_LOG_ERROR
#define HT_ERROR(msg) HT_LOG(isErrorEnabled, ERROR, msg)
#define HT_ERRORF(msg, ...) HT_LOGF(isErrorEnabled, ERROR, msg, __VA_ARGS__)
#define HT_ERROR_OUT HT_OUT2(isErrorEnabled, ERROR)
#else
#define HT_ERROR(msg)
#define HT_ERRORF(msg, ...)
#define HT_ERROR_OUT HT_OUT_DISABLED
#endif

#ifndef HT_DISABLE_LOG_CRIT
#define HT_CRIT(msg) HT_LOG(isCritEnabled, CRIT, msg)
#define HT_CRITF(msg, ...) HT_LOGF(isCritEnabled, CRIT, msg, __VA_ARGS__)
#define HT_CRIT_OUT HT_OUT2(isCritEnabled, CRIT)
#else
#define HT_CRIT(msg)
#define HT_CRITF(msg, ...)
#define HT_CRIT_OUT HT_OUT_DISABLED
#endif

#ifndef HT_DISABLE_LOG_ALERT
#define HT_ALERT(msg) HT_LOG(isAlertEnabled, ALERT, msg)
#define HT_ALERTF(msg, ...) HT_LOGF(isAlertEnabled, ALERT, msg, __VA_ARGS__)
#define HT_ALERT_OUT HT_OUT2(isAlertEnabled, ALERT)
#else
#define HT_ALERT(msg)
#define HT_ALERTF(msg, ...)
#define HT_ALERT_OUT HT_OUT_DISABLED
#endif

#ifndef HT_DISABLE_LOG_EMERG
#define HT_EMERG(msg) HT_LOG(isEmergEnabled, EMERG, msg)
#define HT_EMERGF(msg, ...) HT_LOGF(isEmergEnabled, EMERG, msg, __VA_ARGS__)
#define HT_EMERG_OUT HT_OUT2(isEmergEnabled, EMERG)
#else
#define HT_EMERG(msg)
#define HT_EMERGF(msg, ...)
#define HT_EMERG_OUT HT_OUT_DISABLED
#endif

#ifndef HT_DISABLE_LOG_FATAL
#define HT_FATAL(msg) do { \
  HT_LOG(isFatalEnabled, FATAL, msg); \
  HT_ABORT; \
} while (0)
#define HT_FATALF(msg, ...) do { \
  HT_LOGF(isFatalEnabled, FATAL, msg, __VA_ARGS__); \
  HT_ABORT; \
} while (0)
#define HT_FATAL_OUT HT_OUT2(isFatalEnabled, FATAL)
#define HT_EXPECT(_e_, _code_) do { if (_e_); else { \
    if (_code_ == Error::FAILED_EXPECTATION) \
      HT_FATAL("failed expectation: " #_e_); \
    HT_THROW(_code_, "failed expectation: " #_e_); } \
} while (0)
#else
#define HT_FATAL(msg)
#define HT_FATALF(msg, ...)
#define HT_FATAL_OUT HT_OUT_DISABLED
#define HT_EXPECT(_e_, _code_) do { if (_e_); else \
  HT_THROW(_code_, "failed expectation: " #_e_); \
} while (0)
#endif

#else // HT_DISABLE_LOGGING

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
#define HT_INFO_OUT HT_OUT_DISABLED
#define HT_NOTICE_OUT HT_OUT_DISABLED
#define HT_WARN_OUT HT_OUT_DISABLED
#define HT_ERROR_OUT HT_OUT_DISABLED
#define HT_CRIT_OUT HT_OUT_DISABLED
#define HT_ALERT_OUT HT_OUT_DISABLED
#define HT_EMERG_OUT HT_OUT_DISABLED
#define HT_FATAL_OUT HT_OUT_DISABLED

#endif // HT_DISABLE_LOGGING

#endif // HYPERTABLE_LOGGER_H
