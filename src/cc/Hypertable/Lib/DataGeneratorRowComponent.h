/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_DATAGENERATORROWCOMPONENT_H
#define HYPERTABLE_DATAGENERATORROWCOMPONENT_H

#include <iostream>
#include <iterator>
#include <sstream>
#include <string>

extern "C" {
#include <limits.h>
#include <stdlib.h>
}

#include "Common/Config.h"
#include "Common/DiscreteRandomGeneratorFactory.h"
#include "Common/String.h"

#include "Cell.h"

using namespace Hypertable::Config;
using namespace std;

namespace Hypertable {

  enum Type { INTEGER, STRING, TIMESTAMP };

  enum Order { RANDOM, ASCENDING };

  class RowComponentSpec {
  public:
    RowComponentSpec() : type(-1), order(-1), seed((unsigned)-1) { }
    int type;
    int order;
    String format;
    String min;
    String max;
    unsigned seed;
    String distribution;
  };

  class RowComponent : public RowComponentSpec {
  public:
    RowComponent(RowComponentSpec &spec) : RowComponentSpec(spec) {
      if (order == RANDOM) {
        m_rng = DiscreteRandomGeneratorFactory::create(spec.distribution);
        m_rng->set_seed(seed);
      }
    }
    virtual ~RowComponent() { }
    virtual bool next() = 0;
    virtual void render(String &dst) = 0;
  protected:
    DiscreteRandomGeneratorPtr m_rng;
  };

  class RowComponentInteger : public RowComponent {
  public:
    RowComponentInteger(RowComponentSpec &spec) : RowComponent(spec) {
      m_min = (min != "") ? strtoll(min.c_str(), 0, 0) : 0;
      if (m_min < 0)
        m_min = 0;
      m_max = (max != "") ? strtoll(max.c_str(), 0, 0) : std::numeric_limits<int64_t>::max();
      HT_ASSERT(m_min < m_max);
      m_next = m_max;
      m_diff = m_max - m_min;
      if (order == RANDOM)
        m_rng->set_max(m_diff);
      if (format.length() == 0)
        m_render_buf = new char [ 32 ];
      else {
        char tbuf[32];
        int total_len = snprintf(tbuf, 32, format.c_str(), (Lld)1);
        m_render_buf = new char [ total_len + 32 ];
      }
    }
    virtual ~RowComponentInteger() { 
      delete [] m_render_buf;
    }
    virtual bool next() {
      bool rval = true;
      if (order == ASCENDING) {
        if (m_next == m_max) {
          m_next = m_min;
          rval = false;
        }
        m_next++;
      }
      else if (order == RANDOM) {
        m_next = m_min + m_rng->get_sample() % m_diff;
        rval = false;
      }
      else
        HT_FATAL("invalid order");
      if (format != "")
        sprintf(m_render_buf, format.c_str(), (Lld)m_next);
      else
        sprintf(m_render_buf, "%lld", (Lld)m_next);
      return rval;
    }
    virtual void render(String &dst) {
      dst += (const char *)m_render_buf;
    }
  private:
    int64_t m_min, m_max, m_next, m_diff;
    char *m_render_buf;
  };

  class RowComponentTimestamp : public RowComponent {
  public:
    RowComponentTimestamp(RowComponentSpec &spec) : RowComponent(spec) {
      struct tm tmval;
      if (format != "") {
        if (min != "") {
          strptime(min.c_str(), format.c_str(), &tmval);
          m_min = mktime(&tmval);
        }
        else
          m_min = 0;
        if (max != "") {
          strptime(max.c_str(), format.c_str(), &tmval);
          m_max = mktime(&tmval);
        }
        else
          m_max = std::numeric_limits<time_t>::max();
      }
      else {
        if (min != "") {
          strptime(min.c_str(), "%F %T", &tmval);
          m_min = mktime(&tmval);
        }
        else
          m_min = 0;
        if (max != "") {
          strptime(max.c_str(), "%F %T", &tmval);
          m_max = mktime(&tmval);
        }
        else
          m_max = std::numeric_limits<time_t>::max();
      }
      HT_ASSERT(m_min < m_max);
      m_next = m_max;
      m_diff = m_max - m_min;
      if (order == RANDOM)
        m_rng->set_max(m_diff);

      m_render_buf_len = 32;
      if (format.length() == 0)
        m_render_buf = new char [ m_render_buf_len ];
      else {
        char tbuf[32];
        int total_len = snprintf(tbuf, 32, format.c_str(), (Lld)1);
        m_render_buf_len = total_len + 32;
        m_render_buf = new char [ m_render_buf_len ];
      }

    }
    virtual ~RowComponentTimestamp() { 
      delete [] m_render_buf;
    }
    virtual bool next() {
      bool rval = true;
      if (order == ASCENDING) {
        if (m_next == m_max) {
          m_next = m_min;
          rval = false;
        }
        m_next++;
      }
      else if (order == RANDOM) {
        m_next = m_min + (time_t)(m_rng->get_sample() % m_diff);
        rval = false;
      }
      else
        HT_FATAL("invalid order");
      struct tm tm_val;
      const char *cformat = (format == "") ? "%F %T" : format.c_str();
      gmtime_r(&m_next, &tm_val);
      strftime(m_render_buf, m_render_buf_len, cformat, &tm_val);
      return rval;
    }
    virtual void render(String &dst) {
      dst += (const char *)m_render_buf;
    }
  private:
    time_t m_min, m_max, m_next;
    int64_t m_diff;
    char *m_render_buf;
    int m_render_buf_len;
  };

}

#endif // HYPERTABLE_DATAGENERATORROWCOMPONENT_H
