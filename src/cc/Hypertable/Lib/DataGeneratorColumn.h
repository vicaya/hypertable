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

#ifndef HYPERTABLE_DATAGENERATORCOLUMN_H
#define HYPERTABLE_DATAGENERATORCOLUMN_H

#include <iostream>
#include <iterator>
#include <sstream>
#include <string>

extern "C" {
#include <limits.h>
#include <stdlib.h>
}

#include "Common/Config.h"
#include "Common/FileUtils.h"
#include "Common/Random.h"
#include "Common/String.h"

#include "Cell.h"
#include "DataGeneratorRowComponent.h"
#include "DataGeneratorQualifier.h"

using namespace Hypertable::Config;
using namespace std;

namespace Hypertable {

  class ColumnSpec {
  public:
    ColumnSpec() : size(-1) { }
    QualifierSpec qualifier;
    int size;
    String source;
    String column_family;
  };

  class Column : public ColumnSpec {
  public:
    Column(ColumnSpec &spec) : ColumnSpec(spec) {
      m_qualifiers.push_back( QualifierFactory::create(spec.qualifier) );
      m_next_qualifier = m_qualifiers.size();
    }
    virtual ~Column() { }
    virtual bool next() = 0;
    virtual String &qualifier() = 0;
    virtual String &value() = 0;
  protected:
    std::vector<Qualifier *> m_qualifiers;
    size_t m_next_qualifier;
  };

  class ColumnString : public Column {
  public:
    ColumnString(ColumnSpec &spec) : Column(spec) {
      m_source.reset( FileUtils::file_to_buffer(source, &m_source_len) );
      HT_ASSERT(m_source_len >= size);
      m_source_len -= size;
      m_render_buf.reset( new char [ size * 2 ] + 1 );
    }
    virtual ~ColumnString() { }
    virtual bool next() {
      m_next_qualifier = (m_next_qualifier + 1) % m_qualifiers.size();
      if (m_next_qualifier == 0) {
        off_t offset = Random::number32() % m_source_len;
        const char *src = m_source.get() + offset;
        char *dst = m_render_buf.get();
        for (size_t i=0; i<(size_t)size; i++) {
          if (*src == '\n') {
            *dst++ = '\\';
            *dst++ = 'n';
          }
          else if (*src == '\t') {
            *dst++ = '\\';
            *dst++ = 't';
          }
          else
            *dst++ = *src;
          src++;
        }
        *dst = 0;
        m_value = m_render_buf.get();
      }
      m_qualifiers[m_next_qualifier]->next();
      if (m_next_qualifier == (m_qualifiers.size()-1))
        return false;
      return true;
    }
    virtual String &qualifier() {
      return m_qualifiers[m_next_qualifier]->get();
    }
    virtual String &value() {
      return m_value;
    }
  private:
    String m_value;
    boost::shared_array<char> m_render_buf;
    boost::shared_array<const char> m_source;
    off_t m_source_len;
  };

}

#endif // HYPERTABLE_DATAGENERATORCOLUMN_H
