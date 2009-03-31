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

#ifndef HYPERTABLE_DATAGENERATORQUALIFIER_H
#define HYPERTABLE_DATAGENERATORQUALIFIER_H

#include <boost/shared_array.hpp>

#include "Common/Config.h"
#include "Common/Logger.h"
#include "Common/Random.h"
#include "Common/String.h"

#include "Cell.h"
#include "DataGeneratorRowComponent.h"

using namespace Hypertable;

namespace Hypertable {

  class QualifierSpec {
  public:
    QualifierSpec() : type(-1), size(-1) { }
    int type;
    int size;
    String charset;
  };

  class Qualifier : public QualifierSpec {
  public:
    Qualifier(QualifierSpec &spec) : QualifierSpec(spec) { }
    virtual ~Qualifier() { }
    virtual bool next() = 0;
    virtual String &get() = 0;
  };

  class QualifierString : public Qualifier {
  public:
    QualifierString(QualifierSpec &spec) : Qualifier(spec) {
      HT_ASSERT(size > 0);
      m_render_buf.reset( new char [ size + 1 ] );
      ((char *)m_render_buf.get())[ size ] = 0;
    }
    virtual ~QualifierString() { }
    virtual bool next() {
      if (charset.length() > 0)
        Random::fill_buffer_with_random_chars((char *)m_render_buf.get(), size, charset.c_str());
      else
        Random::fill_buffer_with_random_ascii((char *)m_render_buf.get(), size);
      m_qualifier = m_render_buf.get();
      return false;
    }
    virtual String &get() { return m_qualifier; }
  private:
    boost::shared_array<const char> m_render_buf;
    String m_qualifier;
  };

  class QualifierFactory {
  public:
    static Qualifier *create(QualifierSpec &spec) {
      if (spec.type == STRING)
        return new QualifierString(spec);
      else
        HT_ASSERT(!"Invalid qualifier type");
    }
  };

}

#endif // HYPERTABLE_DATAGENERATORQUALIFIER_H
