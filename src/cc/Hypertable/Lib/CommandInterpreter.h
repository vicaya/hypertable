/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_COMMANDINTERPRETER_H
#define HYPERTABLE_COMMANDINTERPRETER_H

#include "Common/ReferenceCount.h"
#include "Common/String.h"

namespace Hypertable {

  class CommandInterpreter : public ReferenceCount {
  public:
    enum { TIMESTAMP_FORMAT_DEFAULT, TIMESTAMP_FORMAT_USECS };

    CommandInterpreter();
    virtual void execute_line(const String &line) = 0;
    void set_timestamp_output_format(const String &format);
    void set_silent(bool silent) { m_silent = silent; }
    void set_test_mode(bool mode) { m_test_mode = mode; }

    bool silent_mode() { return m_silent; }
    bool test_mode() { return m_test_mode; }
    bool normal_mode() { return !(m_silent || m_test_mode); }
    int timestamp_output_format() { return m_timestamp_output_format; }

  protected:
    int m_timestamp_output_format;
    bool m_silent;
    bool m_test_mode;
  };

  typedef intrusive_ptr<CommandInterpreter> CommandInterpreterPtr;

} // namespace Hypertable

#endif // HYPERTABLE_COMMANDINTERPRETER_H
