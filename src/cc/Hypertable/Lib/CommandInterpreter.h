/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

namespace Hypertable {

  class CommandInterpreter : public ReferenceCount {
  public:
    CommandInterpreter();
    virtual void execute_line(std::string &line) = 0;
    void set_timestamp_output_format(std::string format);

  protected:
    int m_timestamp_output_format;

    enum { TIMESTAMP_FORMAT_DEFAULT, TIMESTAMP_FORMAT_USECS };

  };
  typedef boost::intrusive_ptr<CommandInterpreter> CommandInterpreterPtr;

}

#endif // HYPERTABLE_COMMANDINTERPRETER_H
