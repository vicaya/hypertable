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

#ifndef HYPERTABLE_HQLCOMMANDINTERPRETER_H
#define HYPERTABLE_HQLCOMMANDINTERPRETER_H

#include "Common/ReferenceCount.h"

namespace Hypertable {

  class Client;

  class HqlCommandInterpreter : public ReferenceCount {
  public:
    HqlCommandInterpreter(Client *client);

    void execute_line(std::string &line);

    void set_timestamp_output_format(std::string format);

  private:
    Client *m_client;
    int m_timestamp_output_format;

    enum { TIMESTAMP_FORMAT_DEFAULT, TIMESTAMP_FORMAT_USECS };

  };
  typedef boost::intrusive_ptr<HqlCommandInterpreter> HqlCommandInterpreterPtr;

}

#endif // HYPERTABLE_HQLCOMMANDINTERPRETER_H
