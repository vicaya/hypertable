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

#ifndef HYPERTABLE_PROTOCOL_H
#define HYPERTABLE_PROTOCOL_H

#include "Event.h"
#include "HeaderBuilder.h"

namespace Hypertable {

  class CommBuf;

  class Protocol {

  public:

    virtual ~Protocol() { return; }

    static int32_t response_code(Event *event);
    static int32_t response_code(EventPtr &event_ptr) {
      return response_code(event_ptr.get());
    }

    static String string_format_message(Event *event);
    static String string_format_message(EventPtr &event_ptr) {
      return string_format_message(event_ptr.get());
    }

    static CommBuf *
    create_error_message(HeaderBuilder &hbuilder, int error, const char *msg);

    virtual const char *command_text(short command) = 0;

  };

}

#endif // HYPERTABLE_PROTOCOL_H
