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

#ifndef HYPERTABLE_COMMAND_SHELL_H
#define HYPERTABLE_COMMAND_SHELL_H

#include "Common/ReferenceCount.h"
#include "Common/Properties.h"

#include "CommandInterpreter.h"
#include "Notifier.h"

namespace Hypertable {

  class CommandShell : public ReferenceCount {
  public:
    CommandShell(const String &program_name, CommandInterpreterPtr &,
                 PropertiesPtr &);
    int run();

    bool silent() { return m_silent; }
    bool test_mode() { return m_test_mode; }

    static void add_options(PropertiesDesc &);

    static String ms_history_file;

  private:
    char *rl_gets ();

    String m_program_name;
    CommandInterpreterPtr m_interp_ptr;
    PropertiesPtr m_props;
    NotifierPtr m_notifier_ptr;

    String m_accum;
    bool m_batch_mode;
    bool m_silent;
    bool m_test_mode;
    bool m_no_prompt;
    bool m_cont;
    char *m_line_read;
    bool m_notify;
    String m_input_str;
    String m_prompt_str;
  };

  typedef intrusive_ptr<CommandShell> CommandShellPtr;

} // namespace Hypertable

#endif // HYPERTABLE_COMMAND_SHELL_H

