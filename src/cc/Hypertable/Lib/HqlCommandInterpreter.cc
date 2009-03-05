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

#include "Common/Compat.h"
#include "Schema.h"

#include <cassert>
#include <cstdio>
#include <cstring>

#include <boost/progress.hpp>
#include <boost/timer.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <time.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Stopwatch.h"

#include "Client.h"
#include "HqlCommandInterpreter.h"
#include "HqlHelpText.h"
#include "HqlParser.h"
#include "Key.h"
#include "LoadDataSource.h"

using namespace std;
using namespace Hypertable;
using namespace Hql;

namespace {

  struct CommandCallback : HqlInterpreter::Callback {
    CommandInterpreter &commander;
    int command;
    boost::progress_display *progress;
    Stopwatch stopwatch;

    CommandCallback(CommandInterpreter &interp)
      : HqlInterpreter::Callback(interp.normal_mode()), commander(interp),
        command(0), progress(0) {
      format_ts_in_usecs = interp.timestamp_output_format()
          == CommandInterpreter::TIMESTAMP_FORMAT_USECS;
      output = stdout;
    }

    ~CommandCallback() {
      if (progress)
        delete progress;

      if (output == stdout || output == stderr)
        fflush(output);
      else if (output)
        fclose(output);
    }

    virtual void on_parsed(ParserState &state) { command = state.command; }

    virtual void on_return(const String &str) { cout << str << endl; }

    virtual void on_update(size_t total) {
      if (!normal_mode)
        return;

      HT_ASSERT(command);

      if (command == COMMAND_LOAD_DATA) {
        HT_ASSERT(!progress);
        cout <<"\nLoading "<< format_number(total)
             <<" bytes of input data..." << endl;
        progress = new boost::progress_display(total);
      }
    }

    virtual void on_progress(size_t amount) {
      HT_ASSERT(progress);
      *progress += amount;
    }

    virtual void on_finish(TableMutator *mutator) {
      Callback::on_finish(mutator);
      stopwatch.stop();

      if (normal_mode) {
        if (progress && progress->count() < file_size)
          *progress += file_size - progress->count();

        double elapsed = stopwatch.elapsed();

        if (command == COMMAND_LOAD_DATA)
          fprintf(stderr, "Load complete.\n");

        fputc('\n', stderr);
        fprintf(stderr, "  Elapsed time:  %.2f s\n", elapsed);

        if (total_values_size)
          fprintf(stderr, "Avg value size:  %.2f bytes\n",
                 (double)total_values_size / total_cells);

        if (total_keys_size)
          fprintf(stderr, "  Avg key size:  %.2f bytes\n",
                 (double)total_keys_size / total_cells);

        if (total_keys_size && total_values_size) {
          fprintf(stderr, "    Throughput:  %.2f bytes/s",
                 (total_keys_size + total_values_size) / elapsed);

          if (file_size)
            fprintf(stderr, " (%.2f bytes/s)", file_size / elapsed);

          fputc('\n', stderr);
        }
        if (total_cells) {
          fprintf(stderr, "   Total cells:  %llu\n", (Llu)total_cells);
          fprintf(stderr, "    Throughput:  %.2f cells/s\n",
                 total_cells / elapsed);
        }
        if (mutator)
          fprintf(stderr, "       Resends:  %llu\n",
                  (Llu)mutator->get_resend_count());

        fflush(stderr);
      }
    }
  };

} // local namespace


HqlCommandInterpreter::HqlCommandInterpreter(Client *client)
    : m_interp(new HqlInterpreter(client)) {
}


HqlCommandInterpreter::HqlCommandInterpreter(HqlInterpreter *interp)
    : m_interp(interp) {
}


void HqlCommandInterpreter::execute_line(const String &line) {
  CommandCallback cb(*this);
  m_interp->execute(line, cb);
}

// Q.E.D.
