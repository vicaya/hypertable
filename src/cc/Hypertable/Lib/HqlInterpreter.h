/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_HQLINTERPRETER_H
#define HYPERTABLE_HQLINTERPRETER_H

#include <vector>

#include "Cells.h"
#include "TableScanner.h"
#include "TableMutator.h"

namespace Hypertable {

  class Client;
  using namespace std;

  namespace Hql { class ParserState; }

  /**
   * The API of HQL interpreter
   */
  class HqlInterpreter : public ReferenceCount {
  public:
    /** Callback interface/base class for execute */
    struct Callback {
      FILE *output;             // default is NULL
      bool normal_mode;         // default true
      bool format_ts_in_usecs;  // default false
      // mutator stats
      uint64_t total_cells,
               total_keys_size,
               total_values_size,
               file_size;

      Callback(bool normal = true) : output(0), normal_mode(normal),
          format_ts_in_usecs(false), total_cells(0), total_keys_size(0),
          total_values_size(0), file_size(0) { }
      virtual ~Callback() { }

      /** Called when the hql string is parsed successfully */
      virtual void on_parsed(Hql::ParserState &) { }

      /** Called when interpreter returns a string result
       * Maybe called multiple times for a list of string results
       */
      virtual void on_return(const String &) { }

      /** Called when interpreter is ready to scan */
      virtual void on_scan(TableScanner &) { }

      /** Called when interpreter is ready to update */
      virtual void on_update(size_t total) { }

      /** Called when interpreter updates progress for long running queries */
      virtual void on_progress(size_t amount) { }

      /** Called when interpreter is finished note, mutator pointer maybe NULL
       * in case of things like load data ... into file
       */
      virtual void
      on_finish(TableMutator *mutator = 0) {
        if (mutator) try {
          mutator->flush();
        }
        catch (Exception &e) {
          do {
            mutator->show_failed(e);
          } while (!mutator->retry());
        }
      }
    };

    /** An example for simple queries that returns small number of results */
    struct SmallCallback : Callback {
      CellsBuilder &cells;
      std::vector<String> &retstrs;

      SmallCallback(CellsBuilder &builder, std::vector<String> &strs)
        : cells(builder), retstrs(strs) { }

      virtual void on_return(const String &ret) { retstrs.push_back(ret); }
      virtual void on_scan(TableScanner &scanner) { copy(scanner, cells); }
    };

    /** Construct from hypertable client */
    HqlInterpreter(Client *client);

    /** The main interface for the interpreter */
    void execute(const String &str, Callback &);

    /** A convenient method demonstrate the usage of the interface */
    void
    execute(const String &str, CellsBuilder &output, std::vector<String> ret) {
      SmallCallback cb(output, ret);
      execute(str, cb);
    }

  private:
    Client *m_client;
    uint32_t m_mutator_flags;

  };

  typedef intrusive_ptr<HqlInterpreter> HqlInterpreterPtr;

} // namespace Hypertable

#endif // HYPERTABLE_HQLINTERPRETER_H
