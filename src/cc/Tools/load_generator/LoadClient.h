/**
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

#ifndef HYPERTABLE_LOAD_GENERATOR_CLIENT
#define HYPERTABLE_LOAD_GENERATOR_CLIENT

#include "Common/Compat.h"
#include "Common/ReferenceCount.h"

#include <iostream>
#include <fstream>
#include <cstdio>
#include <cmath>

extern "C" {
#include <poll.h>
#include <stdio.h>
#include <time.h>
}

#include <boost/algorithm/string.hpp>
#include <boost/shared_array.hpp>

#include "Common/String.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/Config.h"

#ifdef HT_WITH_THRIFT
#include "ThriftBroker/Client.h"
#include "ThriftBroker/Config.h"
#include "ThriftBroker/ThriftHelper.h"
#endif

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;
using namespace boost;

class LoadClient : public ReferenceCount {
  public:
    LoadClient(const String &config_file, bool thrift=false);
    LoadClient(bool thrift=false);
    ~LoadClient();

    void create_mutator(const String &tablename, int mutator_flags);
    /**
     * Create a scanner.
     * For thrift scanners, just use the 1st column and 1st row_interval specified in the
     * ScanSpec.
     */
    void create_scanner(const String &tablename, const ScanSpec& scan_spec);
    void set_cells(const Cells &cells);
    void set_delete(const KeySpec &key);
    void flush();
    void close_scanner();

    /**
     * Get all cells that match the spec in the current scanner
     * return the total number of bytes scanned
     */
    uint64_t get_all_cells();

  private:
    bool m_thrift;
    ClientPtr m_native_client;
    TablePtr m_native_table;
    bool m_native_table_open;
    TableMutatorPtr m_native_mutator;
    TableScannerPtr m_native_scanner;
#ifdef HT_WITH_THRIFT
    shared_ptr<Thrift::Client> m_thrift_client;
    ThriftGen::Mutator m_thrift_mutator;
    ThriftGen::Scanner m_thrift_scanner;
#endif
};

typedef intrusive_ptr<LoadClient> LoadClientPtr;
#endif // LoadClient
