/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */
#include "Common/Compat.h"
#include "Common/Logger.h"
#include "Common/Mutex.h"

#include <boost/shared_ptr.hpp>

#include <protocol/TBinaryProtocol.h>
#include <server/TNonblockingServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include "Config.h"
#include "ThriftHelper.h"

#include "Common/Time.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlInterpreter.h"
#include "Hypertable/Lib/Config.h"

#define THROW_TE(_code_, _str_) do { ThriftGen::ClientException te; \
  te.code = _code_; te.what = _str_; \
  te.__isset.code = te.__isset.what = true; \
  throw te; \
} while (0)

#define THROW_(_code_) do { \
  Hypertable::Exception e(_code_, __LINE__, HT_FUNC, __FILE__); \
  std::ostringstream oss; oss << e; \
  HT_ERROR_OUT << oss.str() << HT_END; \
  THROW_TE(_code_, oss.str()); \
} while (0)

#define RETHROW() catch (Hypertable::Exception &e) { \
  std::ostringstream oss;  oss << HT_FUNC <<": "<< e; \
  HT_ERROR_OUT << oss.str() << HT_END; \
  THROW_TE(e.code(), oss.str()); \
}

#define LOG_API(_expr_) do { \
  if (m_log_api) \
    std::cout << hires_ts <<" API "<< __func__ <<": "<< _expr_ << std::endl; \
} while (0)

namespace Hypertable { namespace ThriftBroker {

using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;
using namespace facebook::thrift::concurrency;

using namespace Config;
using namespace ThriftGen;
using namespace boost;
using namespace std;

typedef Meta::list<ThriftBrokerPolicy, DefaultCommPolicy> Policies;

typedef hash_map<int64_t, TableScannerPtr> ScannerMap;
typedef hash_map<int64_t, TableMutatorPtr> MutatorMap;
typedef std::vector<ThriftGen::Cell> ThriftCells;

inline void
convert_scan_spec(const ThriftGen::ScanSpec &tss, Hypertable::ScanSpec &hss) {
  if (tss.__isset.row_limit)
    hss.row_limit = tss.row_limit;

  if (tss.__isset.revs)
    hss.max_versions = tss.revs;

  if (tss.__isset.start_time)
    hss.time_interval.first = tss.start_time;

  if (tss.__isset.end_time)
    hss.time_interval.second = tss.end_time;

  if (tss.__isset.return_deletes)
    hss.return_deletes = tss.return_deletes;

  // shallow copy
  foreach(const ThriftGen::RowInterval &ri, tss.row_intervals)
    hss.row_intervals.push_back(Hypertable::RowInterval(ri.start_row.c_str(),
        ri.start_inclusive, ri.end_row.c_str(), ri.end_inclusive));

  foreach(const ThriftGen::CellInterval &ci, tss.cell_intervals)
    hss.cell_intervals.push_back(Hypertable::CellInterval(
        ci.start_row.c_str(), ci.start_column.c_str(), ci.start_inclusive,
        ci.end_row.c_str(), ci.end_column.c_str(), ci.end_inclusive));
}

inline void
convert_cell(const ThriftGen::Cell &tcell, Hypertable::Cell &hcell) {
  // shallow copy
  if (tcell.__isset.row_key)
    hcell.row_key = tcell.row_key.c_str();

  if (tcell.__isset.column_family)
    hcell.column_family = tcell.column_family.c_str();

  if (tcell.__isset.column_qualifier)
    hcell.column_qualifier = tcell.column_qualifier.c_str();

  if (tcell.__isset.timestamp)
    hcell.timestamp = tcell.timestamp;

  if (tcell.__isset.revision)
    hcell.revision = tcell.revision;

  if (tcell.__isset.value) {
    hcell.value = (uint8_t *)tcell.value.c_str();
    hcell.value_len = tcell.value.length();
  }
  if (tcell.__isset.flag)
    hcell.flag = tcell.flag;
}

inline void
convert_cell(const Hypertable::Cell &hcell, ThriftGen::Cell &tcell) {
  tcell.row_key = hcell.row_key;
  tcell.column_family = hcell.column_family;

  if (hcell.column_qualifier && *hcell.column_qualifier) {
    tcell.column_qualifier = hcell.column_qualifier;
    tcell.__isset.column_qualifier = true;
  }
  tcell.timestamp = hcell.timestamp;
  tcell.revision = hcell.revision;

  if (hcell.value && hcell.value_len) {
    tcell.value = std::string((char *)hcell.value, hcell.value_len);
    tcell.__isset.value = true;
  }
  tcell.flag = hcell.flag;
  tcell.__isset.row_key = tcell.__isset.column_family = tcell.__isset.timestamp
      = tcell.__isset.revision = tcell.__isset.flag = true;
}

inline void
convert_cells(const ThriftCells &tcells, Hypertable::Cells &hcells) {
  // shallow copy
  foreach(const ThriftGen::Cell &tcell, tcells) {
    Hypertable::Cell hcell;
    convert_cell(tcell, hcell);
    hcells.push_back(hcell);
  }
}

class ServerHandler;

struct HqlCallback : HqlInterpreter::Callback {
  typedef HqlInterpreter::Callback Parent;

  HqlResult &result;
  ServerHandler *handler;
  bool flush, buffered;

  HqlCallback(HqlResult &res, ServerHandler *handler, bool flush, bool buffered)
    : result(res), handler(handler), flush(flush), buffered(buffered) { }

  virtual void on_return(const String &);
  virtual void on_scan(TableScanner &);
  virtual void on_finish(TableMutator *);
};

class ServerHandler : public HqlServiceIf {
public:
  ServerHandler() {
    m_client = new Hypertable::Client();
    m_log_api = Config::get_bool("ThriftBroker.API.Logging");
    m_next_limit = Config::get_i32("ThriftBroker.NextLimit");
  }

  virtual void
  hql_exec(HqlResult& result, const String &hql, bool noflush,
           bool unbuffered) {
    LOG_API("hql="<< hql <<" noflush="<< noflush <<" unbuffered="<< unbuffered);

    try {
      HqlCallback cb(result, this, !noflush, !unbuffered);
      get_hql_interp().execute(hql, cb);
      LOG_API("result="<< result);
    } RETHROW()
  }

  virtual void create_table(const String &table, const String &schema) {
    LOG_API("table="<< table <<" schema="<< schema);

    try {
      m_client->create_table(table, schema);
      LOG_API("table="<< table <<" done");
    } RETHROW()
  }

  virtual Scanner
  open_scanner(const String &table, const ThriftGen::ScanSpec &ss) {
    LOG_API("table="<< table <<" scan_spec="<< ss);

    try {
      Scanner id = get_scanner_id(_open_scanner(table, ss).get());
      LOG_API("scanner="<< id);
      return id;
    } RETHROW()
  }

  virtual void close_scanner(const Scanner scanner) {
    LOG_API("scanner="<< scanner);

    try {
      remove_scanner(scanner);
      LOG_API("scanner="<< scanner <<" done");
    } RETHROW()
  }

  virtual void next_cells(ThriftCells &result, const Scanner scanner_id) {
    LOG_API("scanner="<< scanner_id);

    try {
      TableScannerPtr scanner = get_scanner(scanner_id);
      _next(result, scanner, m_next_limit);
      LOG_API("scanner="<< scanner_id <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void
  get_row(ThriftCells &result, const String &table, const String &row) {
    LOG_API("table="<< table <<" row="<< row);

    try {
      TablePtr t = m_client->open_table(table);
      Hypertable::ScanSpec ss;
      ss.row_intervals.push_back(Hypertable::RowInterval(row.c_str(), true,
                                                         row.c_str(), true));
      ss.max_versions = 1;
      TableScannerPtr scanner = t->create_scanner(ss);
      _next(result, scanner, INT32_MAX);
      LOG_API("table="<< table <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void
  get_cell(Value &result, const String &table, const String &row,
           const String &column) {
    LOG_API("table="<< table <<" row="<< row <<" column="<< column);

    try {
      TablePtr t = m_client->open_table(table);
      Hypertable::ScanSpec ss;

      ss.cell_intervals.push_back(Hypertable::CellInterval(row.c_str(),
          column.c_str(), true, row.c_str(), column.c_str(), true));
      ss.max_versions = 1;

      Hypertable::Cell cell;
      TableScannerPtr scanner = t->create_scanner(ss);

      if (scanner->next(cell))
        result = String((char *)cell.value, cell.value_len);

      LOG_API("table="<< table <<" result="<< result);
    } RETHROW()
  }

  virtual void
  get_cells(ThriftCells &result, const String &table,
            const ThriftGen::ScanSpec &ss) {
    LOG_API("table="<< table <<" scan_spec="<< ss);

    try {
      TableScannerPtr scanner = _open_scanner(table, ss);
      _next(result, scanner, INT32_MAX);
      LOG_API("table="<< table <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual Mutator open_mutator(const String &table) {
    LOG_API("table="<< table);

    try {
      TablePtr t = m_client->open_table(table);
      Mutator id =  get_mutator_id(t->create_mutator());
      LOG_API("table="<< table <<" mutator="<< id);
      return id;
    } RETHROW()
  }

  virtual void flush_mutator(const Mutator mutator) {
    LOG_API("mutator="<< mutator);

    try {
      get_mutator(mutator)->flush();
      LOG_API("mutator="<< mutator <<" done");
    } RETHROW()
  }

  virtual void close_mutator(const Mutator mutator, const bool flush) {
    LOG_API("mutator="<< mutator <<" flush="<< flush);

    try {
      if (flush)
        flush_mutator(mutator);

      remove_mutator(mutator);
      LOG_API("mutator="<< mutator <<" done");
    } RETHROW()
  }

  virtual void set_cells(const Mutator mutator, const ThriftCells &cells) {
    LOG_API("mutator="<< mutator <<" cell.size="<< cells.size());

    try {
      Hypertable::Cells hcells;
      convert_cells(cells, hcells);
      get_mutator(mutator)->set_cells(hcells);
      LOG_API("mutator="<< mutator <<" done");
    } RETHROW()
  }

  virtual void set_cell(const Mutator mutator, const ThriftGen::Cell &cell) {
    LOG_API("mutator="<< mutator <<" cell="<< cell);

    try {
      CellsBuilder cb;
      Hypertable::Cell hcell;
      convert_cell(cell, hcell);
      cb.add(hcell, false);
      get_mutator(mutator)->set_cells(cb.get());
      LOG_API("mutator="<< mutator <<" done");
    } RETHROW()
  }

  virtual int32_t get_table_id(const String &name) {
    LOG_API("table="<< name);

    try {
      int32_t id = m_client->get_table_id(name);
      LOG_API("table="<< name <<" id="<< id);
      return id;
    } RETHROW()
  }

  virtual void get_schema(String &result, const String &table) {
    LOG_API("table="<< table);

    try {
      result = m_client->get_schema(table);
      LOG_API("table="<< table <<" schema="<< result);
    } RETHROW()
  }

  virtual void get_tables(std::vector<String> & tables) {
    LOG_API("");
    try {
      m_client->get_tables(tables);
      LOG_API("tables.size="<< tables.size());
    }
    RETHROW()
  }

  virtual void drop_table(const String &table, const bool if_exists) {
    LOG_API("table="<< table <<" if_exists="<< if_exists);
    try {
      m_client->drop_table(table, if_exists);
      LOG_API("table="<< table <<" done");
    }
    RETHROW()
  }


  // helper methods

  TableScannerPtr
  _open_scanner(const String &name, const ThriftGen::ScanSpec &ss) {
    TablePtr t = m_client->open_table(name);
    Hypertable::ScanSpec hss;
    convert_scan_spec(ss, hss);
    return t->create_scanner(hss);
  }

  void _next(ThriftCells &result, TableScannerPtr &scanner, int limit) {
    Hypertable::Cell cell;

    for (int i = 0; i < limit; ++i) {
      if (scanner->next(cell)) {
        ThriftGen::Cell tcell;
        convert_cell(cell, tcell);
        result.push_back(tcell);
      }
      else
        break;
    }
  }

  HqlInterpreter &get_hql_interp() {
    ScopedLock lock(m_interp_mutex);

    if (!m_hql_interp)
      m_hql_interp = m_client->create_hql_interpreter();

    return *m_hql_interp;
  }

  int64_t get_scanner_id(TableScanner *scanner) {
    ScopedLock lock(m_scanner_mutex);
    int64_t id = (int64_t)scanner;
    m_scanner_map.insert(make_pair(id, scanner)); // no overwrite
    return id;
  }

  TableScannerPtr get_scanner(int64_t id) {
    ScopedLock lock(m_scanner_mutex);
    ScannerMap::iterator it = m_scanner_map.find(id);

    if (it != m_scanner_map.end())
      return it->second;

    THROW_(Error::THRIFTBROKER_BAD_SCANNER_ID);
  }

  void remove_scanner(int64_t id) {
    ScopedLock lock(m_scanner_mutex);
    ScannerMap::iterator it = m_scanner_map.find(id);

    if (it != m_scanner_map.end()) {
      m_scanner_map.erase(it);
      return;
    }

    THROW_(Error::THRIFTBROKER_BAD_SCANNER_ID);
  }

  int64_t get_mutator_id(TableMutator *mutator) {
    ScopedLock lock(m_mutator_mutex);
    int64_t id = (int64_t)mutator;
    m_mutator_map.insert(make_pair(id, mutator)); // no overwrite
    return id;
  }

  TableMutatorPtr get_mutator(int64_t id) {
    ScopedLock lock(m_mutator_mutex);
    MutatorMap::iterator it = m_mutator_map.find(id);

    if (it != m_mutator_map.end())
      return it->second;

    THROW_(Error::THRIFTBROKER_BAD_MUTATOR_ID);
  }

  void remove_mutator(int64_t id) {
    ScopedLock lock(m_mutator_mutex);
    MutatorMap::iterator it = m_mutator_map.find(id);

    if (it != m_mutator_map.end()) {
      m_mutator_map.erase(it);
      return;
    }

    THROW_(Error::THRIFTBROKER_BAD_MUTATOR_ID);
  }

private:
  bool       m_log_api;
  Mutex      m_scanner_mutex;
  ScannerMap m_scanner_map;
  Mutex      m_mutator_mutex;
  MutatorMap m_mutator_map;
  int32_t    m_next_limit;
  ClientPtr  m_client;
  Mutex      m_interp_mutex;
  HqlInterpreterPtr m_hql_interp;
};

void HqlCallback::on_return(const String &ret) {
  result.results.push_back(ret);
  result.__isset.results = true;
}

void HqlCallback::on_scan(TableScanner &s) {
  if (buffered) {
    Hypertable::Cell hcell;
    ThriftGen::Cell tcell;

    while (s.next(hcell)) {
      convert_cell(hcell, tcell);
      result.cells.push_back(tcell);
    }
    result.__isset.cells = true;
  }
  else {
    result.scanner = handler->get_scanner_id(&s);
    result.__isset.scanner = true;
  }
}

void HqlCallback::on_finish(TableMutator *m) {
  if (flush) {
    Parent::on_finish(m);
  }
  else if (m) {
    result.mutator = handler->get_mutator_id(m);
    result.__isset.mutator = true;
  }
}

}} // namespace Hypertable::ThriftBroker


int main(int argc, char **argv) {
  using namespace Hypertable;
  using namespace ThriftBroker;

  try {
    init_with_policies<Policies>(argc, argv);

    uint16_t port = get_i16("port");
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<ServerHandler> handler(new ServerHandler());
    shared_ptr<TProcessor> processor(new HqlServiceProcessor(handler));
    TNonblockingServer server(processor, protocolFactory, port);

    HT_INFO("Starting the server...");
    server.serve();
    HT_INFO("Exiting.\n");
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  return 0;
}
