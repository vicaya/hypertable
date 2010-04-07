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
#include "Common/Init.h"
#include "Common/Logger.h"
#include "Common/Mutex.h"

#include <boost/shared_ptr.hpp>

#include <protocol/TBinaryProtocol.h>
#include <server/TNonblockingServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include "Common/Time.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlInterpreter.h"
#include "Hypertable/Lib/Key.h"

#include "Config.h"
#include "ThriftHelper.h"

#define THROW_TE(_code_, _str_) do { ThriftGen::ClientException te; \
  te.code = _code_; te.message = _str_; \
  te.__isset.code = te.__isset.message = true; \
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

#define LOG_HQL_RESULT(_res_) do { \
  if (m_log_api) \
    cout << hires_ts <<" API "<< __func__ <<": result: "; \
  if (Logger::logger->isDebugEnabled()) \
    cout << _res_; \
  else { \
    if (_res_.__isset.results) \
      cout <<"results.size=" << _res_.results.size(); \
    if (_res_.__isset.cells) \
      cout <<"cells.size=" << _res_.cells.size(); \
    if (_res_.__isset.scanner) \
      cout <<"scanner="<< _res_.scanner; \
    if (_res_.__isset.mutator) \
      cout <<"mutator="<< _res_.mutator; \
  } \
  cout << std::endl; \
} while(0)

namespace Hypertable { namespace ThriftBroker {

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;

using namespace Config;
using namespace ThriftGen;
using namespace boost;
using namespace std;

class SharedMutatorMapKey {
public:
  SharedMutatorMapKey(const String &tablename, const ThriftGen::MutateSpec &mutate_spec) :
      m_tablename(tablename), m_mutate_spec(mutate_spec) {}

  int compare(const SharedMutatorMapKey &skey) const {
    int cmp = m_tablename.compare(skey.m_tablename);
    if (cmp != 0)
      return cmp;
    cmp = m_mutate_spec.appname.compare(skey.m_mutate_spec.appname);
    if (cmp != 0)
      return cmp;
    cmp = m_mutate_spec.flush_interval - skey.m_mutate_spec.flush_interval;
    if (cmp != 0)
      return cmp;
    cmp = m_mutate_spec.flags - skey.m_mutate_spec.flags;
    return cmp;
  }

  String m_tablename;
  ThriftGen::MutateSpec m_mutate_spec;
};

inline bool operator < (const SharedMutatorMapKey &skey1, const SharedMutatorMapKey &skey2) {
  return skey1.compare(skey2) < 0;
}


typedef Meta::list<ThriftBrokerPolicy, DefaultCommPolicy> Policies;

typedef std::map<SharedMutatorMapKey, TableMutatorPtr> SharedMutatorMap;
typedef hash_map< ::int64_t, TableScannerPtr> ScannerMap;
typedef hash_map< ::int64_t, TableMutatorPtr> MutatorMap;
typedef std::vector<ThriftGen::Cell> ThriftCells;
typedef std::vector<CellAsArray> ThriftCellsAsArrays;

void
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

  if (tss.__isset.keys_only)
    hss.keys_only = tss.keys_only;

  // shallow copy
  foreach(const ThriftGen::RowInterval &ri, tss.row_intervals)
    hss.row_intervals.push_back(Hypertable::RowInterval(ri.start_row.c_str(),
        ri.start_inclusive, ri.end_row.c_str(), ri.end_inclusive));

  foreach(const ThriftGen::CellInterval &ci, tss.cell_intervals)
    hss.cell_intervals.push_back(Hypertable::CellInterval(
        ci.start_row.c_str(), ci.start_column.c_str(), ci.start_inclusive,
        ci.end_row.c_str(), ci.end_column.c_str(), ci.end_inclusive));

  foreach(const std::string &col, tss.columns)
    hss.columns.push_back(col.c_str());
}

void convert_cell(const ThriftGen::Cell &tcell, Hypertable::Cell &hcell) {
  // shallow copy
  if (tcell.key.__isset.row)
    hcell.row_key = tcell.key.row.c_str();

  if (tcell.key.__isset.column_family)
    hcell.column_family = tcell.key.column_family.c_str();

  if (tcell.key.__isset.column_qualifier)
    hcell.column_qualifier = tcell.key.column_qualifier.c_str();

  if (tcell.key.__isset.timestamp)
    hcell.timestamp = tcell.key.timestamp;

  if (tcell.key.__isset.revision)
    hcell.revision = tcell.key.revision;

  if (tcell.__isset.value) {
    hcell.value = (::uint8_t *)tcell.value.c_str();
    hcell.value_len = tcell.value.length();
  }
  if (tcell.key.__isset.flag)
    hcell.flag = tcell.key.flag;
}

int32_t convert_cell(const Hypertable::Cell &hcell, ThriftGen::Cell &tcell) {
  int32_t amount = sizeof(ThriftGen::Cell);

  tcell.key.row = hcell.row_key;
  amount += tcell.key.row.length();
  tcell.key.column_family = hcell.column_family;
  amount += tcell.key.column_family.length();

  if (hcell.column_qualifier && *hcell.column_qualifier) {
    tcell.key.column_qualifier = hcell.column_qualifier;
    tcell.key.__isset.column_qualifier = true;
    amount += tcell.key.column_qualifier.length();
  }
  else
    tcell.key.__isset.column_qualifier = false;

  tcell.key.timestamp = hcell.timestamp;
  tcell.key.revision = hcell.revision;

  if (hcell.value && hcell.value_len) {
    tcell.value = std::string((char *)hcell.value, hcell.value_len);
    tcell.__isset.value = true;
    amount += hcell.value_len;
  }
  else
    tcell.__isset.value = false;

  tcell.key.flag = hcell.flag;
  tcell.key.__isset.row = tcell.key.__isset.column_family = tcell.key.__isset.timestamp
      = tcell.key.__isset.revision = tcell.key.__isset.flag = true;
  return amount;
}

void convert_cells(const ThriftCells &tcells, Hypertable::Cells &hcells) {
  // shallow copy
  foreach(const ThriftGen::Cell &tcell, tcells) {
    Hypertable::Cell hcell;
    convert_cell(tcell, hcell);
    hcells.push_back(hcell);
  }
}

::int64_t
cell_str_to_num(const std::string &from, const char *label,
                ::int64_t min_num = INT64_MIN, ::int64_t max_num = INT64_MAX) {
  char *endp;

  ::int64_t value = strtoll(from.data(), &endp, 0);

  if (endp - from.data() != (int)from.size()
      || value < min_num || value > max_num)
    HT_THROWF(Error::BAD_KEY, "Error converting %s to %s", from.c_str(), label);

  return value;
}

void convert_cell(const CellAsArray &tcell, Hypertable::Cell &hcell) {
  int len = tcell.size();

  switch (len) {
  case 7: hcell.flag = cell_str_to_num(tcell[6], "cell flag", 0, 255);
  case 6: hcell.revision = cell_str_to_num(tcell[5], "revision");
  case 5: hcell.timestamp = cell_str_to_num(tcell[4], "timestamp");
  case 4: hcell.value = (::uint8_t *)tcell[3].c_str();
          hcell.value_len = tcell[3].length();
  case 3: hcell.column_qualifier = tcell[2].c_str();
  case 2: hcell.column_family = tcell[1].c_str();
  case 1: hcell.row_key = tcell[0].c_str();
    break;
  default:
    HT_THROWF(Error::BAD_KEY, "CellAsArray: bad size: %d", len);
  }
}

int32_t convert_cell(const Hypertable::Cell &hcell, CellAsArray &tcell) {
  int32_t amount = 5*sizeof(std::string);
  tcell.resize(5);
  tcell[0] = hcell.row_key;
  amount += tcell[0].length();
  tcell[1] = hcell.column_family;
  amount += tcell[1].length();
  tcell[2] = hcell.column_qualifier ? hcell.column_qualifier : "";
  amount += tcell[2].length();
  tcell[3] = std::string((char *)hcell.value, hcell.value_len);
  amount += tcell[3].length();
  tcell[4] = format("%llu", (Llu)hcell.timestamp);
  amount += tcell[4].length();
  return amount;
}

void
convert_cells(const ThriftCellsAsArrays &tcells, Hypertable::Cells &hcells) {
  // shallow copy
  foreach(const CellAsArray &tcell, tcells) {
    Hypertable::Cell hcell;
    convert_cell(tcell, hcell);
    hcells.push_back(hcell);
  }
}

void convert_table_split(const Hypertable::TableSplit &hsplit, ThriftGen::TableSplit &tsplit) {

  /** start_row **/
  if (hsplit.start_row) {
    tsplit.start_row = hsplit.start_row;
    tsplit.__isset.start_row = true;
  }
  else {
    tsplit.start_row = "";
    tsplit.__isset.start_row = false;
  }

  /** end_row **/
  if (hsplit.end_row &&
      !(hsplit.end_row[0] == (char)0xff && hsplit.end_row[1] == (char)0xff)) {
    tsplit.end_row = hsplit.end_row;
    tsplit.__isset.end_row = true;
  }
  else {
    tsplit.end_row = Hypertable::Key::END_ROW_MARKER;
    tsplit.__isset.end_row = false;
  }

  /** location **/
  if (hsplit.location) {
    tsplit.location = hsplit.location;
    tsplit.__isset.location = true;
  }
  else {
    tsplit.location = "";
    tsplit.__isset.location = false;
  }

  /** ip_address **/
  if (hsplit.ip_address) {
    tsplit.ip_address = hsplit.ip_address;
    tsplit.__isset.ip_address = true;
  }
  else {
    tsplit.ip_address = "";
    tsplit.__isset.ip_address = false;
  }

}


class ServerHandler;

template <class ResultT, class CellT>
struct HqlCallback : HqlInterpreter::Callback {
  typedef HqlInterpreter::Callback Parent;

  ResultT &result;
  ServerHandler &handler;
  bool flush, buffered;

  HqlCallback(ResultT &r, ServerHandler *handler, bool flush, bool buffered)
    : result(r), handler(*handler), flush(flush), buffered(buffered) { }

  virtual void on_return(const String &);
  virtual void on_scan(TableScanner &);
  virtual void on_finish(TableMutator *);
};

class ServerHandler : public HqlServiceIf {
public:
  ServerHandler() {
    m_log_api = Config::get_bool("ThriftBroker.API.Logging");
    m_next_threshold = Config::get_i32("ThriftBroker.NextThreshold");
    m_client = new Hypertable::Client();
  }

  virtual void
  hql_exec(HqlResult& result, const String &hql, bool noflush,
           bool unbuffered) {
    LOG_API("hql="<< hql <<" noflush="<< noflush <<" unbuffered="<< unbuffered);

    try {
      HqlCallback<HqlResult, ThriftGen::Cell>
          cb(result, this, !noflush, !unbuffered);
      get_hql_interp().execute(hql, cb);
      LOG_HQL_RESULT(result);
    } RETHROW()
  }

  virtual void
  hql_query(HqlResult& result, const String &hql) {
    hql_exec(result, hql, false, false);
  }

  virtual void
  hql_exec2(HqlResult2& result, const String &hql, bool noflush,
            bool unbuffered) {
    LOG_API("hql="<< hql <<" noflush="<< noflush <<" unbuffered="<< unbuffered);

    try {
      HqlCallback<HqlResult2, CellAsArray>
          cb(result, this, !noflush, !unbuffered);
      get_hql_interp().execute(hql, cb);
      LOG_HQL_RESULT(result);
    } RETHROW()
  }

  virtual void
  hql_query2(HqlResult2& result, const String &hql) {
    hql_exec2(result, hql, false, false);
  }

  virtual void create_table(const String &table, const String &schema) {
    LOG_API("table="<< table <<" schema="<< schema);

    try {
      m_client->create_table(table, schema);
      LOG_API("table="<< table <<" done");
    } RETHROW()
  }

  virtual Scanner
  open_scanner(const String &table, const ThriftGen::ScanSpec &ss,
               bool retry_table_not_found) {
    LOG_API("table="<< table <<" scan_spec="<< ss);

    try {
      Scanner id = get_scanner_id(_open_scanner(table, ss,
                                                retry_table_not_found).get());
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
      _next(result, scanner, m_next_threshold);
      LOG_API("scanner="<< scanner_id <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void
  next_cells_as_arrays(ThriftCellsAsArrays &result, const Scanner scanner_id) {
    LOG_API("scanner="<< scanner_id);

    try {
      TableScannerPtr scanner = get_scanner(scanner_id);
      _next(result, scanner, m_next_threshold);
      LOG_API("scanner="<< scanner_id <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void next_row(ThriftCells &result, const Scanner scanner_id) {
    LOG_API("scanner="<< scanner_id);

    try {
      TableScannerPtr scanner = get_scanner(scanner_id);
      _next_row(result, scanner);
      LOG_API("scanner="<< scanner_id <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void
  next_row_as_arrays(ThriftCellsAsArrays &result, const Scanner scanner_id) {
    LOG_API("scanner="<< scanner_id);

    try {
      TableScannerPtr scanner = get_scanner(scanner_id);
      _next_row(result, scanner);
      LOG_API("scanner="<< scanner_id <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void
  get_row(ThriftCells &result, const String &table, const String &row) {
    LOG_API("table="<< table <<" row="<< row);

    try {
      _get_row(result, table, row);
      LOG_API("table="<< table <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void
  get_row_as_arrays(ThriftCellsAsArrays &result, const String &table,
                    const String &row) {
    LOG_API("table="<< table <<" row="<< row);

    try {
      _get_row(result, table, row);
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
      TableScannerPtr scanner = t->create_scanner(ss, 0, true);

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
      TableScannerPtr scanner = _open_scanner(table, ss, true);
      _next(result, scanner, INT32_MAX);
      LOG_API("table="<< table <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void
  get_cells_as_arrays(ThriftCellsAsArrays &result, const String &table,
                      const ThriftGen::ScanSpec &ss) {
    LOG_API("table="<< table <<" scan_spec="<< ss);

    try {
      TableScannerPtr scanner = _open_scanner(table, ss, true);
      _next(result, scanner, INT32_MAX);
      LOG_API("table="<< table <<" result.size="<< result.size());
    } RETHROW()
  }

  virtual void
  put_cells(const String &table, const ThriftGen::MutateSpec &mutate_spec,
            const ThriftCells &cells) {
    LOG_API(" table=" << table <<" mutate_spec.appname="<< mutate_spec.appname <<
            " cells.size="<< cells.size());

    try {
      _put_cells(table, mutate_spec, cells);
      LOG_API("mutate_spec.appname="<< mutate_spec.appname <<" done");
    } RETHROW()
  }

  virtual void
  put_cell(const String &table, const ThriftGen::MutateSpec &mutate_spec,
            const ThriftGen::Cell &cell) {
    LOG_API(" table=" << table <<" mutate_spec.appname="<< mutate_spec.appname <<
            " cell="<< cell);

    try {
      _put_cell(table, mutate_spec, cell);
      LOG_API("mutate_spec.appname="<< mutate_spec.appname <<" done");
    } RETHROW()
  }

  virtual void
  put_cells_as_arrays(const String &table, const ThriftGen::MutateSpec &mutate_spec,
            const ThriftCellsAsArrays &cells) {
    LOG_API(" table=" << table <<" mutate_spec.appname="<< mutate_spec.appname <<
            " cells.size="<< cells.size());

    try {
      _put_cells(table, mutate_spec, cells);
      LOG_API("mutate_spec.appname="<< mutate_spec.appname <<" done");
    } RETHROW()
  }

  virtual void
  put_cell_as_array(const String &table, const ThriftGen::MutateSpec &mutate_spec,
            const CellAsArray &cell) {
    // gcc 4.0.1 cannot seems to handle << cell here (see ThriftHelper.h)
    LOG_API(" table=" << table <<" mutate_spec.appname="<< mutate_spec.appname <<
            " cell.size="<< cell.size());

    try {
      _put_cell(table, mutate_spec, cell);
      LOG_API("mutate_spec.appname="<< mutate_spec.appname <<" done");
    } RETHROW()
  }

  virtual Mutator open_mutator(const String &table, ::int32_t flags,
                               ::int32_t flush_interval) {
    LOG_API("table="<< table <<" flags="<< flags <<" flush_interval="<< flush_interval);

    try {
      TablePtr t = m_client->open_table(table);
      Mutator id =  get_mutator_id(t->create_mutator(0, flags, flush_interval));
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
      _set_cells(mutator, cells);
      LOG_API("mutator="<< mutator <<" done");
    } RETHROW()
  }

  virtual void set_cell(const Mutator mutator, const ThriftGen::Cell &cell) {
    LOG_API("mutator="<< mutator <<" cell="<< cell);

    try {
      _set_cell(mutator, cell);
      LOG_API("mutator="<< mutator <<" done");
    } RETHROW()
  }

  virtual void
  set_cells_as_arrays(const Mutator mutator, const ThriftCellsAsArrays &cells) {
    LOG_API("mutator="<< mutator <<" cell.size="<< cells.size());

    try {
      _set_cells(mutator, cells);
      LOG_API("mutator="<< mutator <<" done");
    } RETHROW()
  }

  virtual void
  set_cell_as_array(const Mutator mutator, const CellAsArray &cell) {
    // gcc 4.0.1 cannot seems to handle << cell here (see ThriftHelper.h)
    LOG_API("mutator="<< mutator <<" cell_as_array.size="<< cell.size());

    try {
      _set_cell(mutator, cell);
      LOG_API("mutator="<< mutator <<" done");
    } RETHROW()
  }

  virtual bool exists_table(const String &name) {
    LOG_API("table="<< name);

    try {
      bool exists = m_client->exists_table(name);
      LOG_API("table="<< name <<" exists="<< exists);
      return exists;
    } RETHROW()
  }

  virtual ::int32_t get_table_id(const String &name) {
    LOG_API("table="<< name);

    try {
      ::int32_t id = m_client->get_table_id(name);
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

  virtual void get_table_splits(std::vector<ThriftGen::TableSplit> & _return, const std::string& name) {
    LOG_API("table="<<name);
    try {
      TableSplitsContainer splits;
      ThriftGen::TableSplit tsplit;
      m_client->get_table_splits(name, splits);
      for (TableSplitsContainer::iterator iter=splits.begin(); iter != splits.end(); ++iter) {
	convert_table_split(*iter, tsplit);
	_return.push_back(tsplit);
      }
      LOG_API("splits.size="<< _return.size());
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
  _open_scanner(const String &name, const ThriftGen::ScanSpec &ss,
                bool retry_table_not_found) {
    TablePtr t = m_client->open_table(name);
    Hypertable::ScanSpec hss;
    convert_scan_spec(ss, hss);
    return t->create_scanner(hss, 0, retry_table_not_found);
  }

  template <class CellT>
  void _next(vector<CellT> &result, TableScannerPtr &scanner, int limit) {
    Hypertable::Cell cell;
    int32_t amount_read = 0;

    while (amount_read < limit) {
      if (scanner->next(cell)) {
        CellT tcell;
        amount_read += convert_cell(cell, tcell);
        result.push_back(tcell);
      }
      else
        break;
    }
  }

  template <class CellT>
  void _next_row(vector<CellT> &result, TableScannerPtr &scanner) {
    Hypertable::Cell cell;
    std::string prev_row;

    while (scanner->next(cell)) {
      if (prev_row.empty() || prev_row == cell.row_key) {
        CellT tcell;
        convert_cell(cell, tcell);
        result.push_back(tcell);
        prev_row = cell.row_key;
      }
      else {
        scanner->unget(cell);
        break;
      }
    }
  }

  template <class CellT>
  void _get_row(vector<CellT> &result, const String &table, const String &row) {
    TablePtr t = m_client->open_table(table);
    Hypertable::ScanSpec ss;
    ss.row_intervals.push_back(Hypertable::RowInterval(row.c_str(), true,
                                                       row.c_str(), true));
    ss.max_versions = 1;
    TableScannerPtr scanner = t->create_scanner(ss);
    _next(result, scanner, INT32_MAX);
  }

  HqlInterpreter &get_hql_interp() {
    ScopedLock lock(m_interp_mutex);

    if (!m_hql_interp)
      m_hql_interp = m_client->create_hql_interpreter();

    return *m_hql_interp;
  }

  template <class CellT>
  void _put_cells(const String &table, const ThriftGen::MutateSpec &mutate_spec,
                  const vector<CellT> &cells) {
    Hypertable::Cells hcells;
    convert_cells(cells, hcells);
    get_shared_mutator(table, mutate_spec)->set_cells(hcells);
  }

  template <class CellT>
  void _put_cell(const String &table, const ThriftGen::MutateSpec &mutate_spec,
                 const CellT &cell) {
    CellsBuilder cb;
    Hypertable::Cell hcell;
    convert_cell(cell, hcell);
    cb.add(hcell, false);
    get_shared_mutator(table, mutate_spec)->set_cells(cb.get());
  }

  template <class CellT>
  void _set_cells(const Mutator mutator, const vector<CellT> &cells) {
    Hypertable::Cells hcells;
    convert_cells(cells, hcells);
    get_mutator(mutator)->set_cells(hcells);
  }

  template <class CellT>
  void _set_cell(const Mutator mutator, const CellT &cell) {
    CellsBuilder cb;
    Hypertable::Cell hcell;
    convert_cell(cell, hcell);
    cb.add(hcell, false);
    get_mutator(mutator)->set_cells(cb.get());
  }

  ::int64_t get_scanner_id(TableScanner *scanner) {
    ScopedLock lock(m_scanner_mutex);
    ::int64_t id = (::int64_t)scanner;
    m_scanner_map.insert(make_pair(id, scanner)); // no overwrite
    return id;
  }

  TableScannerPtr get_scanner(::int64_t id) {
    ScopedLock lock(m_scanner_mutex);
    ScannerMap::iterator it = m_scanner_map.find(id);

    if (it != m_scanner_map.end())
      return it->second;

    HT_ERROR_OUT << "Bad scanner id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_SCANNER_ID,
             format("Invalid scanner id: %lld", (Lld)id));
  }

  void remove_scanner(::int64_t id) {
    ScopedLock lock(m_scanner_mutex);
    ScannerMap::iterator it = m_scanner_map.find(id);

    if (it != m_scanner_map.end()) {
      m_scanner_map.erase(it);
      return;
    }

    HT_ERROR_OUT << "Bad scanner id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_SCANNER_ID,
             format("Invalid scanner id: %lld", (Lld)id));
  }

  ::int64_t get_mutator_id(TableMutator *mutator) {
    ScopedLock lock(m_mutator_mutex);
    ::int64_t id = (::int64_t)mutator;
    m_mutator_map.insert(make_pair(id, mutator)); // no overwrite
    return id;
  }

  TableMutatorPtr get_shared_mutator(const String &table, const ThriftGen::MutateSpec &mutate_spec) {
    ScopedLock lock(m_shared_mutator_mutex);
    SharedMutatorMapKey skey(table, mutate_spec);

    SharedMutatorMap::iterator it = m_shared_mutator_map.find(skey);

    // if mutator exists then return it
    if (it != m_shared_mutator_map.end())
      return it->second;
    else {
      // else create it and insert it in the map
      LOG_API("creating shared mutator on table="<< table <<" with appname="
              << mutate_spec.appname);
      TablePtr t = m_client->open_table(table) ;
      TableMutatorPtr mutator = t->create_mutator(0, mutate_spec.flags, mutate_spec.flush_interval);
      m_shared_mutator_map[skey] = mutator;
      return mutator;
    }
  }

  TableMutatorPtr get_mutator(::int64_t id) {
    ScopedLock lock(m_mutator_mutex);
    MutatorMap::iterator it = m_mutator_map.find(id);

    if (it != m_mutator_map.end())
      return it->second;

    HT_ERROR_OUT << "Bad mutator id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_MUTATOR_ID,
             format("Invalid mutator id: %lld", (Lld)id));
  }

  void remove_mutator(::int64_t id) {
    ScopedLock lock(m_mutator_mutex);
    MutatorMap::iterator it = m_mutator_map.find(id);

    if (it != m_mutator_map.end()) {
      m_mutator_map.erase(it);
      return;
    }

    HT_ERROR_OUT << "Bad mutator id - " << id << HT_END;
    THROW_TE(Error::THRIFTBROKER_BAD_MUTATOR_ID,
             format("Invalid mutator id: %lld", (Lld)id));
  }

private:
  bool             m_log_api;
  Mutex            m_scanner_mutex;
  ScannerMap       m_scanner_map;
  Mutex            m_mutator_mutex;
  MutatorMap       m_mutator_map;
  Mutex            m_shared_mutator_mutex;
  SharedMutatorMap m_shared_mutator_map;
  ::int32_t        m_next_threshold;
  ClientPtr        m_client;
  Mutex            m_interp_mutex;
  HqlInterpreterPtr m_hql_interp;
};

template <class ResultT, class CellT>
void HqlCallback<ResultT, CellT>::on_return(const String &ret) {
  result.results.push_back(ret);
  result.__isset.results = true;
}

template <class ResultT, class CellT>
void HqlCallback<ResultT, CellT>::on_scan(TableScanner &s) {
  if (buffered) {
    Hypertable::Cell hcell;
    CellT tcell;

    while (s.next(hcell)) {
      convert_cell(hcell, tcell);
      result.cells.push_back(tcell);
    }
    result.__isset.cells = true;
  }
  else {
    result.scanner = handler.get_scanner_id(&s);
    result.__isset.scanner = true;
  }
}

template <class ResultT, class CellT>
void HqlCallback<ResultT, CellT>::on_finish(TableMutator *m) {
  if (flush) {
    Parent::on_finish(m);
  }
  else if (m) {
    result.mutator = handler.get_mutator_id(m);
    result.__isset.mutator = true;
  }
}

}} // namespace Hypertable::ThriftBroker


int main(int argc, char **argv) {
  using namespace Hypertable;
  using namespace ThriftBroker;

  try {
    init_with_policies<Policies>(argc, argv);

    ::uint16_t port = get_i16("port");
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
