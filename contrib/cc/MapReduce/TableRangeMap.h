#ifndef MAPREDUCE_TABLE_RANGE_MAP_H
#define MAPREDUCE_TABLE_RANGE_MAP_H

#include <Hypertable/Lib/Table.h>
#include <Hypertable/Lib/TableScanner.h>
#include <Hypertable/Lib/Types.h>
#include <Hypertable/Lib/Cell.h>
#include <Hypertable/Lib/Client.h>
#include <Hypertable/Lib/RangeLocationInfo.h>
#include <string>
#include <vector>

namespace Mapreduce
{
  using namespace Hypertable;

  class TableRangeMap {
  public:
    TableRangeMap(const std::string TableName, const std::string ConfigPath);
    ~TableRangeMap() {}

    std::vector<RangeLocationInfo> *getMap();

  private:
    TablePtr m_user_table;
    TablePtr m_meta_table;
    TableIdentifier m_table_id;
    ClientPtr m_client;
  };
}
#endif

