#include <vector>
#include <string>
#include "Table.h"
#include "TableScanner.h"
#include "Types.h"
#include "Cell.h"
#include "Client.h"
#include "RangeLocationInfo.h"

namespace Hypertable {
  class TableRangeMap {
  public:
    TableRangeMap(std::string TableName);
    ~TableRangeMap() {}
  
    std::vector<RangeLocationInfo> *getMap();

  private:
    TablePtr m_user_table;
    TablePtr m_meta_table;
    TableIdentifier m_table_id;
    ClientPtr m_client;
  };
}
