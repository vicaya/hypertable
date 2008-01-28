
#include <cstdio>

#include "Common/Error.h"
#include "Common/System.h"

#include "Hypertable/Lib/Client.h"

using namespace Hypertable;
using namespace std;

/**
 */
int main(int argc, char **argv) {
  int error;
  ClientPtr hypertable_client_ptr;
  TablePtr table_ptr;
  TableScannerPtr scanner_ptr;
  ScanSpecificationT scan_spec;
  CellT cell;
  uint32_t nsec;
  time_t unix_time;
  struct tm tms;

  if (argc <= 1) { 
    cout << "usage: apache_log_query <page>" << endl;
    return 0;
  }

  try {

    hypertable_client_ptr = new Client(argv[0]);

    if ((error = hypertable_client_ptr->open_table("WebServerLog", table_ptr)) != Error::OK) {
      cerr << "Error: unable to open table 'WebServerLog' - " << Error::get_text(error) << endl;
      return 1;
    }

    scan_spec.rowLimit = 1;
    scan_spec.max_versions = 0;
    scan_spec.startRow = scan_spec.endRow = argv[1];
    scan_spec.startRowInclusive = scan_spec.endRowInclusive = true;
    scan_spec.interval.first = scan_spec.interval.second = 0;

    if ((error = table_ptr->create_scanner(scan_spec, scanner_ptr)) != Error::OK) {
      cerr << "Error: problem creating scanner on table 'WebServerLog' - " << Error::get_text(error) << endl;
      return 1;
    }
  }
  catch (std::exception &e) {
    cerr << "error: " << e.what() << endl;
    return 1;
  }

  while (scanner_ptr->next(cell)) {
    nsec = cell.timestamp % 1000000000LL;
    unix_time = cell.timestamp / 1000000000LL;
    localtime_r(&unix_time, &tms);
    printf("%d-%02d-%02d %02d:%02d:%02d.%09d", tms.tm_year+1900, tms.tm_mon+1, tms.tm_mday, tms.tm_hour, tms.tm_min, tms.tm_sec, nsec);
    printf("\t%s\t%s", cell.row_key, cell.column_family);
    if (*cell.column_qualifier)
      printf(":%s", cell.column_qualifier);
    printf("\t%s\n", std::string((const char *)cell.value, cell.value_len).c_str());
  }

  return 0;
}
