#include <Common/Compat.h>
#include "../TableRangeMap.h"
#include <cstdio>
#include <vector>

using namespace std;
using namespace Hypertable;
using namespace Mapreduce;

int main(int argc, char *argv[])
{
  if (argc < 2)
    return -1;

  try {
    TableRangeMap meta_range_map("METADATA", "/opt/hypertable/0.9.0.9/");
    std::vector<RangeLocationInfo> *rangemap;
    cout << "Getting map..." << endl;
    rangemap = meta_range_map.getMap();
    cout << "Done correctly and safely." << endl;

    cout << "rangemap size is " << rangemap->size() << endl;
    vector<RangeLocationInfo>::iterator it;
    for (it = rangemap->begin(); it != rangemap->end(); ++it)
    {
      RangeLocationInfo r = *it;
      cout << "Range" << endl;
      cout << "\tstart row:<";
      if (r.start_row.length() == 0)
        cout << "START> ";
      else
        cout << r.start_row << "> ";

      cout << "end row:<";
      if (r.end_row == "\xff\xff")
        cout << "END> ";
      else
        cout << r.end_row << "> ";

      cout << "location: " << r.location << endl;
    }
  } catch (std::exception &e) {
    cerr << e.what() << endl;
    cout << "Error: " << e.what() << endl;
  }
}
