#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/SystemInfo.h";

using namespace std;
using namespace Hypertable;

namespace {

void dump_all() {
  cout << system_info_lib_version << endl;
  cout << System::cpu_info() << endl;
  cout << System::cpu_stat() << endl;
  cout << System::mem_stat() << endl;
  cout << System::disk_stat() << endl;
  cout << System::os_info() << endl;
  cout << System::swap_stat() << endl;
  cout << System::net_info() << endl;
  cout << System::net_stat() << endl;
  cout << System::proc_info() << endl;
  cout << System::proc_stat() << endl;
  cout << System::fs_stat() << endl;
  cout << System::term_info() << endl;
  cout <<"{System: install_dir='"<< System::install_dir
       <<"' exe_name='"<< System::exe_name <<"'}" << endl;
}

} // local namespace

int main(int ac, char *av[]) {
  Config::init(ac, av);
  dump_all();
  return 0;
}
