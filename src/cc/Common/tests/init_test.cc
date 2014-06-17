#include "Common/Compat.h"
#include "Common/Init.h"

using namespace Hypertable;
using namespace Hypertable::Config;

namespace {

struct AppPolicy : Policy {
  static void init_options() {
    cmdline_desc("Usage: %s [Options] [args]\nOptions").add_options()
      ("i16", i16(), "16-bit integer")
      ("i32", i32(), "32-bit integer")
      ("i64", i64(), "64-bit integer")
      ("i64s", i64s(), "a list of 64-bit integers")
      ("boo", boo()->zero_tokens()->default_value(false), "a boolean arg")
      ("str", str(), "a string arg")
      ("strs", strs(), "a list of strings")
      ("f64", f64(), "a double arg")
      ("f64s", f64s(), "a list of doubles")
      ;
    cmdline_hidden_desc().add_options()("args", strs(), "arguments");
    cmdline_positional_desc().add("args", -1);
  }
};

typedef Meta::list<AppPolicy, DefaultPolicy> Policies;

} // local namespace

int main(int argc, char *argv[]) {
  init_with_policies<Policies>(argc, argv);
  Config::properties->print(std::cout);
}
