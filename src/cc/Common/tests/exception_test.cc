#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/System.h"

using namespace Hypertable;

namespace {

void test_ex4() {
  HT_THROW(Error::PROTOCOL_ERROR, "");
}

void test_ex3() {
  HT_TRY("testing ex4", test_ex4());
}

void test_ex2() {
  HT_TRY("testing ex3", test_ex3());
}

void test_ex1() {
  HT_TRY("testing ex2", test_ex2());
}

void test_ex0() {
  HT_TRY("testing ex1", test_ex1());
}

} // local namespace

int main(int ac, char *av[]) {
  System::initialize(av[0]);

  try {
    test_ex0();
  }
  catch (Exception &e) {
    HT_INFO_OUT <<"Exception trace: "<< e << HT_END;
  }
}
