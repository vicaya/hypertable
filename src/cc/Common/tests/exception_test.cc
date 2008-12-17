#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Config.h"

using namespace Hypertable;

namespace ExceptionTest {

void test_ex4(double) {
  HT_THROW(Error::EXTERNAL, "throw an external error");
}

void test_ex3(uint64_t x) {
  HT_TRY("testing ex4", test_ex4(x));
}

void test_ex2(uint32_t x) {
  HT_TRY("testing ex3", test_ex3(x));
}

void test_ex1(uint16_t x) {
  HT_TRY("testing ex2", test_ex2(x));
}

void test_ex0(uint8_t x) {
  try { test_ex1(x); }
  catch (Exception &e) {
    HT_THROW2(Error::PROTOCOL_ERROR, e, "testing ex1");
  }
}

} // namespace ExceptionTest

int main(int ac, char *av[]) {
  Config::init(ac, av);

  try {
    ExceptionTest::test_ex0(0);
  }
  catch (Exception &e) {
    HT_INFO_OUT <<"Error: "<< e.messages() << HT_END;
    HT_INFO_OUT <<"Exception trace: "<< e << HT_END;
  }
}
