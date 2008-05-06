#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/System.h"

using namespace Hypertable;

namespace {

void test_ex4() {
  throw Exception(Error::PROTOCOL_ERROR, __func__);
}

void test_ex3() {
  try {
    test_ex4();
  }
  catch (Exception &e) {
    throw Exception(e.code(), __func__, e);
  }
}

void test_ex2() {
  try {
    test_ex3();
  }
  catch (Exception &e) {
    throw Exception(e.code(), __func__, e);
  }
}

void test_ex1() {
  try {
    test_ex2();
  }
  catch (Exception &e) {
    throw Exception(e.code(), __func__, e);
  }
}

void test_ex0() {
  try {
    test_ex1();
  }
  catch (Exception &e) {
    throw Exception(e.code(), __func__, e);
  }
}

} // local namespace

int main(int ac, char *av[]) {
  System::initialize(av[0]);

  try {
    test_ex0();
  }
  catch (Exception &e) {
    HT_DEBUG_OUT << e << HT_DEBUG_END;
    HT_WARN_OUT << e << HT_WARN_END;
    HT_INFO_OUT << e << HT_INFO_END;
    HT_ERROR_OUT << e << HT_ERROR_END;
  }
}
