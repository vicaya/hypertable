#include "Common/Compat.h"
#include "Common/System.h"
#include "AsyncComm/Serialization.h"

using namespace Hypertable;
using namespace Serialization;

namespace {

void test_i16() {
  uint8_t buf[2], *p = buf;
  uint16_t input = 0xcafe;
  encode_i16(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_EXPECT(decode_i16(&p2, &len) == 0xcafeu, Error::FAILED_EXPECTATION);
}

void test_i32() {
  uint8_t buf[4], *p = buf;
  uint32_t input = 0xcafebabe;
  encode_i32(&p, 0xcafebabe);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_EXPECT(decode_i32(&p2, &len) == input, Error::FAILED_EXPECTATION);
}

void test_i64() {
  uint8_t buf[8], *p = buf;
  uint64_t input = 0xcafebabeabadbabeull;
  encode_i64(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_EXPECT(decode_i64(&p2, &len) == input, Error::FAILED_EXPECTATION);
}

void test_vi32() {
  uint8_t buf[5], *p = buf;
  uint32_t input = 0xcafebabe;
  encode_vi32(&p, 0xcafebabe);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_EXPECT(decode_vi32(&p2, &len) == input, Error::FAILED_EXPECTATION);
}

void test_vi64() {
  uint8_t buf[10], *p = buf;
  uint64_t input = 0xcafebabeabadbabeull;
  encode_vi64(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_EXPECT(decode_vi64(&p2, &len) == input, Error::FAILED_EXPECTATION);
}

void test_cstr() {
  uint8_t buf[128], *p = buf;
  const char *input = "the quick brown fox jumps over a lazy dog";
  encode_cstr(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_EXPECT(!strcmp(decode_cstr(&p2, &len), input), Error::FAILED_EXPECTATION);
}

void test_bad_vi32() {
  try {
    uint8_t buf[5] = {0xde, 0xad, 0xbe, 0xef, 0xca};
    const uint8_t *p = buf;
    size_t len = sizeof(buf);
    decode_vi32(&p, &len);
  }
  catch (Exception &e) {
    HT_INFO_OUT << e << HT_INFO_END;
    HT_EXPECT(e.code() == Error::SERIALIZATION_INPUT_OVERRUN,
              Error::FAILED_EXPECTATION);
  }
}

void test_bad_vi64() {
  try {
    uint8_t buf[14] = {0xab, 0xad, 0xba, 0xbe, 0xde, 0xad, 0xbe, 0xef,
                      0xca, 0xfe, 0xba, 0xbe, 0xbe, 0xef};
    const uint8_t *p = buf;
    size_t len = sizeof(buf);
    decode_vi64(&p, &len);
  }
  catch (Exception &e) {
    HT_INFO_OUT << e << HT_INFO_END;
    HT_EXPECT(e.code() == Error::SERIALIZATION_BAD_VINT,
              Error::FAILED_EXPECTATION);
  }
}

void test_bad_cstr() {
  try {
    uint8_t buf[20] = {0x0f, 't', 'h', 'e', ' ', 'b', 'r', 'o', 'w', 'n', ' ',
                       'f', 'o', 'x', ' ', 'j', 'u', 'm', 'p', 's'};
    const uint8_t *p = buf;
    size_t len = sizeof(buf);
    decode_cstr(&p, &len);
  }
  catch (Exception &e) {
    HT_INFO_OUT << e << HT_INFO_END;
    HT_EXPECT(e.code() == Error::SERIALIZATION_BAD_CSTR,
              Error::FAILED_EXPECTATION);
  }
}

void test_ser() {
  test_i16();
  test_i32();
  test_i64();
  test_vi32();
  test_vi64();
  test_cstr();
  test_bad_vi32();
  test_bad_vi64();
  test_bad_cstr();
}

} // local namespace

int main(int ac, char *av[]) {
  System::initialize(av[0]);

  try {
    test_ser();
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_FATAL_END;
  }
  return 0;
}
