#include "Common/Compat.h"
#include "Common/System.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"

using namespace Hypertable;
using namespace Serialization;

namespace {

void test_i8() {
  uint8_t buf[1];
  uint8_t input = 0xca;
  *buf = input;
  const uint8_t *p = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding i8",
    HT_EXPECT(decode_i8(&p, &len) == input, -1);
    HT_EXPECT(p - buf == 1, -1);
    HT_EXPECT(len == 0, -1));
}

void test_i16() {
  uint8_t buf[2], *p = buf;
  uint16_t input = 0xcafe;
  encode_i16(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding i16",
    HT_EXPECT(decode_i16(&p2, &len) == input, -1);
    HT_EXPECT(p2 - buf == 2, -1);
    HT_EXPECT(len == 0, -1));
}

void test_i32() {
  uint8_t buf[4], *p = buf;
  uint32_t input = 0xcafebabe;
  encode_i32(&p, 0xcafebabe);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding i32",
    HT_EXPECT(decode_i32(&p2, &len) == input, -1);
    HT_EXPECT(p2 - buf == 4, -1);
    HT_EXPECT(len == 0, -1));
}

void test_i64() {
  uint8_t buf[8], *p = buf;
  uint64_t input = 0xcafebabeabadbabeull;
  encode_i64(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding i64",
    HT_EXPECT(decode_i64(&p2, &len) == input, -1);
    HT_EXPECT(p2 - buf == 8, -1);
    HT_EXPECT(len == 0, -1));
}

void test_vi32() {
  uint8_t buf[5], *p = buf;
  uint32_t input = 0xcafebabe;
  encode_vi32(&p, 0xcafebabe);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding vint32",
    HT_EXPECT(decode_vi32(&p2, &len) == input, -1);
    HT_EXPECT(p2 - buf == 5, -1);
    HT_EXPECT(len == 0, -1));
}

void test_vi64() {
  uint8_t buf[10], *p = buf;
  uint64_t input = 0xcafebabeabadbabeull;
  encode_vi64(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding vint64",
    HT_EXPECT(decode_vi64(&p2, &len) == input, -1);
    HT_EXPECT(p2 - buf == 10, -1);
    HT_EXPECT(len == 0, -1));
}

void test_str16() {
  uint8_t buf[128], *p = buf;
  const char *input = "the quick brown fox jumps over a lazy dog";
  encode_str16(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("testing str16",
    HT_EXPECT(!strcmp(decode_str16(&p2, &len), input), -1);
    HT_EXPECT(p2 - buf == (int)(encoded_length_str16(input)), -1);
    HT_EXPECT(len == sizeof(buf) - (p2 - buf), -1));
}

void test_bytes32() {
  uint8_t buf[128], *p = buf;
  const char *input = "the quick brown fox jumps over a lazy dog";
  size_t ilen = strlen(input);
  encode_bytes32(&p, input, ilen);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  uint32_t olen;
  HT_TRY("testing bytes32",
    HT_EXPECT(olen == ilen && !memcmp(decode_bytes32(&p2, &len, &olen), input, ilen), -1);
    HT_EXPECT(p2 - buf == (int)(encoded_length_bytes32(ilen)), -1);
    HT_EXPECT(len == sizeof(buf) - (p2 - buf), -1));
}

void test_vstr() {
  uint8_t buf[128], *p = buf;
  const char *input = "the quick brown fox jumps over a lazy dog";
  encode_vstr(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("testing vstr",
    HT_EXPECT(!strcmp(decode_vstr(&p2, &len), input), -1);
    HT_EXPECT(p2 - buf == (int)(encoded_length_vstr(input)), -1);
    HT_EXPECT(len == sizeof(buf) - (p2 - buf), -1));
}

void test_bad_vi32() {
  try {
    uint8_t buf[5] = {0xde, 0xad, 0xbe, 0xef, 0xca};
    const uint8_t *p = buf;
    size_t len = sizeof(buf);
    decode_vi32(&p, &len);
  }
  catch (Exception &e) {
    HT_INFO_OUT << e << HT_END;
    HT_EXPECT(e.code() == Error::SERIALIZATION_INPUT_OVERRUN, -1);
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
    HT_INFO_OUT << e << HT_END;
    HT_EXPECT(e.code() == Error::SERIALIZATION_BAD_VINT, -1);
  }
}

void test_bad_vstr() {
  try {
    uint8_t buf[20] = {0x0f, 't', 'h', 'e', ' ', 'b', 'r', 'o', 'w', 'n', ' ',
                       'f', 'o', 'x', ' ', 'j', 'u', 'm', 'p', 's'};
    const uint8_t *p = buf;
    size_t len = sizeof(buf);
    decode_vstr(&p, &len);
  }
  catch (Exception &e) {
    HT_INFO_OUT << e << HT_END;
    HT_EXPECT(e.code() == Error::SERIALIZATION_BAD_VSTR, -1);
  }
}

void test_ser() {
  test_i16();
  test_i32();
  test_i64();
  test_vi32();
  test_vi64();
  test_vstr();
  test_bad_vi32();
  test_bad_vi64();
  test_bad_vstr();
}

} // local namespace

int main(int ac, char *av[]) {
  System::initialize(av[0]);

  try {
    test_ser();
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
