#include "Common/Compat.h"
#include "Common/Config.h"
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
    HT_ASSERT(decode_i8(&p, &len) == input);
    HT_ASSERT(p - buf == 1);
    HT_ASSERT(len == 0));
}

void test_i16() {
  uint8_t buf[2], *p = buf;
  uint16_t input = 0xcafe;
  encode_i16(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding i16",
    HT_ASSERT(decode_i16(&p2, &len) == input);
    HT_ASSERT(p2 - buf == 2);
    HT_ASSERT(len == 0));
}

void test_i32() {
  uint8_t buf[4], *p = buf;
  uint32_t input = 0xcafebabe;
  encode_i32(&p, 0xcafebabe);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding i32",
    HT_ASSERT(decode_i32(&p2, &len) == input);
    HT_ASSERT(p2 - buf == 4);
    HT_ASSERT(len == 0));
}

void test_i64() {
  uint8_t buf[8], *p = buf;
  uint64_t input = 0xcafebabeabadbabeull;
  encode_i64(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding i64",
    HT_ASSERT(decode_i64(&p2, &len) == input);
    HT_ASSERT(p2 - buf == 8);
    HT_ASSERT(len == 0));
}

void test_vi32() {
  uint8_t buf[5], *p = buf;
  uint32_t input = 0xcafebabe;
  encode_vi32(&p, 0xcafebabe);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding vint32",
    HT_ASSERT(decode_vi32(&p2, &len) == input);
    HT_ASSERT(p2 - buf == 5);
    HT_ASSERT(len == 0));
}

void test_vi64() {
  uint8_t buf[10], *p = buf;
  uint64_t input = 0xcafebabeabadbabeull;
  encode_vi64(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("decoding vint64",
    HT_ASSERT(decode_vi64(&p2, &len) == input);
    HT_ASSERT(p2 - buf == 10);
    HT_ASSERT(len == 0));
}

void test_str16() {
  uint8_t buf[128], *p = buf;
  const char *input = "the quick brown fox jumps over a lazy dog";
  encode_str16(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("testing str16",
    HT_ASSERT(!strcmp(decode_str16(&p2, &len), input));
    HT_ASSERT(p2 - buf == (int)(encoded_length_str16(input)));
    HT_ASSERT(len == sizeof(buf) - (p2 - buf)));
}

void test_bytes32() {
  uint8_t buf[128], *p = buf;
  const char *input = "the quick brown fox jumps over a lazy dog";
  size_t ilen = strlen(input);
  encode_bytes32(&p, input, ilen);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  uint32_t olen = 0;
  HT_TRY("testing bytes32",
    HT_EXPECT(olen == ilen && !memcmp(decode_bytes32(&p2, &len, &olen), input,
              ilen), -1);
    HT_ASSERT(p2 - buf == (int)(encoded_length_bytes32(ilen)));
    HT_ASSERT(len == sizeof(buf) - (p2 - buf)));
}

void test_vstr() {
  uint8_t buf[128], *p = buf;
  const char *input = "the quick brown fox jumps over a lazy dog";
  encode_vstr(&p, input);
  const uint8_t *p2 = buf;
  size_t len = sizeof(buf);
  HT_TRY("testing vstr",
    HT_ASSERT(!strcmp(decode_vstr(&p2, &len), input));
    HT_ASSERT(p2 - buf == (int)(encoded_length_vstr(input)));
    HT_ASSERT(len == sizeof(buf) - (p2 - buf)));
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
    HT_ASSERT(e.code() == Error::SERIALIZATION_INPUT_OVERRUN);
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
    HT_ASSERT(e.code() == Error::SERIALIZATION_BAD_VINT);
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
    HT_ASSERT(e.code() == Error::SERIALIZATION_BAD_VSTR);
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
  Config::init(ac, av);

  try {
    test_ser();
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
