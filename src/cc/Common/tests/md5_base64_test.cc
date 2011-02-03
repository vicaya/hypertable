#include "Common/Compat.h"
#include "Common/md5.h"
#include "Common/TestUtils.h"

using namespace Hypertable;

int main(int ac, char *av[]) {
  char input[5][13] = { {"foobarfoobar"}, {"himnotpotchi"}, {"base64encode"}, {"ABCDEFGHIJKL"},
                        {"?kw??lr??Й?"}};
  char golden[5][17] = { {"Zm9vYmFyZm9vYmFy"}, {"aGltbm90cG90Y2hp"}, {"YmFzZTY0ZW5jb2Rl"},
                         {"QUJDREVGR0hJSktM"}, {"P2t3Pz9scj8_0Jk_"}};
  int64_t golden_hashes[5] = { 7891606660087872089LL, 241383655341626916LL,
                               -2486747253553136565LL, 7702431781597023531LL,
                               -8752389877473518803 };

  char output[17];

  for (int ii=0; ii<5; ++ii) {
    digest_to_trunc_modified_base64(input[ii], output);

    if (strcmp(output, golden[ii])) {
      printf( "Test failed expected output %s got %s\n", golden[ii], output);
      exit(1);
    }
    else
      printf("Test passed expected output %s got %s\n", golden[ii], output);

    HT_ASSERT(md5_hash((const char *)input[ii]) == golden_hashes[ii]);
  }
  exit(0);

}
