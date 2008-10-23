#include "Common/Compat.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Stopwatch.h"

#include <arpa/inet.h>
#include <netdb.h>

using namespace Hypertable;

#define MEASURE(_code_, _n_) do { \
  size_t _n = _n_; \
  InetAddr addr; \
  addr.sin_port = htons(38060); \
  Stopwatch w; \
  while (_n--) { _code_; } \
  w.stop(); \
  std::cout << addr.hex() <<": "<< _n_ / w.elapsed() <<"/s"<< std::endl; \
} while (0)

namespace {

void test_localhost() {
  InetAddr addr("localhost", 8888);
  uint32_t expected_localhost_addr = (127 << 24) + 1;
  HT_ASSERT(addr.sin_addr.s_addr == htonl(expected_localhost_addr));
}

bool test_parse_ipv4(const char *ipstr) {
  InetAddr addr;
  if (InetAddr::parse_ipv4(ipstr, 0, addr)) {
    struct hostent *he = gethostbyname(ipstr);
    HT_ASSERT(he && *(uint32_t *)he->h_addr_list[0] == addr.sin_addr.s_addr);
    return true;
  }
  return false;
}

void bench_loop(uint32_t n, sockaddr_in &addr) {
  addr.sin_addr.s_addr = htonl(n);
}

void bench_parse_ipv4(const char *ipstr, sockaddr_in &addr, int base = 0) {
  InetAddr::parse_ipv4(ipstr, 38060, addr, base);
}

void bench_gethostbyname(const char *ipstr, sockaddr_in &addr) {
  struct hostent *he = gethostbyname(ipstr);
  addr.sin_addr.s_addr = *(uint32_t *)he->h_addr_list[0];
  addr.sin_family = AF_INET;
}

} // local namespace

int main(int argc, char *argv[]) {
  test_localhost();
  HT_ASSERT(test_parse_ipv4("10.8.238.1"));
  HT_ASSERT(test_parse_ipv4("1491994634"));
  HT_ASSERT(test_parse_ipv4("0xa08ee01"));
  HT_ASSERT(test_parse_ipv4("127.0.0.1"));
  HT_ASSERT(test_parse_ipv4("127.0.0.1_") == false);

  if (argc > 1) {
    size_t n = atoi(argv[1]);
    MEASURE(bench_loop(0xa08ee58, addr), n);
    MEASURE(bench_parse_ipv4("10.8.238.88", addr), n);
    MEASURE(bench_parse_ipv4("168357464", addr), n);
    MEASURE(bench_parse_ipv4("0xa08ee58", addr), n);
    MEASURE(bench_parse_ipv4("a08ee58", addr, 16), n);
    MEASURE(bench_gethostbyname("10.8.238.88", addr), n);
  }
  return 0;
}
