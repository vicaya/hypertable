#include "Common/Compat.h"
#include "Common/Logger.h"
#include "Common/Config.h"
#include <sstream>
#include <stdio.h>
#include <signal.h>
#include <setjmp.h>

using namespace Hypertable;

namespace {

volatile int n_sigs = 0;
volatile bool last_try = false;
const int N_EXPECTED_SIGS = 4;
jmp_buf jmp_ctx;

#define DEF_SIG_HANDLER(_sig_) \
void _sig_##_handler(int) { \
  if (++n_sigs >= N_EXPECTED_SIGS && !last_try) { \
    HT_INFO("Caught unexpected " #_sig_ " signal, aborting..."); \
    exit(1); \
  } \
  HT_INFO("Caught " #_sig_ " signal, continuing..."); \
  longjmp(jmp_ctx, 1); \
}

#define INSTALL_SIG_HANDLER(_sig_) do { \
  struct sigaction sa; \
  memset(&sa, 0, sizeof(sa)); \
  sa.sa_handler = _sig_##_handler; \
  sa.sa_flags = SA_NODEFER | SA_RESTART; \
  HT_ASSERT(sigaction(_sig_, &sa, NULL) == 0); \
} while (0)

#define TRY_FATAL(_code_) do { \
  if (setjmp(jmp_ctx) == 0) { \
    _code_; \
  } \
} while (0)

DEF_SIG_HANDLER(SIGABRT)
DEF_SIG_HANDLER(SIGSEGV)
DEF_SIG_HANDLER(SIGBUS)

void test_basic_logging(const char *msg) {
  INSTALL_SIG_HANDLER(SIGABRT);
  INSTALL_SIG_HANDLER(SIGSEGV);
  INSTALL_SIG_HANDLER(SIGBUS);

  HT_DEBUG(msg);
  HT_DEBUGF("%s", msg);
  HT_INFO(msg);
  HT_INFOF("%s", msg);;
  HT_NOTICE(msg);
  HT_NOTICEF("%s", msg);
  HT_WARN(msg);
  HT_WARNF("%s", msg);
  HT_ERROR(msg);
  HT_ERRORF("%s", msg);
  HT_CRIT(msg);
  HT_CRITF("%s", msg);
  HT_ALERT(msg);
  HT_ALERTF("%s", msg);
  HT_EMERG(msg);
  HT_EMERGF("%s", msg);
  TRY_FATAL(HT_FATAL(msg));
  TRY_FATAL(HT_FATALF("%s", msg));
  HT_LOG_ENTER;
  HT_LOG_EXIT;
  HT_DEBUG_OUT << msg << HT_END;
  HT_INFO_OUT << msg << HT_END;
  HT_NOTICE_OUT << msg << HT_END;
  HT_WARN_OUT << msg << HT_END;
  HT_ERROR_OUT << msg <<  HT_END;
  HT_CRIT_OUT << msg << HT_END;
  HT_ALERT_OUT << msg <<  HT_END;
  TRY_FATAL(HT_EMERG_OUT << msg <<  HT_END);
  last_try = true;
  TRY_FATAL(HT_FATAL_OUT << msg <<  HT_END);
  HT_ASSERT(n_sigs == N_EXPECTED_SIGS);
}

// testing the workaround of log4cpp > 1k message segfault on certain platforms
void test_big_message() {
  std::ostringstream s;

  for (int i = 0; i < 2000; ++i)
    s << char('0' + (i % 10));

  String buf = s.str();

  HT_INFOF("%s %d", buf.c_str(), 1);
  HT_INFO_OUT << buf << HT_END;
}

} // local namespace

int main(int ac, char *av[]) {
  Config::init(ac, av);
  test_basic_logging(av[0]);
  test_big_message();
  return 0;
}
