#include "Common/Compat.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include <stdio.h>
#include <signal.h>
#include <setjmp.h>

using namespace Hypertable;

namespace {

int n_sigs = 0;
const int N_EXPECTED_SIGS = 3;
jmp_buf jmp_ctx;

#define DEF_SIG_HANDLER(_sig_) \
void _sig_##_handler(int) { \
  if (++n_sigs >= N_EXPECTED_SIGS) { \
    HT_INFO("Caught " #_sig_ " signal, exiting..."); \
    exit(0); \
  } \
  HT_INFO("Caught " #_sig_ " signal, continuing..."); \
  HT_EXPECT(signal(_sig_, _sig_##_handler) != SIG_ERR, -1); \
  longjmp(jmp_ctx, 1); \
}

#define INSTALL_SIG_HANDLER(_sig_) \
  HT_EXPECT(signal(_sig_, _sig_##_handler) != SIG_ERR, -1)

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
  if (setjmp(jmp_ctx) == 0)
    HT_FATAL(msg);
  if (setjmp(jmp_ctx) == 0)
    HT_FATALF("%s", msg);
  HT_LOG_ENTER;
  HT_LOG_EXIT;
  HT_DEBUG_OUT << msg << HT_DEBUG_END;
  HT_INFO_OUT << msg << HT_INFO_END;
  HT_NOTICE_OUT << msg << HT_NOTICE_END;
  HT_WARN_OUT << msg << HT_WARN_END;
  HT_ERROR_OUT << msg <<  HT_ERROR_END;
  HT_CRIT_OUT << msg << HT_CRIT_END;
  HT_ALERT_OUT << msg <<  HT_ALERT_END;
  HT_EMERG_OUT << msg <<  HT_EMERG_END;
  if (setjmp(jmp_ctx) == 0)
    HT_FATAL_OUT << msg <<  HT_FATAL_END;
}

} // local namespace

int main(int ac, char *av[]) {
  System::initialize(av[0]);
  test_basic_logging(av[0]);
  return 1; // normal exits in sig handlers
}
