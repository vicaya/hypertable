#include <stdio.h>
#include <db_cxx.h>

int main() {
  int major, minor, patch;
  const char *version = DbEnv::version(&major, &minor, &patch);
  if (major != DB_VERSION_MAJOR || minor != DB_VERSION_MINOR ||
      patch != DB_VERSION_PATCH) {
    fprintf(stderr, "BerkeleyDB header/library mismatch:\n "
            "header: %s\nlibrary: %s\n", DB_VERSION_STRING, version);
    return 1;
  }
  printf("%d.%d.%d\n", major, minor, patch);
  return 0;
}
