#include <stdio.h>
#include <rrd.h>

int main() {

  char *version = rrd_strversion();
  printf("%s\n", version);
  return 0;
}
