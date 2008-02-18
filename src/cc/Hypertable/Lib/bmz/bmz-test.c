/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include "bmz-internal.h"
#include "test-helper.h"
#ifndef HT_NO_MMAP
#include <sys/mman.h>
static size_t s_no_mmap = 0;
#else
static size_t s_no_mmap = 1;
#endif

/* To silence warnings in format strings */
typedef long unsigned Lu;

#define TIMES(_n_, _code_) do { \
  size_t _n = _n_; \
  while (_n--) { _code_; } \
} while (0)

#define BENCH(_label_, _code_, _n_, _times_) do { \
  double t1; \
  HT_MEASURE(t1, TIMES(_times_, _code_)); \
  printf("%16s: %.3fs (%.3fMB/s)\n", \
         _label_, t1, (_n_) / 1e6 / t1 *( _times_)); \
  fflush(stdout); fflush(stderr); \
} while (0)

#define LOG(_lvl_, _fmt_, ...) if (s_verbosity >= _lvl_) do { \
  fprintf(stderr, "%s: " _fmt_ "\n", __FUNCTION__, ##__VA_ARGS__); \
} while(0)

#define DIE(_fmt_, ...) do { \
  LOG(0, "fatal: " _fmt_, ##__VA_ARGS__); \
  exit(1); \
} while (0)

/* options */
#define O_BENCH_HASH    (1 << 0)
#define O_BENCH_LUT     (1 << 1)
#define O_CHECK_HASH    (1 << 2)
#define O_MEMCPY        (1 << 3)
#define O_HASH_MOD      (1 << 4)
#define O_HASH_MOD16X2  (1 << 5)
#define O_HASH_MASK16X2 (1 << 6)
#define O_HASH_MASK     (1 << 7)
#define O_HASH_MASK32X2 (1 << 8)
#define O_DEFAULT       (0xffffffff & ~O_CHECK_HASH)
#define O_HASHES        0xfffffff0

/* defaults */
static size_t s_options = 0;
static size_t s_fp_len = 20;
static size_t s_offset = 0;
static size_t s_times = 1;
static int s_verbosity = 0;
static int s_bm_dump = 0;
static int s_show_hash = 0;
/* From Andrew Trigdell's thesis p72-73:
 * D1D3: b1=3, b2=7, m1=0xffff m2=(0xffff - 4)
 * D3D4: b1=7, b2=17, m1=(0xffff - 4), m2=(0xffff - 6)
 */
static size_t s_b1 = 257;
static size_t s_b2 = 277;
static size_t s_m = 0xffffffff;
static size_t s_m1 = 0xffff;
static size_t s_m2 = (0xffff - 4);

static void
dump_bm(const char *label, const char *in, size_t len) {
  int ret;

  if (s_verbosity > 1) {
    printf("----%s encoded:\n", label);
    fwrite(in, 1, len, stdout);
  }
  if (s_bm_dump) {
    printf("\n----%s dumped:\n", label);

    if ((ret = bmz_bm_dump(in, len)) != BMZ_E_OK)
      LOG(1, "error: bad encoded data (ret=%d)", ret);

    puts("\n----end-dump");
  }
}

static void
test_bm_mod(const char *in, size_t len, char *out, size_t *len_p,
            void *work_mem) {
  bmz_bm_pack_mod(in, len, out, len_p, s_offset, s_fp_len, work_mem, s_b1, s_m);
  dump_bm("mod", out, *len_p);
}

static void
test_bm_mod16x2(const char *in, size_t len, char *out, size_t *len_p,
             void *work_mem) {
  bmz_bm_pack_mod16x2(in, len, out, len_p, s_offset, s_fp_len,
                      work_mem, s_b1, s_b2, s_m1, s_m2);
  dump_bm("mod16x2", out, *len_p);
}

static void
test_bm_mask16x2(const char *in, size_t len, char *out, size_t *len_p,
                 void *work_mem) {
  bmz_bm_pack_mask16x2(in, len, out, len_p, s_offset, s_fp_len,
                       work_mem, s_b1, s_b2);
  dump_bm("mask16x2", out, *len_p);
}

static void
test_bm_mask(const char *in, size_t len, char *out, size_t *len_p,
             void *work_mem) {
  bmz_bm_pack_mask(in, len, out, len_p, s_offset, s_fp_len, work_mem, s_b1);
  dump_bm("mask", out, *len_p);
}

static void
test_bm_mask32x2(const char *in, size_t len, char *out, size_t *len_p,
                 void *work_mem) {
  bmz_bm_pack_mask32x2(in, len, out, len_p, s_offset, s_fp_len,
                       work_mem, s_b1, s_b2);
  dump_bm("mask32x2", out, *len_p);
}

static void
test_bm_unpack(const char *in, size_t len, char *out, size_t *len_p) {
  int ret = bmz_bm_unpack(in, len, out, len_p);
  LOG(1, "\nbm_unpack returned %d, size: %lu\n", ret, (Lu)*len_p);
  if (s_verbosity < 2) return;
  puts("bm decoded:");
  fwrite(out, 1, *len_p, stdout);
  puts("\nend-decoded");
}

static char *
read_from_fp(FILE *fp, size_t *len_p) {
  char *data = NULL;
  char buf[65536];
  size_t len = 0, size = 0, ret;

  while ((ret = fread(buf, 1, sizeof(buf), fp)) > 0) {
    len += ret;

    if (len > size) {
      size = (len + 16) * 3 / 2;
      data = realloc(data, size);
    }
    memcpy(data + len - ret, buf, ret);
  }
  *len_p = len;
  return data;
}

static void
print_hash(const char *label, size_t h) {
  printf("%16s: %lx\n", label, (Lu)h);
}

static void
show_hash(const char *data, size_t len) {

  if (s_options & O_HASH_MOD)
    print_hash("hash-mod", bmz_hash_mod(data, len, s_b1, s_m));

  if (s_options & O_HASH_MOD16X2)
    print_hash("hash-mod16x2", bmz_hash_mod16x2(data, len, s_b1, s_b2,
                                                s_m1, s_m2));
  if (s_options & O_HASH_MASK16X2)
    print_hash("hash-mask16X2", bmz_hash_mask16x2(data, len, s_b1, s_b2));

  if (s_options & O_HASH_MASK)
    print_hash("hash-mask", bmz_hash_mask(data, len, s_b1));

  if (s_options & O_HASH_MASK32X2)
    print_hash("hash-mask32x2", bmz_hash_mask32x2(data, len, s_b1, s_b2));
}

static void
test_from_string(const char *data, size_t len) {
  char *buf, *mem;
  size_t n = s_times;
  int opt = s_options;
  size_t out_len, out_len0, len2 = len, work_len;

  if (s_show_hash) {
    show_hash(data, len);
    return;
  }

  out_len0 = out_len = bmz_pack_buflen(len);
  buf = malloc(out_len);
  work_len = bmz_bm_pack_worklen(len, s_fp_len);
  mem = malloc(work_len);
  LOG(1, "input length: %lu, out_len %lu, work_len: %lu\n",
      (Lu)len, (Lu)out_len, (Lu)work_len);

  /* memcpy/memmove for comparison */
  if (opt & O_MEMCPY) {
    BENCH("memcpy", memcpy(buf, data, len), len, n);
  }

  if (opt & O_CHECK_HASH) {

    if (opt & O_HASH_MOD)
      HT_CHECK(bmz_check_hash_mod(data, len, s_fp_len, s_b1, s_m) == BMZ_E_OK);

    if (opt & O_HASH_MOD16X2)
      HT_CHECK(bmz_check_hash_mod16x2(data, len, s_fp_len,
                                      s_b1, s_b2, s_m1, s_m2) == BMZ_E_OK);
    if (opt & O_HASH_MASK16X2)
      HT_CHECK(bmz_check_hash_mask16x2(data, len, s_fp_len, s_b1, s_b2)
               == BMZ_E_OK);

    if (opt & O_HASH_MASK)
      HT_CHECK(bmz_check_hash_mask(data, len, s_fp_len, s_b1) == BMZ_E_OK);

    if (opt & O_HASH_MASK32X2)
      HT_CHECK(bmz_check_hash_mask32x2(data, len, s_fp_len, s_b1, s_b2)
               == BMZ_E_OK);
  }

  if (opt & O_BENCH_HASH) {

    if (opt & O_HASH_MOD)
      BENCH("hash mod", bmz_bench_hash(data, len, BMZ_HASH_MOD), len, n);

    if (opt & O_HASH_MOD16X2)
      BENCH("hash mod16x2", bmz_bench_hash(data, len, BMZ_HASH_MOD16X2),
            len, n);

    if (opt & O_HASH_MASK16X2)
      BENCH("hash mask16x2", bmz_bench_hash(data, len, BMZ_HASH_MASK16X2),
            len, n); 

    if (opt & O_HASH_MASK)
      BENCH("hash mask", bmz_bench_hash(data, len, BMZ_HASH_MASK), len, n);

    if (opt & O_HASH_MASK32X2)
      BENCH("hash mask32x2", bmz_bench_hash(data, len, BMZ_HASH_MASK32X2),
            len, n);
  }

  if (opt & O_BENCH_LUT) {

    if (opt & O_HASH_MOD)
      BENCH("lut mod", bmz_bench_lut_mod(data, len, s_fp_len, mem, s_b1, s_m),
            len, n);

    if (opt & O_HASH_MOD16X2)
      BENCH("lut mod16x2", bmz_bench_lut_mod16x2(data, len, s_fp_len, mem,
            s_b1, s_b2, s_m1, s_m2), len, n);

    if (opt & O_HASH_MASK16X2)
      BENCH("lut mask16x2", bmz_bench_lut_mask16x2(data, len, s_fp_len, mem,
            s_b1, s_b2), len, n);

    if (opt & O_HASH_MASK)
      BENCH("lut mask", bmz_bench_lut_mask(data, len, s_fp_len, mem, s_b1),
            len, n);

    if (opt & O_HASH_MASK32X2)
      BENCH("lut mask32x2", bmz_bench_lut_mask32x2(data, len, s_fp_len, mem,
            s_b1, s_b2), len, n);
  }

  if (opt != O_DEFAULT && (opt & 0xf)) return;

  if (opt & O_HASH_MOD) {
    BENCH("bm pack mod", test_bm_mod(data, len, buf, &out_len, mem), len, n);
    BENCH("bm unpack", test_bm_unpack(buf, out_len, buf + out_len, &len2),
          len, n);
    HT_CHECK(len == len2);
    HT_CHECK(memcmp(data, buf + out_len, len) == 0);
  }

  if (opt & O_HASH_MOD16X2) {
    memset(buf, 0, out_len); out_len = out_len0;
    BENCH("bm pack mod16x2", test_bm_mod16x2(data, len, buf, &out_len, mem),
          len, n);
    BENCH("bm unpack", test_bm_unpack(buf, out_len, buf + out_len, &len2),
          len, n);
    HT_CHECK(len == len2);
    HT_CHECK(memcmp(data, buf + out_len, len) == 0);
  }

  if (opt & O_HASH_MASK16X2) {
    memset(buf, 0, out_len); out_len = out_len0;
    BENCH("bm pack mask16x2", test_bm_mask16x2(data, len, buf, &out_len, mem),
          len, n);
    BENCH("bm unpack", test_bm_unpack(buf, out_len, buf + out_len, &len2),
          len, n);
    HT_CHECK(len == len2);
    HT_CHECK(memcmp(data, buf + out_len, len) == 0);
  }

  if (opt & O_HASH_MASK) {
    memset(buf, 0, out_len); out_len = out_len0;
    BENCH("bm pack mask", test_bm_mask(data, len, buf, &out_len, mem), len, n);
    BENCH("bm unpack", test_bm_unpack(buf, out_len, buf + out_len, &len2),
          len, n);
    HT_CHECK(len == len2);
    HT_CHECK(memcmp(data, buf + out_len, len) == 0);
  }

  if (opt & O_HASH_MASK32X2) {
    memset(buf, 0, out_len); out_len = out_len0;
    BENCH("bm pack mask32x2", test_bm_mask32x2(data, len, buf, &out_len, mem),
          len, n);
    BENCH("bm unpack", test_bm_unpack(buf, out_len, buf + out_len, &len2),
          len, n);
    HT_CHECK(len == len2);
    HT_CHECK(memcmp(data, buf + out_len, len) == 0);
  }
}

static void
test_from_stdin() {
  size_t len;
  char *data = read_from_fp(stdin, &len);
  test_from_string(data, len);
}

static void
test_from_file(const char *fname) {
  int fd = open(fname, O_RDONLY, 0);
  char *data = NULL;
  struct stat st;
  long len;

  if (fd == -1) DIE("cannot open '%s'", fname);

  if (fstat(fd, &st) != 0) DIE("stat failed on '%s'", fname);

  len = st.st_size;

  if (!s_no_mmap) {
#ifndef HT_NO_MMAP
    LOG(1, "mmaping %ld bytes in to memory...", len);
    data = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

    if ((char *)-1 == data) {
      LOG(0, "mmap failed on '%s', trying alternative...", fname);
      errno = 0;
      data = NULL;
    }
#endif
  }

  if (!data) {
    LOG(1, "reading %ld bytes in to memory...", len);

    data = malloc(len);

    if (!data) DIE("error alloc'ing %ld bytes", len);

    if ((len = read(fd, data, len)) != st.st_size) 
      DIE("error reading %s (expecting %ld bytes, got %ld",
          fname, (long)st.st_size, len);
  }
  else {
  }

  test_from_string(data, st.st_size);
}

static void
show_usage() {
  printf("usage: bmz-test [options] [string...]\n%s%s%s",
         "--help                        show this help\n"
         "--verbose[=level]             set verbose level\n"
         "--file        <file>          use <file as input\n"
         "--offset      <number>        set bm offset\n"
         "--fp-len      <number>        set bm fingerprint length\n"
         "--hash                        compute hash value only\n"
         "--b1          <number>        hash param b1\n"
         "--b2          <number>        hash param b2\n"
         "--m           <number>        hash param m\n",
         "--m1          <number>        hash param m1\n"
         "--m2          <number>        hash param m2\n"
         "--hash-mod                    use hash-mod\n"
         "--hash-mod16x2                use hash-mod16x2\n"
         "--hash-mask16x2               use hash-mask16x2\n"
         "--hash-mask                   use hash-mask\n",
         "--hash-mask32x2               use hash-mask32x2\n"
         "--bench-hash                  bechmarks for builtin hashes\n"
         "--check-hash                  verify rolling hashes\n"
         "--bench-lut                   benchmarks for lookup table\n"
         "--times       <number>        number of repeats for the test\n"
         );
  exit(0);
}

int
main(int ac, char *av[]) {
  char **ia = av + 1, **a_end = av + ac, *ep;
  const char *fname = NULL;

  for (; ia < a_end; ++ia) {
    if (!strcmp("--fp-len", *ia))               s_fp_len = atoi(*++ia);
    else if (!strcmp("--offset", *ia))          s_offset = atoi(*++ia);
    else if (!strcmp("--times", *ia))           s_times = atoi(*++ia);
    else if (!strcmp("--hash", *ia))            s_show_hash = 1;
    else if (!strcmp("--b1", *ia))              s_b1 = atoi(*++ia);
    else if (!strcmp("--b2", *ia))              s_b2 = atoi(*++ia);
    else if (!strcmp("--m", *ia))               s_m = strtol(*++ia, &ep, 0);
    else if (!strcmp("--m1", *ia))              s_m1 = strtol(*++ia, &ep, 0);
    else if (!strcmp("--m2", *ia))              s_m2 = strtol(*++ia, &ep, 0);
    else if (!strcmp("--file", *ia))            fname = *++ia;
    else if (!strcmp("--no-mmap", *ia))         s_no_mmap = 1;
    else if (!strcmp("--verbose", *ia))         s_verbosity = 1;
    else if (!strncmp("--verbose=", *ia, 10))   s_verbosity = atoi(*ia + 10);
    else if (!strcmp("--bm-dump", *ia))         s_bm_dump = 1;
    else if (!strcmp("--bench-hash", *ia))      s_options |= O_BENCH_HASH;
    else if (!strcmp("--check-hash", *ia))      s_options |= O_CHECK_HASH;
    else if (!strcmp("--bench-lut", *ia))       s_options |= O_BENCH_LUT;
    else if (!strcmp("--hash-mod", *ia))        s_options |= O_HASH_MOD;
    else if (!strcmp("--hash-mod16x2", *ia))    s_options |= O_HASH_MOD16X2;
    else if (!strcmp("--hash-mask16x2", *ia))   s_options |= O_HASH_MASK16X2;
    else if (!strcmp("--hash-mask", *ia))       s_options |= O_HASH_MASK;
    else if (!strcmp("--hash-mask32x2", *ia))   s_options |= O_HASH_MASK32X2;
    else if (!strcmp("--help", *ia))            show_usage(); 
    else if (!strcmp("--", *ia)) {
      ++ia;
      break;
    }
    else if ('-' == **ia) {
      DIE("unknown option: %s\n", *ia);
    }
    else break;
  }
  bmz_set_verbosity(s_verbosity);

  if (!s_options) s_options = O_DEFAULT;
  else if (!(s_options & O_HASHES)) s_options |= O_HASHES;

  if (fname)
    test_from_file(fname);
  else if (ia >= a_end)
    test_from_stdin();
  else for (; ia < a_end; ++ia)
    test_from_string(*ia, strlen(*ia));

  return 0;
}

/* vim: et sw=2
 */
