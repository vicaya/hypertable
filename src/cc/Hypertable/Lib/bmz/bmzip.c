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

/** A demo app for bmz compression */

#include "compat-c.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <stdint.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#ifndef HT_NO_MMAP
#  include <sys/mman.h>
static int s_no_mmap = 0;
#else
static int s_no_mmap = 1;
#endif

#include "bmz-internal.h"

#define BMZ_MAGIC       "BMZ"
#define BMZIP_VER       0x0110
#define BMZ_HEADER_SZ   (strlen(BMZ_MAGIC) + 2 + 1 + 6 + 4)

#define BMZ_A_PACK      0
#define BMZ_A_UNPACK    1
#define BMZ_A_LIST      2

#define BMZ_O_BM_ONLY   1
#define BMZ_O_STREAM    2       /* TODO */

typedef unsigned char Byte;

/* To silence warnings in format strings */
typedef long long unsigned Llu;
typedef long unsigned Lu;

static int s_verbosity = 0;
static int s_bm_dump = 0;
static int s_bm_hash = 0;

#define LOG(_lvl_, _fmt_, ...) if (s_verbosity >= _lvl_) do { \
  fprintf(stderr, "bmzip: %s: " _fmt_, __FUNCTION__, ##__VA_ARGS__); \
  if (errno) fprintf(stderr, ": %s", strerror(errno)); \
  putc('\n', stderr); \
} while (0)

#define WARN(_fmt_, ...) do { \
  LOG(0, "warning: " _fmt_, ##__VA_ARGS__); \
} while (0)

#define DIE(_fmt_, ...) do { \
  LOG(0, "fatal: " _fmt_, ##__VA_ARGS__); \
  exit(1); \
} while (0)

#define BMZ_ALIGN(_mem_, _n_) (Byte *)(_mem_) + _n_ - (((size_t)(_mem_))%(_n_))

#define BMZ_READ_INT16(_p_, _n_) \
  _n_ = (*_p_++ << 8); \
  _n_ |= (*_p_++)

#define BMZ_READ_INT32(_p_, _n_) \
  _n_ = (*_p_++ << 24); \
  _n_ |= (*_p_++ << 16); \
  _n_ |= (*_p_++ << 8); \
  _n_ |= (*_p_++)

#define BMZ_READ_INT48(_p_, _n_) \
  _n_ = ((uint64_t)*_p_++ << 40); \
  _n_ |= ((uint64_t)*_p_++ << 32); \
  _n_ |= (*_p_++ << 24); \
  _n_ |= (*_p_++ << 16); \
  _n_ |= (*_p_++ << 8); \
  _n_ |= (*_p_++)

#define BMZ_WRITE_INT16(_p_, _n_) \
  *_p_++ = (Byte)(_n_ >> 8); \
  *_p_++ = (Byte)(_n_)

#define BMZ_WRITE_INT32(_p_, _n_) \
  *_p_++ = (Byte)(_n_ >> 24); \
  *_p_++ = (Byte)(_n_ >> 16); \
  *_p_++ = (Byte)(_n_ >> 8); \
  *_p_++ = (Byte)(_n_)

#define BMZ_WRITE_INT48(_p_, _n_) \
  *_p_++ = (Byte)(_n_ >> 40); \
  *_p_++ = (Byte)(_n_ >> 32); \
  *_p_++ = (Byte)(_n_ >> 24); \
  *_p_++ = (Byte)(_n_ >> 16); \
  *_p_++ = (Byte)(_n_ >> 8); \
  *_p_++ = (Byte)(_n_)

static void
read_bmz_header(int fd, Byte *buf) {
  if (read(fd, buf, BMZ_HEADER_SZ) != BMZ_HEADER_SZ)
    DIE("error reading bmz file header (%lu bytes)", (Lu)BMZ_HEADER_SZ);
}

static void
parse_bmz_header(const Byte *buf, uint16_t *version_p, uint64_t *orig_size_p,
                uint32_t *checksum_p, uint32_t *options) {
  const Byte *bp = buf;
  size_t magic_len = strlen(BMZ_MAGIC);

  if (memcmp(buf, BMZ_MAGIC, magic_len)) {
    DIE("bad magic in file header (%lu bytes)", (Lu)magic_len);
  }
  bp += magic_len;
  BMZ_READ_INT16(bp, *version_p);

  if (*version_p > BMZIP_VER)
    DIE("incomaptible version: %04x", *version_p);

  *options = *bp++;
  BMZ_READ_INT48(bp, *orig_size_p);
  BMZ_READ_INT32(bp, *checksum_p);
}

static void
write_bmz_header(int fd, size_t in_len, uint32_t checksum, Byte options) {
  char buf[BMZ_HEADER_SZ], *bp = buf;
  uint64_t orig_size = in_len;

  strcpy(buf, BMZ_MAGIC);
  bp += strlen(BMZ_MAGIC);
  BMZ_WRITE_INT16(bp, BMZIP_VER);
  *bp++ = options;
  BMZ_WRITE_INT48(bp, orig_size);
  BMZ_WRITE_INT32(bp, checksum);

  if (write(fd, buf, BMZ_HEADER_SZ) != BMZ_HEADER_SZ)
    DIE("error writing header (%lu bytes)", (Lu)BMZ_HEADER_SZ);
}

static void
do_list(int fd) {
  Byte buf[BMZ_HEADER_SZ];
  uint16_t version;
  uint64_t orig_size, size;
  uint32_t checksum, options;
  struct stat st;

  if (fstat(fd, &st) != 0) DIE("error getting stat from file (%d)", fd);

  size = st.st_size;
  read_bmz_header(fd, buf);
  parse_bmz_header(buf, &version, &orig_size, &checksum, &options);
  printf("%8s%16s%16s%8s\n", "version", "compressed", "uncompressed", "ratio");
  printf("    %04x%16llu%16llu%7.2f%%\n", version, (Llu)size,
         (Llu)orig_size, orig_size ? size * 100. / orig_size : 1);
}

static void
do_pack(const void *in, size_t in_len, size_t buf_len,
        size_t offset, size_t fp_len, Byte options) {
  size_t buflen = bmz_pack_buflen(in_len), out_len = buflen;
  size_t worklen = bmz_pack_worklen(in_len, fp_len);
  int ret, bm_only = (options & BMZ_O_BM_ONLY) || s_bm_dump;
  Byte *out, *work_mem;

  if (bm_only) {
    out_len = in_len + 1;

    if (buf_len > in_len + worklen) {
      out = (Byte *)in + in_len;
      work_mem = out + out_len;
    }
    else {
      out = malloc(worklen); /* bmz_pack_worklen includes out_len for bm */

      if (!out)
        DIE("error allocating %lu bytes memory", (Lu)worklen);

      work_mem = out + out_len;
    }
    /* calling internal API need to align work memory */
    work_mem = BMZ_ALIGN(work_mem, 8);
  }
  else if (buf_len > buflen + worklen) {
    work_mem = (Byte *)in + buflen;
    out = (Byte *)in; /* inplace */
  }
  else {
    out = malloc(buflen + worklen);

    if (!out)
      DIE("error allocating %lu bytes memory", (Lu)buflen + worklen);

    work_mem = out + buflen;
  }

  if (bm_only) {
    ret = bmz_bm_pack_mask(in, in_len, out, &out_len, offset, fp_len,
                           work_mem, 257);
    if (ret != BMZ_E_OK)
      DIE("error encoding bm output (error %d)", ret);

    if (s_bm_dump) {
      if ((ret = bmz_bm_dump(out, out_len)) != BMZ_E_OK)
        WARN("error dumping bm encoding (ret=%d)", ret);

      return;
    }
  }
  else if ((ret = bmz_pack(in, in_len, out, &out_len, offset, fp_len,
                           (s_bm_hash << 24), work_mem))
           != BMZ_E_OK) {
    DIE("error compressing input (error %d)", ret);
  }
  write_bmz_header(1, in_len, bmz_checksum(out, out_len), options);
  write(1, out, out_len);
}

static void
do_unpack(const void *in, size_t in_len, size_t buf_len) {
  const Byte *bp = (Byte *)in;
  uint16_t version;
  uint64_t orig_size;
  uint32_t checksum, cs, options;
  size_t outlen, worklen, len = in_len - BMZ_HEADER_SZ;
  Byte *out, *workmem;
  int ret;

  if (in_len < BMZ_HEADER_SZ) DIE("file truncated (size: %lu)", (Lu)in_len);

  parse_bmz_header(bp, &version, &orig_size, &checksum, &options);

  if (orig_size > INT_MAX && sizeof(size_t) == 4) 
    DIE("original file size %llu requires 64-bit version of bmzip",
        (Llu)orig_size);

  bp += BMZ_HEADER_SZ;
  buf_len -= BMZ_HEADER_SZ;
  cs = bmz_checksum(bp, len);
  outlen = orig_size;

  if (cs != checksum)
    DIE("checksum mismatch (expecting %x, got %x).", checksum, cs);

  if (options & BMZ_O_BM_ONLY) {
    out = buf_len > in_len + orig_size ? (Byte*)bp + len : malloc(outlen);

    if ((ret = bmz_bm_unpack(bp, len, out, &outlen)) != BMZ_E_OK)
      DIE("error decoding bm input (error %d)", ret);
  }
  else {
    worklen = bmz_unpack_worklen(orig_size > len ? orig_size : len);
    out = (buf_len > outlen + worklen) ? (Byte *)bp : malloc(outlen + worklen);
    workmem = out + outlen;

    if ((ret = bmz_unpack(bp, len, out, &outlen, workmem)) != BMZ_E_OK)
      DIE("error decompressing (error %d)", ret);
  }
  if (orig_size != outlen)
    WARN("size mismatch (expecting %llu, got %llu)", 
         (Llu)orig_size, (Llu)outlen);

  write(1, out, outlen);
}

static void
do_block(const void *in, size_t len, size_t buf_len, size_t offset,
         size_t fp_len, int action, int options) {
  switch (action) {
  case BMZ_A_PACK:
    do_pack(in, len, buf_len, offset, fp_len, options);
    break;
  case BMZ_A_UNPACK:
    do_unpack(in, len, buf_len);
    break;
  default:
    DIE("unknown action: %d", action);
  }
}

static char *
read_from_fp(FILE *fp, size_t *len_p, size_t *size_p) {
  char *data = NULL;
  char buf[65536];
  int64_t len = 0, size = 0, ret;

  while ((ret = fread(buf, 1, sizeof(buf), fp)) > 0) {
    len += ret;
    if (len > INT_MAX)
      DIE("reading from stdin for data size greater than 2GB "
          "not yet supported (current size: %lld)", (long long)len);

    if (len > size) {
      size = (len + 16) * 5 / 2;
      data = realloc(data, size);
    }
    memcpy(data + len - ret, buf, ret);
  }
  *len_p = len;
  *size_p = size;
  return data;
}

static char *
read_from_fd(int fd, size_t *len_p, size_t *size_p) {
  struct stat st;
  void *data = NULL;
  size_t sz;

  if (fstat(fd, &st) != 0) DIE("cannot stat fd <%d>", fd);

  if (st.st_size > INT_MAX && sizeof(size_t) == 4)
    DIE("file size %llu requires 64-bit version of bmzip",
        (Llu)st.st_size);

  sz = *len_p = *size_p = st.st_size;

  if (!sz) return data;

  if (!s_no_mmap) {
#ifndef HT_NO_MMAP
    LOG(1, "mmapping file (size: %lu)...", (Lu)sz);
    data = mmap(NULL, sz, PROT_READ, MAP_PRIVATE, fd, 0);

    if (!data || (void *)-1 == data) {
      LOG(1, "mmap failed on fd %d", fd);
      errno = 0;
      LOG(1, "%s", "trying alternative");
      data = NULL;
    }
#endif
  }
  if (!data) {
    LOG(1, "reading file (size: %lu) into memory...", (Lu)sz);
    data = malloc(sz);

    if (!data) DIE("cannot allocate %lu bytes memory", (Lu)sz);

    if (read(fd, data, sz) != sz) DIE("error reading %lu bytes", (Lu)sz);
  }

  return data;
}

static void
input_from_stdin(size_t offset, size_t fp_len, int action, int options) {
  size_t len, buf_len;

  if (action == BMZ_A_LIST) {
    do_list(0);
  }
  else {
    void *data = read_from_fp(stdin, &len, &buf_len);
    do_block(data, len, buf_len, offset, fp_len, action, options);
  }
}

static void
input_from_file(const char *fname, size_t offset, size_t fp_len, int action,
                int options) {
  size_t len, buf_len;
  int fd = open(fname, O_RDONLY, 0);

  if (fd == -1) DIE("cannot open '%s'", fname);

  if (action == BMZ_A_LIST) {
    do_list(fd);
  }
  else {
    void *data = read_from_fd(fd, &len, &buf_len);
    do_block(data, len, buf_len, offset, fp_len, action, options);
  }
  /* close and free etc. are omitted intentionally */
}

static int
bm_hash(const char *name) {

  if (!strcmp("mod", name))             return BMZ_HASH_MOD;
  else if (!strcmp("mod16x2", name))    return BMZ_HASH_MOD16X2;
  else if (!strcmp("mask16x2", name))   return BMZ_HASH_MASK16X2;
  else if (!strcmp("mask", name))       return BMZ_HASH_MASK;
  else if (!strcmp("mask32x2", name))   return BMZ_HASH_MASK32X2;

  DIE("unknown hash: %s", name);
  return 0;
}

static void HT_NORETURN
show_usage() {
  fprintf(stderr, "%s%s", /* c89 string literal limit is 509 */
    "usage: bmzip [options] [<file>]\n"
    "-d, --decompress         decompress to stdout\n"
    "--verbose[=level]        show some diagnostic messages\n"
    "-l, --list               list compressed file info\n"
    "-h, --help               show this message\n"
    "--offset    <number>     expert: bm encoding start offset\n"
    "--fp-len    <number>     expert: bm encoding fingerprint size\n"
    "--bm-thresh <number>     expert: bm hash collision threshold\n",
    "--bm-hash   <name>       expert: use <name> as bm hash\n"
    "--bm-only                expert: skip lz compression\n"
    "--bm-dump                expert: dump human readable bm encoding\n"
    "--no-mmap                expert: do not use mmap\n");
  exit(0);
}

int
main(int ac, char *av[]) {
  char **ia = av + 1, **a_end = av + ac;
  /* defaults */
  size_t fp_len = 64, offset = 0;
  int bm_thresh = 0, action = BMZ_A_PACK, options = 0;

  for (; ia < a_end; ++ia) {
    if (!strcmp("-d", *ia) ||
        !strcmp("--decompress", *ia))           action = BMZ_A_UNPACK;
    else if (!strcmp("--verbose", *ia))         s_verbosity = 1;
    else if (!strcmp("--verbose=", *ia))        s_verbosity = atoi(*ia + 9);
    else if (!strcmp("--offset", *ia))          offset = atoi(*++ia);
    else if (!strcmp("--fp-len", *ia))          fp_len = atoi(*++ia);
    else if (!strcmp("--bm-only", *ia))         options |= BMZ_O_BM_ONLY;
    else if (!strcmp("--bm-dump", *ia))         s_bm_dump = 1;
    else if (!strcmp("--no-mmap", *ia))         s_no_mmap = 1;
    else if (!strcmp("--bm-thresh", *ia))       bm_thresh = atoi(*++ia);
    else if (!strcmp("--bm-hash", *ia))         s_bm_hash = bm_hash(*++ia);
    else if (!strcmp("-l", *ia) ||
             !strcmp("--list", *ia))            action = BMZ_A_LIST;
    else if (!strcmp("-h", *ia) ||
             !strcmp("--help", *ia)) {
      show_usage();
    }
    else if (!strcmp("--version", *ia)) {
      LOG(0, "version %d.%d.%d.%d", BMZIP_VER >> 12, (BMZIP_VER >> 8) & 0xf,
          (BMZIP_VER >> 4) & 0xf, BMZIP_VER & 0xf);
      exit(0);
    }
    else if (!strcmp("--", *ia)) {
      ++ia;
      break;
    }
    else if ('-' == **ia)
      DIE("unknown option: %s\n", *ia);
    else break;
  }
  if (s_verbosity)
    bmz_set_verbosity(s_verbosity);

  if (bm_thresh)
    bmz_set_collision_thresh(bm_thresh);

  if (ia >= a_end)
    input_from_stdin(offset, fp_len, action, options);
  else
    input_from_file(*ia, offset, fp_len, action, options);

  return 0;
}
