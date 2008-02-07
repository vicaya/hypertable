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

/**
 * cf. Bentley & McIlroy, "Data Compression Using Long Common Strings", 1999
 */

/* Avoid platform specific stuff in this file */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include "bmz-internal.h"
#include "../lzo/minilzo.h"

/* Initial bases for computing Rabin Karp rolling hash */
#define BM_B1 257
#define BM_B2 277
#define BM_M1 0xffff
#define BM_M2 (0xffff - 4)

#define BM_MASK16 0xffff
#define BM_MASK24 0xffffff
#define BM_MASK32 0xffffffff
#define BM_MASKSZ BM_MASK32

/* Escape character
 * Don't change without understanding BM_DECODE_POS
 */
#define BM_ESC 0xfe
#define BM_MAX_LEN 0xfdffffffffffull /* 253TB ought to be enough for any one! */
#define BM_MAX_1B  0xfd
#define BM_MAX_2B  0xfdff
#define BM_MAX_3B  0xfdffff
#define BM_MAX_4B  0xfdfffffful
#define BM_MAX_5B  0xfdffffffffull

/* VInt limits */
#define BM_MAX_V1B 0x7f
#define BM_MAX_V2B 0x3fff
#define BM_MAX_V3B 0x1fffff
#define BM_MAX_V4B 0xfffffff
#define BM_MAX_V5B 0x7ffffffffull
#define BM_MAX_V6B 0x3ffffffffffull

/* Rolling hash collision thresholds */
#define BM_COLLISION_ABORT_THRESH 1
#define BM_ABORT_COLLISIONS \
  ((BM_COLLISION_ABORT_THRESH + 1) * collision_thresh + 1)

/* Some colors for debugging/dump output */
#define BM_COLOR_DBG "\x1b[31m"         /* red */
#define BM_COLOR_ALT "\x1b[32m"         /* green */
#define BM_COLOR_END "\x1b[m"

/* May need to do something here, in case stdint.h is not available */
typedef uint8_t Byte;
typedef uint32_t UInt32;
typedef uint64_t UInt64;
typedef int64_t Int64;
typedef int32_t Int32;

/* For printf %llu in case some system has funky headers */
typedef long long unsigned Llu;

/* For lookup table */
#define BM_NPOS ((size_t)-1)

/* Aligning memory for work space */
#define BM_ALIGN(_mem_, _n_) (Byte *)(_mem_) + _n_ - (((size_t)(_mem_))%(_n_))

/* Some prime numbers as candidates for RK hash bases in case of adaptation */
static size_t s_primes[] = {
           3,    5,    7,   11,   13,   17,   19,   23,   29,
    31,   37,   41,   43,   47,   53,   59,   61,   67,   71,
    73,   79,   83,   89,   97,  101,  103,  107,  109,  113,
   127,  131,  137,  139,  149,  151,  157,  163,  167,  173,
   179,  181,  191,  193,  197,  199,  211,  223,  227,  229,
   233,  239,  241,  251,  257,  263,  269,  271,  277,  281,
   283,  293,  307,  311,  313,  317,  331,  337,  347,  349,
   353,  359,  367,  373,  379,  383,  389,  397,  401,  409,
   419,  421,  431,  433,  439,  443,  449,  457,  461,  463,
   467,  479,  487,  491,  499,  503,  509,  521,  523,  541,
   547,  557,  563,  569,  571,  577,  587,  593,  599,  601,
   607,  613,  617,  619,  631,  641,  643,  647,  653,  659,
   661,  673,  677,  683,  691,  701,  709,  719,  727,  733,
   739,  743,  751,  757,  761,  769,  773,  787,  797,  809,
   811,  821,  823,  827,  829,  839,  853,  857,  859,  863,
   877,  881,  883,  887,  907,  911,  919,  929,  937,  941,
   947,  953,  967,  971,  977,  983,  991,  997, 1009, 1013,
  1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069,
  1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151,
  1153, 1163, 1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223,
  1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291,
  1297, 1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373,
  1381, 1399, 1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451,
  1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499, 1511,
  1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583,
  1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657,
  1663, 1667, 1669, 1693, 1697, 1699, 1709, 1721, 1723, 1733,
  1741, 1747, 1753, 1759, 1777, 1783, 1787, 1789, 1801, 1811,
  1823, 1831, 1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889,
  1901, 1907, 1913, 1931, 1933, 1949, 1951, 1973, 1979, 1987,
  1993, 1997, 1999, 2003, 2011, 2017, 2027, 2029, 2039, 2053,
  2063, 2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129,
  2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207, 2213,
  2221, 2237, 2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287,
  2293, 2297, 2309, 2311, 2333, 2339, 2341, 2347, 2351, 2357,
  2371, 2377, 2381, 2383, 2389, 2393, 2399, 2411, 2417, 2423,
  2437, 2441, 2447, 2459, 2467, 2473, 2477, 2503, 2521, 2531,
  2539, 2543, 2549, 2551, 2557, 2579, 2591, 2593, 2609, 2617,
  2621, 2633, 2647, 2657, 2659, 2663, 2671, 2677, 2683, 2687,
  2689, 2693, 2699, 2707, 2711, 2713, 2719, 2729, 2731, 2741,
  2749, 2753, 2767, 2777, 2789, 2791, 2797, 2801, 2803, 2819,
  2833, 2837, 2843, 2851, 2857, 2861, 2879, 2887, 2897, 2903,
  2909, 2917, 2927, 2939, 2953, 2957, 2963, 2969, 2971, 2999,
  3001, 3011, 3019, 3023, 3037, 3041, 3049, 3061, 3067, 3079,
  3083, 3089, 3109, 3119, 3121, 3137, 3163, 3167, 3169, 3181,
  3187, 3191, 3203, 3209, 3217, 3221, 3229, 3251, 3253, 3257,
  3259, 3271, 3299, 3301, 3307, 3313, 3319, 3323, 3329, 3331,
  3343, 3347, 3359, 3361, 3371, 3373, 3389, 3391, 3407, 3413,
  3433, 3449, 3457, 3461, 3463, 3467, 3469, 3491, 3499, 3511,
  3517, 3527, 3529, 3533, 3539, 3541, 3547, 3557, 3559, 3571,
  3581, 3583, 3593, 3607, 3613, 3617, 3623, 3631, 3637, 3643,
  3659, 3671, 3673, 3677, 3691, 3697, 3701, 3709, 3719, 3727,
  3733, 3739, 3761, 3767, 3769, 3779, 3793, 3797, 3803, 3821,
  3823, 3833, 3847, 3851, 3853, 3863, 3877, 3881, 3889, 3907,
  3911, 3917, 3919, 3923, 3929, 3931, 3943, 3947, 3967, 3989,
  4001, 4003, 4007, 4013, 4019, 4021, 4027, 4049, 4051, 4057,
  4073, 4079, 4091, 4093, 4099, 4111, 4127, 4129, 4133, 4139,
  4153, 4157, 4159, 4177, 4201, 4211, 4217, 4219, 4229, 4231,
  4241, 4243, 4253, 4259, 4261, 4271, 4273, 4283, 4289, 4297,
  4327, 4337, 4339, 4349, 4357, 4363, 4373, 4391, 4397, 4409,
  4421, 4423, 4441, 4447, 4451, 4457, 4463, 4481, 4483, 4493,
  4507, 4513, 4517, 4519, 4523, 4547, 4549, 4561, 4567, 4583,
  4591, 4597, 4603, 4621, 4637, 4639, 4643, 4649, 4651, 4657,
  4663, 4673, 4679, 4691, 4703, 4721, 4723, 4729, 4733, 4751,
  4759, 4783, 4787, 4789, 4793, 4799, 4801, 4813, 4817, 4831,
  4861, 4871, 4877, 4889, 4903, 4909, 4919, 4931, 4933, 4937,
  4943, 4951, 4957, 4967, 4969, 4973, 4987, 4993, 4999, 5003,
  5009, 5011, 5021, 5023, 5039, 5051, 5059, 5077, 5081, 5087,
  5099, 5101, 5107, 5113, 5119, 5147, 5153, 5167, 5171, 5179,
  5189, 5197, 5209, 5227, 5231, 5233, 5237, 5261, 5273, 5279,
  5281, 5297, 5303, 5309, 5323, 5333, 5347, 5351, 5381, 5387,
  5393, 5399, 5407, 5413, 5417, 5419, 5431, 5437, 5441, 5443,
  5449, 5471, 5477, 5479, 5483, 5501, 5503, 5507, 5519, 5521,
  5527, 5531, 5557, 5563, 5569, 5573, 5581, 5591, 5623, 5639,
  5641, 5647, 5651, 5653, 5657, 5659, 5669, 5683, 5689, 5693,
  5701, 5711, 5717, 5737, 5741, 5743, 5749, 5779, 5783, 5791,
  5801, 5807, 5813, 5821, 5827, 5839, 5843, 5849, 5851, 5857,
  5861, 5867, 5869, 5879, 5881, 5897, 5903, 5923, 5927, 5939,
  5953, 5981, 5987, 6007, 6011, 6029, 6037, 6043, 6047, 6053,
  6067, 6073, 6079, 6089, 6091, 6101, 6113, 6121, 6131, 6133,
  6143, 6151, 6163, 6173, 6197, 6199, 6203, 6211, 6217, 6221,
  6229, 6247, 6257, 6263, 6269, 6271, 6277, 6287, 6299, 6301,
  6311, 6317, 6323, 6329, 6337, 6343, 6353, 6359, 6361, 6367,
  6373, 6379, 6389, 6397, 6421, 6427, 6449, 6451, 6469, 6473,
  6481, 6491, 6521, 6529, 6547, 6551, 6553, 6563, 6569, 6571,
  6577, 6581, 6599, 6607, 6619, 6637, 6653, 6659, 6661, 6673,
  6679, 6689, 6691, 6701, 6703, 6709, 6719, 6733, 6737, 6761,
  6763, 6779, 6781, 6791, 6793, 6803, 6823, 6827, 6829, 6833,
  6841, 6857, 6863, 6869, 6871, 6883, 6899, 6907, 6911, 6917,
  6947, 6949, 6959, 6961, 6967, 6971, 6977, 6983, 6991, 6997,
  7001, 7013, 7019, 7027, 7039, 7043, 7057, 7069, 7079, 7103,
  7109, 7121, 7127, 7129, 7151, 7159, 7177, 7187, 7193, 7207,
  7211, 7213, 7219, 7229, 7237, 7243, 7247, 7253, 7283, 7297,
  7307, 7309, 7321, 7331, 7333, 7349, 7351, 7369, 7393, 7411,
  7417, 7433, 7451, 7457, 7459, 7477, 7481, 7487, 7489, 7499,
  7507, 7517, 7523, 7529, 7537, 7541, 7547, 7549, 7559, 7561,
  7573, 7577, 7583, 7589, 7591, 7603, 7607, 7621, 7639, 7643,
  7649, 7669, 7673, 7681, 7687, 7691, 7699, 7703, 7717, 7723,
  7727, 7741, 7753, 7757, 7759, 7789, 7793, 7817, 7823, 7829,
  7841, 7853, 7867, 7873, 7877, 7879, 7883, 7901, 7907, 7919,
  7927, 7933, 7937, 7949, 7951, 7963, 7993, 8009, 8011, 8017
};

/* Hash collision adaption threshold */
static size_t s_collision_thresh = 0;

/* Logging/messaging facilities */
static int s_verbosity = 0;

static void
builtin_out(const char *buf, size_t len) {
  fwrite(buf, 1, len, stderr);
}

static HT_NORETURN void
builtin_die(const char *msg) {
  fputs(msg, stderr);
  exit(1);
}

static BmzOutProc s_out_proc = builtin_out;
static BmzDieProc s_die_proc = builtin_die;

#define BM_LOG(_level_, _fmt_, ...) if (s_verbosity >= _level_) do { \
  char msg[256]; \
  int len = snprintf(msg, sizeof(msg), "%s: " _fmt_, __FUNCTION__, \
                     ##__VA_ARGS__); \
  s_out_proc(msg, len); \
} while (0)

#define BM_LOG_(_level_, _buf_, _len_) if (s_verbosity >= _level_) do { \
  s_out_proc((char *)_buf_, _len_); \
} while (0)

#define BM_DIE(_fmt_, ...) do { \
  char msg[256]; \
  snprintf(msg, sizeof(msg), "fatal: %s: " _fmt_ "\n", \
           __FUNCTION__, ##__VA_ARGS__); \
  s_die_proc(msg); \
} while (0)

#define BM_CHECK(_cond_) \
    if (_cond_); else BM_DIE("%s:%d: expects: %s", \
                             __FILE__, __LINE__, #_cond_)

int
bmz_set_verbosity(int verbosity) {
  int old = s_verbosity;
  s_verbosity = verbosity;
  return old;
}

BmzOutProc
bmz_set_out_proc(BmzOutProc proc) {
  BmzOutProc old = s_out_proc;
  s_out_proc = proc;
  return old;
}

int
bmz_set_collision_thresh(int thresh) {
  int old = s_collision_thresh;
  s_collision_thresh = thresh;
  return old;
}

/* Pick one or two primes out of 1009
 * used for adaptive hashing in face of bad data
 */
static size_t
random_prime(size_t excluded) {
  const size_t n = sizeof(s_primes) / sizeof(size_t);
  size_t i = 0;
   /* Don't really care if rand/srand are not thread-safe for
    * predictable results, as long as it doesn't crash.
    */
  static int sranded = 0;

  if (!sranded) {
    srand(time(NULL));
    sranded = 1;
  }

  for (; i < n; ++i) {
    /* random() in os x and bsd is better, but not in glibc yet. */
    size_t s = s_primes[rand() % n];
    if (s != excluded) return s;
  }
  /* Safety net, though highly unlikely (1/1009^1009) to reach here */
  return 8111;
}

static size_t
bm_lut_size(size_t n) {
  UInt64 x = n; /* size_t could be 64-bit */

  do {
    /* power of 2 for mask mod */
    x |= (x >> 1);
    x |= (x >> 2);
    x |= (x >> 4);
    x |= (x >> 8);
    x |= (x >> 16);
    x |= (x >> 32);
    ++x;
  } while (x < n * 8 / 5); /* avoid too much probes */
  return x;
}

typedef struct {
  size_t hash;
  size_t pos;
} BmLutEntry;

typedef struct {
  size_t size, collisions, adapts, probes;
  BmLutEntry *entries;
} BmLut;

static void
init_bmlut(BmLut *table, size_t n, void *work_mem) {
  BmLutEntry *p, *endp;

  table->probes = table->adapts = table->collisions = 0;
  table->size = bm_lut_size(n);
  table->entries = (BmLutEntry *) work_mem;

  for (p = table->entries, endp = p + table->size; p < endp; ++p)
    p->pos = BM_NPOS;

  BM_LOG(1, "n(fps): %llu, size: %llu\n", (Llu)n, (Llu)table->size);
}

/* Simple hash lookup with probing */
#define BM_LOOKUP(_lut_, _hash_, _entry_) do { \
  size_t sz = _lut_.size, idx = (_hash_) & (sz - 1), probes = 0; \
  BmLutEntry *b = _lut_.entries, *p = b + idx, *endp = b + sz; \
  \
  while (p->pos != BM_NPOS) { \
    if (p->hash == _hash_) break; \
    ++p; \
    if (p >= endp) p = b; \
    ++probes; \
  } \
  if (probes) _lut_.probes += probes; \
  _entry_ = p; \
} while (0)

/* Classical/generic Rabin Karp */
#define R_HASH_MOD_BODY(_int_type_) \
  _int_type_ h = 0, p = 1; \
  \
  while (len--) { \
    h += p * buf[len]; \
    h %= m; \
    p *= b; \
    p %= m; \
  } \
  return h

static UInt64
r_hash_mod(const Byte *buf, size_t len, size_t b, UInt64 m) {
  R_HASH_MOD_BODY(UInt64);
}

static UInt32
r_hash_mod32(const Byte *buf, size_t len, UInt32 b, UInt32 m) {
  R_HASH_MOD_BODY(UInt32);
}

/* D* rolling hash family (Andrew Trigdell's thesis p72-73) */
static UInt32
r_hash_mod16x2(const Byte *buf, size_t len, UInt32 b1, UInt32 b2,
               UInt32 m1, UInt32 m2) {
  return (r_hash_mod32(buf, len, b1, m1) << 16) |
         r_hash_mod32(buf, len, b2, m2);
}

/* Compute (x^n) % m */
#define POW_MOD_BODY(_pow_fun_, _int_type_) \
  if (n == 0) return 1; \
  if (n == 1) return x % m; \
  { \
    _int_type_ sqr = (x * x) % m; \
    _int_type_ pow = _pow_fun_(sqr, n / 2, m); \
    if (n & 1) return (pow * x) % m; \
    return pow % m; \
  }

static UInt64
pow_mod(UInt64 x, UInt64 n, UInt64 m) {
  POW_MOD_BODY(pow_mod, UInt64)
}

static UInt32
pow_mod32(UInt32 x, UInt32 n, UInt32 m) {
  POW_MOD_BODY(pow_mod32, UInt32)
}

/* update rolling hash */
#define UPDATE_HASH_MOD_BODY \
  h *= b; \
  h -= (out * pow_n) % m; \
  h += in; \
  if (h < 0) h += m; \
  return (h % m)

static inline Int64
update_hash_mod(Int64 h, Byte in, Byte out, Int64 pow_n, Int64 b, Int64 m) {
  UPDATE_HASH_MOD_BODY;
}

static inline Int32
update_hash_mod32(Int32 h, Byte in, Byte out, Int32 pow_n, Int32 b, Int32 m) {
  UPDATE_HASH_MOD_BODY;
}

static inline UInt32
update_hash_mod16x2(UInt32 h, int in, int out, UInt32 pow1, UInt32 pow2,
                    UInt32 b1, UInt32 b2, UInt32 m1, UInt32 m2) {
  return (update_hash_mod32((h >> 16), in, out, pow1, b1, m1) << 16) |
         update_hash_mod32((h & BM_MASK16), in, out, pow2, b2, m2);
}

/* Faster hash using mask instead of mod 
 * m needs to be power-of-2
 */
#define R_HASH_MASK_BODY(int_type) \
  int_type h = 0, p = 1; \
  \
  while (len--) { \
    h += p * buf[len]; \
    h &= m; \
    p *= b; \
    p &= m; \
  } \
  return h

static UInt64
r_hash_mask(const Byte *buf, size_t len, UInt64 b, UInt64 m) {
  R_HASH_MASK_BODY(UInt64);
}

static UInt32
r_hash_mask32(const Byte *buf, size_t len, UInt32 b, UInt32 m) {
  R_HASH_MASK_BODY(UInt32);
}

/* C* rolling hash family (see D* above) */
static UInt32
r_hash_mask16x2(const Byte *buf, size_t len, UInt32 b1, UInt32 b2) {
  return (r_hash_mask32(buf, len, b1, BM_MASK16) << 16) |
         r_hash_mask32(buf, len, b2, BM_MASK16);
}

static UInt64
r_hash_mask32x2(const Byte *buf, size_t len, UInt64 b1, UInt64 b2) {
  return (r_hash_mask(buf, len, b1, BM_MASK32) << 32) |
         r_hash_mask(buf, len, b2, BM_MASK32);
}

#define POW_MASK_BODY(_pow_fun_, _int_type_) \
  if (n == 0) return 1; \
  if (n == 1) return (x & m); \
  { \
    _int_type_ sqr = (x * x) & m; \
    _int_type_ pow = _pow_fun_(sqr, n / 2, m); \
    if (n & 1) return ((pow * x) & m); \
    return (pow & m); \
  }

static UInt64
pow_mask(UInt64 x, UInt64 n, UInt64 m) {
  POW_MASK_BODY(pow_mask, UInt64)
}

static UInt32
pow_mask32(UInt32 x, UInt32 n, UInt32 m) {
  POW_MASK_BODY(pow_mask32, UInt32)
}

#define UPDATE_HASH_MASK_BODY \
  h *= b; \
  h -= (out * pow_n) & m; \
  h += in; \
  return (h & m)

static inline UInt64
update_hash_mask(UInt64 h, int in, int out, UInt64 pow_n, UInt64 b, UInt64 m) {
  UPDATE_HASH_MASK_BODY;
}

static inline UInt32
update_hash_mask32(UInt32 h, int in, int out, UInt32 pow_n,
                   UInt32 b, UInt32 m) {
  UPDATE_HASH_MASK_BODY;
}

static inline UInt32
update_hash_mask16x2(UInt32 h, int in, int out, UInt32 pow1, UInt32 pow2,
                     UInt32 b1, UInt32 b2) {
  return (update_hash_mask32((h >> 16), in, out, pow1, b1, BM_MASK16) << 16) |
         update_hash_mask32((h & BM_MASK16), in, out, pow2, b2, BM_MASK16);
}

static inline UInt64
update_hash_mask32x2(UInt64 h, int in, int out, UInt64 pow1, UInt64 pow2,
                     UInt64 b1, UInt64 b2) {
  return (update_hash_mask((h >> 32), in, out, pow1, b1, BM_MASK32) << 32) |
         update_hash_mask((h & BM_MASK32), in, out, pow2, b2, BM_MASK32);
}

/* External AP Ifor computing the hashes */
size_t
bmz_hash_mod(const void *in, size_t in_len, size_t b, size_t m) {
  return r_hash_mod((Byte *)in, in_len, b, m);
}

size_t
bmz_hash_mod16x2(const void *in, size_t in_len, size_t b1, size_t b2,
                 size_t m1, size_t m2) {
  return r_hash_mod16x2((Byte *)in, in_len, b1, b2, m1, m2);
}

size_t
bmz_hash_mask16x2(const void *in, size_t in_len, size_t b1, size_t b2) {
  return r_hash_mask16x2((Byte *)in, in_len, b1, b2);
}

size_t
bmz_hash_mask(const void *in, size_t in_len, size_t b) {
  return r_hash_mask((Byte *)in, in_len, b, BM_MASKSZ);
}

size_t
bmz_hash_mask32x2(const void *in, size_t in_len, size_t b1, size_t b2) {
  return r_hash_mask32x2((Byte *)in, in_len, b1, b2);
}

/* Verify the rolling hashes */
#define BM_HASH_CHECK_BODY(_init_hash_, _rehash_, _update_hash_) \
  size_t hash; \
  Byte *in = (Byte *)src, *ip = in, *in_end = in + in_len; \
  int errs = 0; \
  \
  BM_CHECK(fp_len < in_len); \
  _init_hash_; \
  \
  for (; ip < in_end - fp_len - 1; ++ip) { \
    size_t h0 = _rehash_; \
    hash = _update_hash_; \
    if (h0 != hash) { \
      BM_LOG(0, "mismatch at pos %ld: %lx vs %lx\n", \
             (long)(ip - in), (long)h0, (long)hash); \
      ++errs; \
    } \
  } \
  BM_LOG(1, "total errors: %d\n", errs); \
  return errs ? BMZ_E_ERROR : BMZ_E_OK

#define BM_HASH_BENCH_BODY(_init_hash_, _update_hash_) \
  size_t hash; \
  Byte *in = (Byte *)src, *ip = in, *in_end = in + in_len; \
  \
  BM_CHECK(fp_len < in_len); \
  _init_hash_; \
  \
  for (; ip < in_end - fp_len - 1; ++ip) { \
    hash = _update_hash_; \
  } \
  BM_LOG(1, "last hash: %lx\n", (long)hash) /* or the loop gets optimized out */

int
bmz_check_hash_mod(const void *src, size_t in_len, size_t fp_len,
                   size_t b, size_t m) {
  size_t pow_n;

  BM_HASH_CHECK_BODY(hash = r_hash_mod(in, fp_len, b, m);
                     pow_n = pow_mod(b, fp_len, m),
                     r_hash_mod(ip + 1, fp_len, b, m),
                     update_hash_mod(hash, ip[fp_len], *ip, pow_n, b, m));
}

void
bmz_bench_hash_mod(const void *src, size_t in_len, size_t fp_len) {
  UInt64 b = BM_B1, m = BM_MASKSZ;
  size_t pow_n;

  BM_HASH_BENCH_BODY(hash = r_hash_mod(in, fp_len, b, m);
                     pow_n = pow_mod(b, fp_len, m),
                     update_hash_mod(hash, ip[fp_len], *ip, pow_n, b, m));
}

int
bmz_check_hash_mod16x2(const void *src, size_t in_len, size_t fp_len,
                       size_t b1, size_t b2, size_t m1, size_t m2) {
  size_t pow_n_b1, pow_n_b2;

  BM_HASH_CHECK_BODY(hash = r_hash_mod16x2(in, fp_len, b1, b2, m1, m2);
                     pow_n_b1 = pow_mod32(b1, fp_len, m1);
                     pow_n_b2 = pow_mod32(b2, fp_len, m2),
                     r_hash_mod16x2(ip + 1, fp_len, b1, b2, m1, m2),
                     update_hash_mod16x2(hash, ip[fp_len], *ip, pow_n_b1,
                                         pow_n_b2, b1, b2, m1, m2));
}

void
bmz_bench_hash_mod16x2(const void *src, size_t in_len, size_t fp_len) {
  size_t pow_n_b1, pow_n_b2, b1 = BM_B1, b2 = BM_B2, m1 = BM_M1, m2 = BM_M2;

  BM_HASH_BENCH_BODY(hash = r_hash_mod16x2(in, fp_len, b1, b2, m1, m2);
                     pow_n_b1 = pow_mod32(b1, fp_len, m1);
                     pow_n_b2 = pow_mod32(b2, fp_len, m2),
                     update_hash_mod16x2(hash, ip[fp_len], *ip, pow_n_b1,
                                         pow_n_b2, b1, b2, m1, m2));
}

int
bmz_check_hash_mask16x2(const void *src, size_t in_len, size_t fp_len,
                        size_t b1, size_t b2) {
  size_t pow_n_b1, pow_n_b2;

  BM_HASH_CHECK_BODY(hash = r_hash_mask16x2(in, fp_len, b1, b2);
                     pow_n_b1 = pow_mask32(b1, fp_len, BM_MASK16);
                     pow_n_b2 = pow_mask32(b2, fp_len, BM_MASK16),
                     r_hash_mask16x2(ip + 1, fp_len, b1, b2),
                     update_hash_mask16x2(hash, ip[fp_len], *ip, pow_n_b1,
                                          pow_n_b2, b1, b2));
}

void
bmz_bench_hash_mask16x2(const void *src, size_t in_len, size_t fp_len) {
  size_t pow_n_b1, pow_n_b2, b1 = BM_B1, b2 = BM_B2;

  BM_HASH_BENCH_BODY(hash = r_hash_mask16x2(in, fp_len, b1, b2);
                     pow_n_b1 = pow_mask32(b1, fp_len, BM_MASK16);
                     pow_n_b2 = pow_mask32(b2, fp_len, BM_MASK16),
                     update_hash_mask16x2(hash, ip[fp_len], *ip, pow_n_b1,
                                          pow_n_b2, b1, b2));
}

int
bmz_check_hash_mask(const void *src, size_t in_len, size_t fp_len,
                    size_t b) {
  size_t pow_n, m = BM_MASKSZ;

  BM_HASH_CHECK_BODY(hash = r_hash_mask(in, fp_len, b, m);
                     pow_n = pow_mask(b, fp_len, m),
                     r_hash_mask(ip + 1, fp_len, b, m),
                     update_hash_mask(hash, ip[fp_len], *ip, pow_n, b, m));
}

void
bmz_bench_hash_mask(const void *src, size_t in_len, size_t fp_len) {
  size_t pow_n, b = BM_B1, m = BM_MASKSZ;

  BM_HASH_BENCH_BODY(hash = r_hash_mask(in, fp_len, b, m);
                     pow_n = pow_mask(b, fp_len, m),
                     update_hash_mask(hash, *ip, ip[fp_len], pow_n, b, m));
}

int
bmz_check_hash_mask32x2(const void *src, size_t in_len, size_t fp_len,
                        size_t b1, size_t b2) {
  size_t pow_n_b1, pow_n_b2;

  BM_HASH_CHECK_BODY(hash = r_hash_mask32x2(in, fp_len, b1, b2);
                     pow_n_b1 = pow_mask(b1, fp_len, BM_MASK32);
                     pow_n_b2 = pow_mask(b2, fp_len, BM_MASK32),
                     r_hash_mask32x2(ip + 1, fp_len, b1, b2),
                     update_hash_mask32x2(hash, ip[fp_len], *ip, pow_n_b1,
                                          pow_n_b2, b1, b2));
}

void
bmz_bench_hash_mask32x2(const void *src, size_t in_len, size_t fp_len) {
  size_t pow_n_b1, pow_n_b2, b1 = BM_B1, b2 = BM_B2;

  BM_HASH_BENCH_BODY(hash = r_hash_mask32x2(in, fp_len, b1, b2);
                     pow_n_b1 = pow_mask(b1, fp_len, BM_MASK32);
                     pow_n_b2 = pow_mask(b2, fp_len, BM_MASK32),
                     update_hash_mask32x2(hash, ip[fp_len], *ip, pow_n_b1,
                                          pow_n_b2, b1, b2));
}

void
bmz_bench_hash(const void *in, size_t in_len, unsigned type) {
  size_t fp_len = 100;

  switch (type) {
  case BMZ_HASH_MOD:
    bmz_bench_hash_mod(in, in_len, fp_len);
    break;
  case BMZ_HASH_MOD16X2:
    bmz_bench_hash_mod16x2(in, in_len, fp_len);
    break;
  case BMZ_HASH_MASK16X2:
    bmz_bench_hash_mask16x2(in, in_len, fp_len);
    break;
  case BMZ_HASH_MASK:
    bmz_bench_hash_mask(in, in_len, fp_len);
    break;
  case BMZ_HASH_MASK32X2:
    bmz_bench_hash_mask32x2(in, in_len, fp_len);
  }
}

/* Benchmark lookup table performance */
#define BM_LUT_BENCH_BODY(_init_hash_, _update_hash_) \
  size_t hash; \
  Byte *in = (Byte *)src, *ip = in, *in_end = in + in_len; \
  size_t scan_len = in_len - fp_len, c, boundary; \
  BmLut lut; \
  BmLutEntry *entry; \
  \
  BM_CHECK(fp_len < in_len); \
  init_bmlut(&lut, in_len / fp_len, work_mem); \
  _init_hash_; \
  \
  for (boundary = fp_len; ip < in_end - fp_len - 1; ++ip) { \
    size_t pos = ip - in; \
    BM_LOOKUP(lut, hash, entry); \
    \
    if (entry->pos != BM_NPOS) { \
      if (memcmp(in + entry->pos, ip, fp_len) != 0) ++lut.collisions; \
    } \
    if (pos == boundary) { \
      entry->hash = hash; \
      entry->pos = pos; \
      boundary += fp_len; \
    } \
    hash = _update_hash_; \
  } \
  c = lut.collisions; \
  BM_LOG(1, "length: %llu, lut.size: %llu, lut.collisions: %llu (eff. bits: " \
         "%.3f), lut.probes: %llu (%.3f/lu)\n", (Llu)in_len, (Llu)lut.size, \
         (Llu)c, c ? log((double)in_len / fp_len * scan_len / c) / log(2) \
                   : sizeof(size_t) * 8., \
         (Llu)lut.probes, (double)lut.probes / scan_len)

void
bmz_bench_lut_mod(const void *src, size_t in_len, size_t fp_len,
                  void *work_mem, size_t b, size_t m) {
  size_t pow_n;

  BM_LUT_BENCH_BODY(hash = r_hash_mod(in, fp_len, b, m);
                    pow_n = pow_mod(b, fp_len, m),
                    update_hash_mod(hash, ip[fp_len], *ip, pow_n, b, m));
}

void
bmz_bench_lut_mod16x2(const void *src, size_t in_len, size_t fp_len,
                      void *work_mem, size_t b1, size_t b2,
                      size_t m1, size_t m2) {
  size_t pow_n_b1, pow_n_b2;

  BM_LUT_BENCH_BODY(hash = r_hash_mod16x2(in, fp_len, b1, b2, m1, m2);
                    pow_n_b1 = pow_mod32(b1, fp_len, m1);
                    pow_n_b2 = pow_mod32(b2, fp_len, m2),
                    update_hash_mod16x2(hash, ip[fp_len], *ip, pow_n_b1,
                                        pow_n_b2, b1, b2, m1, m2));
}

void
bmz_bench_lut_mask16x2(const void *src, size_t in_len, size_t fp_len,
                   void *work_mem, size_t b1, size_t b2) {
  size_t pow_n_b1, pow_n_b2;

  BM_LUT_BENCH_BODY(hash = r_hash_mask16x2(in, fp_len, b1, b2);
                    pow_n_b1 = pow_mask32(b1, fp_len, BM_MASK16);
                    pow_n_b2 = pow_mask32(b2, fp_len, BM_MASK16),
                    update_hash_mask16x2(hash, ip[fp_len], *ip, pow_n_b1,
                                         pow_n_b2, b1, b2));
}

void
bmz_bench_lut_mask(const void *src, size_t in_len, size_t fp_len,
                   void *work_mem, size_t b) {
  size_t pow_n, m = BM_MASKSZ;

  BM_LUT_BENCH_BODY(hash = r_hash_mask(in, fp_len, b, m);
                    pow_n = pow_mask(b, fp_len, m),
                    update_hash_mask(hash, *ip, ip[fp_len], pow_n, b, m));
}

void
bmz_bench_lut_mask32x2(const void *src, size_t in_len, size_t fp_len,
                       void *work_mem, size_t b1, size_t b2) {
  size_t pow_n_b1, pow_n_b2;

  BM_LUT_BENCH_BODY(hash = r_hash_mask32x2(in, fp_len, b1, b2);
                    pow_n_b1 = pow_mask(b1, fp_len, BM_MASK32);
                    pow_n_b2 = pow_mask(b2, fp_len, BM_MASK32),
                    update_hash_mask32x2(hash, ip[fp_len], *ip, pow_n_b1,
                                         pow_n_b2, b1, b2));
}

/* One way to debug macro laden code:
 * gcc -E % | sed '/^\#/d' | gnuindent -st -i2 -kr > bmzpp.c
 */

#define BM_NEED_IN(_n_) \
  if ((int)(in_end - ip) < (int)(_n_)) goto input_overrun

#define BM_NEED_OUT(_n_) \
  if ((int)(out_end - op) < (int)(_n_)) goto output_overrun

#define BM_ENCODE_OUTPUT(_op_, _ip_, _ip_end_) \
  while (_ip_ < _ip_end_) { \
    if (*_ip_ == BM_ESC) { \
      BM_NEED_OUT(1); \
      *_op_++ = BM_ESC; \
    } \
    BM_NEED_OUT(1); \
    *_op_++ = *_ip_++; \
  }

/* Less comparison and earlier termination for uncompressible stuff */
#define BM_ENCODE_FINAL_OUTPUT(_op_, _ip_, _ip_end_) \
  BM_NEED_OUT((_ip_end_) - (_ip_)); /* at least */ \
  while (_ip_ < _ip_end_) { \
    if (*_ip_ == BM_ESC) { \
      BM_NEED_OUT((_ip_end_) - (_ip_) + 1); \
      *_op_++ = BM_ESC; \
    } \
    *_op_++ = *_ip_++; \
  }

#define BM_ENCODE_PASS do { \
  *out = 0; \
  memcpy(out + 1, in, in_len); \
  *out_len_p = in_len + 1; \
  BM_ENCODE_STAT("no reduction"); \
  return BMZ_E_OK; \
} while (0)

#define BM_ENCODE_INT(_op_, _n_, _width_) do { \
  UInt64 _n = _n_; \
  switch (_width_) { \
  case 1: \
    *_op_++ = (Byte) _n; \
    break; \
  case 2: \
    *_op_++ = (Byte)(_n >> 8); \
    *_op_++ = (Byte)(_n); \
    break; \
  case 3: \
    *_op_++ = (Byte)(_n >> 16); \
    *_op_++ = (Byte)(_n >> 8); \
    *_op_++ = (Byte)(_n); \
    break; \
  case 4: \
    *_op_++ = (Byte)(_n >> 24); \
    *_op_++ = (Byte)(_n >> 16); \
    *_op_++ = (Byte)(_n >> 8); \
    *_op_++ = (Byte)(_n); \
    break; \
  case 5: \
    *_op_++ = (Byte)(_n >> 32); \
    *_op_++ = (Byte)(_n >> 24); \
    *_op_++ = (Byte)(_n >> 16); \
    *_op_++ = (Byte)(_n >> 8); \
    *_op_++ = (Byte)(_n); \
    break; \
  case 6: \
    *_op_++ = (Byte)(_n >> 40); \
    *_op_++ = (Byte)(_n >> 32); \
    *_op_++ = (Byte)(_n >> 24); \
    *_op_++ = (Byte)(_n >> 16); \
    *_op_++ = (Byte)(_n >> 8); \
    *_op_++ = (Byte)(_n); \
    break; \
  default: \
    BM_DIE("%s", "256TB ought to be enough for anyone!"); \
  } \
} while (0)

#define BM_ENCODE_VINT(_op_, _n_, _width_) do { \
  UInt64 _n = _n_; \
  switch (_width_) { \
  case 1: \
    *_op_++ = (Byte)((_n) & 0x7f); \
    break; \
  case 2: \
    *_op_++ = (Byte)(_n | 0x80); \
    *_op_++ = (Byte)((_n >> 7) & 0x7f); \
    break; \
  case 3: \
    *_op_++ = (Byte)(_n | 0x80); \
    *_op_++ = (Byte)((_n >> 7) | 0x80); \
    *_op_++ = (Byte)((_n >> 14) & 0x7f); \
    break; \
  case 4: \
    *_op_++ = (Byte)(_n | 0x80); \
    *_op_++ = (Byte)((_n >> 7) | 0x80); \
    *_op_++ = (Byte)((_n >> 14) | 0x80); \
    *_op_++ = (Byte)((_n >> 21) & 0x7f); \
    break; \
  case 5: \
    *_op_++ = (Byte)(_n | 0x80); \
    *_op_++ = (Byte)((_n >> 7) | 0x80); \
    *_op_++ = (Byte)((_n >> 14) | 0x80); \
    *_op_++ = (Byte)((_n >> 21) | 0x80); \
    *_op_++ = (Byte)((_n >> 28) & 0x7f); \
    break; \
  case 6: \
    *_op_++ = (Byte)(_n | 0x80); \
    *_op_++ = (Byte)((_n >> 7) | 0x80); \
    *_op_++ = (Byte)((_n >> 14) | 0x80); \
    *_op_++ = (Byte)((_n >> 21) | 0x80); \
    *_op_++ = (Byte)((_n >> 28) | 0x80); \
    *_op_++ = (Byte)((_n >> 35) & 0x7f); \
    break; \
  case 7: \
    *_op_++ = (Byte)(_n | 0x80); \
    *_op_++ = (Byte)((_n >> 7) | 0x80); \
    *_op_++ = (Byte)((_n >> 14) | 0x80); \
    *_op_++ = (Byte)((_n >> 21) | 0x80); \
    *_op_++ = (Byte)((_n >> 28) | 0x80); \
    *_op_++ = (Byte)((_n >> 35) | 0x80); \
    *_op_++ = (Byte)((_n >> 42) & 0x7f); \
    break; \
  default: \
    BM_DIE("%s", "512TB ought to be enough for anyone!"); \
  } \
} while (0)

#define BM_DECODE_VINT(_n_) do { \
  BM_NEED_IN(1); \
  _n_ = (*ip++ & 0x7f); \
  if (ip[-1] & 0x80) { \
    BM_NEED_IN(1); \
    _n_ |= ((*ip++ & 0x7f) << 7); \
  } else break; \
  if (ip[-1] & 0x80) { \
    BM_NEED_IN(1); \
    _n_ |= ((*ip++ & 0x7f) << 14); \
  } else break; \
  if (ip[-1] & 0x80) { \
    BM_NEED_IN(1); \
    _n_ |= ((*ip++ & 0x7f) << 21); \
  } else break; \
  if (ip[-1] & 0x80) { \
    BM_NEED_IN(1); \
    _n_ |= ((UInt64)(*ip++ & 0x7f) << 28); \
  } else break; \
  if (ip[-1] & 0x80) { \
    BM_NEED_IN(1); \
    _n_ |= ((UInt64)(*ip++ & 0x7f) << 35); \
  } else break; \
  if (ip[-1] & 0x80) { \
    BM_NEED_IN(1); \
    _n_ |= ((UInt64)(*ip++ & 0x7f) << 42); \
  } \
} while (0)

#define BM_INT_WIDTH(_n_) \
  (_n_ > BM_MAX_5B ? 6 : \
   (_n_ > BM_MAX_4B ? 5 : \
    (_n_ > BM_MAX_3B ? 4 : \
     (_n_ > BM_MAX_2B ? 3 : \
      (_n_ > BM_MAX_1B ? 2 : 1)))))

#define BM_VINT_WIDTH(_n_) \
  (_n_ > BM_MAX_V6B ? 7 : \
   (_n_ > BM_MAX_V5B ? 6 : \
    (_n_ > BM_MAX_V4B ? 5 : \
     (_n_ > BM_MAX_V3B ? 4 : \
      (_n_ > BM_MAX_V2B ? 3 : \
       (_n_ > BM_MAX_V1B ? 2 : 1))))))

#define BM_ENCODE_DELTA(_op_, _ipos_, _pos_, _len_) do { \
  UInt64 _ipos = _ipos_, _len = _len_; \
  int pos_w = BM_INT_WIDTH(_ipos); \
  int len_w = BM_VINT_WIDTH(_len); \
  BM_NEED_OUT(pos_w + len_w + 1); \
  *_op_++ = BM_ESC; \
  BM_ENCODE_INT(_op_, _pos_, pos_w); \
  BM_ENCODE_VINT(_op_, _len_, len_w); \
} while (0)

#define BM_HANDLE_COLLISIONS(_randomize_, _init_hash_) do { \
  size_t adapts = lut.adapts, collisions = ++lut.collisions; \
  \
  if (adapts > BM_COLLISION_ABORT_THRESH) { \
    /* stumped & give up, just pass the bytes */ \
    BM_LOG(1, "too much collisions: %llu, giving up.\n", \
           (Llu)BM_ABORT_COLLISIONS); \
    goto output_overrun; \
  } \
  else if (collisions > collision_thresh) { \
    /* unlikely when the hash is good, so reinitialize hash, in case \
     * data is wacked/adversarial for some reasons. \
     */ \
    BM_LOG(2, "hash collision %llu: %llx: pos: %llu and %llu: [" BM_COLOR_DBG, \
           (Llu)lut.collisions, (Llu)hash, (Llu)pos, (Llu)i); \
    BM_LOG_(2, in + pos, fp_len); \
    BM_LOG(2, "%s", BM_COLOR_END "] vs [" BM_COLOR_ALT); \
    BM_LOG_(2, in + i, fp_len); \
    BM_LOG(2, "%s", BM_COLOR_END "] trying to adapt.\n"); \
    _randomize_; \
    boundary = last_i = i = offset; \
    _init_hash_; \
    init_bmlut(&lut, in_len / fp_len, work_mem); \
    lut.adapts = adapts + 1; \
    op = out + 1; \
    last_ip = in; \
    continue; \
  } \
} while (0)

#define BM_ENCODE_STAT(_msg_) do { \
  size_t c = lut.collisions; \
  BM_LOG(1, "%s: in: %llu, out: %llu, lut.collisions: %llu (eff. bits: %.3f) " \
         "thresh: %llu), lut.probes: %llu (%.3f/lu)\n", _msg_, (Llu)in_len, \
         (Llu)*out_len_p, (Llu)c, \
         c ? log((double)nfps * scan_len / c) / log(2) : sizeof(size_t) * 8., \
         (Llu)collision_thresh, (Llu)lut.probes, \
         (double)lut.probes / scan_len); \
} while (0)

/* The main algorithm */
#define BM_ENCODE_BODY(_init_hash_, _update_hash_, _randomize_) \
  size_t i = offset, last_i = i, end_i = in_len - fp_len; \
  size_t boundary = i, scan_len = end_i - offset; \
  size_t nfps, hash, pos, collision_thresh; \
  Byte *in = (Byte *) src, *out = (Byte *) dst; \
  Byte *ip = in, *last_ip = ip, *in_end = in + in_len; \
  Byte *op = out, *out_end; \
  BmLut lut = { 0, 0, 0, 0, NULL }; \
  BmLutEntry *entry = NULL; \
  \
  BM_CHECK(fp_len > 0); \
  nfps = in_len / fp_len; \
  collision_thresh = s_collision_thresh ? s_collision_thresh \
                                        : scan_len / 1e9 * nfps + 8; \
  if (in_len <= offset + fp_len) BM_ENCODE_PASS; \
  \
  init_bmlut(&lut, nfps, work_mem); \
  _init_hash_; \
  \
  *op++ = 1; /* default */ \
  out_end = op + (*out_len_p < in_len ? *out_len_p : in_len);  \
  \
  for (; i <= end_i; ++i) { \
    if (i > last_i) { \
      BM_LOOKUP(lut, hash, entry); \
      pos = entry->pos; \
      \
      if (pos != BM_NPOS) { \
        Byte *ip0 = in + pos, *cp = in + i, *cp0 = cp; \
        Byte *ip1 = ip0 + fp_len, *cp1 = cp0 + fp_len; \
        \
        if (memcmp(ip0, cp0, fp_len) == 0) { \
          /* we have a match */ \
          size_t mlen = 0; \
          /* greedy backward up to fp_len - 1 */ \
          for (ip = ip0; ip > in && cp > last_ip && \
               mlen < fp_len - 1 && *--ip == *--cp; ++mlen); \
          cp = cp0 - mlen; \
          BM_ENCODE_OUTPUT(op, last_ip, cp); \
          pos = ip0 - mlen - in; \
          mlen += fp_len; \
          /* greedy forward as much as possible */ \
          for (ip = ip1, cp = cp1; cp < in_end && *ip++ == *cp++; ++mlen); \
          BM_ENCODE_DELTA(op, last_ip - in, pos, mlen); \
          last_ip = cp0 - (ip0 - (in + pos)) + mlen; \
          if (last_ip == in_end) break; \
          last_i = last_ip - in; \
        } \
        else BM_HANDLE_COLLISIONS(_randomize_, _init_hash_); \
      } \
    } \
    if ((i - offset) == boundary) { \
      if (i <= last_i) BM_LOOKUP(lut, hash, entry); \
      /* insert hash */ \
      entry->hash = hash; \
      entry->pos = i; \
      boundary += fp_len; \
    } \
    if (i < end_i) hash = _update_hash_; /* for next iter */ \
  } \
  /* copy the remaining bytes */ \
  BM_ENCODE_FINAL_OUTPUT(op, last_ip, in_end); \
  *out_len_p = op - out; \
  BM_ENCODE_STAT("success"); \
  \
  return BMZ_E_OK; \
  \
output_overrun: \
  if (*out_len_p > in_len) BM_ENCODE_PASS; \
  BM_ENCODE_STAT("tip: make output buffer at least input length + 1"); \
  return BMZ_E_OUTPUT_OVERRUN

/* External APIs for bm encoding */
int
bmz_bm_pack_mod(const void *src, size_t in_len, void *dst, size_t *out_len_p,
                size_t offset, size_t fp_len, void *work_mem,
                size_t b, size_t m) {
  size_t pow_n;
  BM_ENCODE_BODY(hash = r_hash_mod(in + i, fp_len, b, m);
                 pow_n = pow_mod(b, fp_len, m),
                 update_hash_mod(hash, in[i + fp_len], in[i], pow_n, b, m),
                 b = random_prime(0));
}

int
bmz_bm_pack_mod16x2(const void *src, size_t in_len, void *dst,
                    size_t *out_len_p, size_t offset, size_t fp_len,
                    void *work_mem, size_t b1, size_t b2,
                    size_t m1, size_t m2) {
  size_t pow_n_b1, pow_n_b2;
  BM_ENCODE_BODY(hash = r_hash_mod16x2(in + i, fp_len, b1, b2, m1, m2);
                 pow_n_b1 = pow_mod32(b1, fp_len, m1);
                 pow_n_b2 = pow_mod32(b2, fp_len, m2),
                 update_hash_mod16x2(hash, in[i + fp_len], in[i],
                                     pow_n_b1, pow_n_b2, b1, b2, m1, m2),
                 b1 = random_prime(0);
                 b2 = random_prime(b1));
}

int
bmz_bm_pack_mask16x2(const void *src, size_t in_len, void *dst,
                     size_t *out_len_p, size_t offset, size_t fp_len,
                     void *work_mem, size_t b1, size_t b2) {
  size_t pow_n_b1, pow_n_b2;
  BM_ENCODE_BODY(hash = r_hash_mask16x2(in + i, fp_len, b1, b2);
                 pow_n_b1 = pow_mask32(b1, fp_len, BM_MASK16);
                 pow_n_b2 = pow_mask32(b2, fp_len, BM_MASK16),
                 update_hash_mask16x2(hash, in[i + fp_len], in[i],
                                      pow_n_b1, pow_n_b2, b1, b2),
                 b1 = random_prime(0);
                 b2 = random_prime(b1));
}

int
bmz_bm_pack_mask(const void *src, size_t in_len, void *dst, size_t *out_len_p,
                   size_t offset, size_t fp_len, void *work_mem, size_t b) {
  size_t pow_n;
  BM_ENCODE_BODY(hash = r_hash_mask(in + i, fp_len, b, BM_MASKSZ);
                 pow_n = pow_mask(b, fp_len, BM_MASKSZ),
                 update_hash_mask(hash, in[i + fp_len], in[i],
                                  pow_n, b, BM_MASKSZ),
                 b = random_prime(0));
}

int
bmz_bm_pack_mask32x2(const void *src, size_t in_len, void *dst,
                     size_t *out_len_p, size_t offset, size_t fp_len,
                     void *work_mem, size_t b1, size_t b2) {
  size_t pow_n_b1, pow_n_b2;
  BM_ENCODE_BODY(hash = r_hash_mask32x2(in + i, fp_len, b1, b2);
                 pow_n_b1 = pow_mask(b1, fp_len, BM_MASK32);
                 pow_n_b2 = pow_mask(b2, fp_len, BM_MASK32),
                 update_hash_mask32x2(hash, in[i + fp_len], in[i],
                                      pow_n_b1, pow_n_b2, b1, b2),
                 b1 = random_prime(0);
                 b2 = random_prime(b1));
}

/* Max lz overhead for incompressible block */
#define BM_LZ_MAX(_len_) ((_len_) / 16 + 64 + 3)

size_t
bmz_pack_buflen(size_t in_len) {
  return in_len + BM_LZ_MAX(in_len) + 1;
}



/* Workmem (lut) for bm pack */
size_t
bmz_bm_pack_worklen(size_t in_len, size_t fp_len) {
  return bm_lut_size(in_len / fp_len) * sizeof(BmLutEntry);
}

/* Size for compressor auxiliary memory */
static size_t
bmz_pack_auxlen(size_t in_len, size_t fp_len) {
  size_t lz_worklen = bmz_lz_pack_worklen(in_len) + 16;
  size_t bm_worklen = bmz_bm_pack_worklen(in_len, fp_len) + 16;
  return bm_worklen > lz_worklen ? bm_worklen : lz_worklen;
}

/* Including temporary space for bm output as well */
size_t
bmz_pack_worklen(size_t in_len, size_t fp_len) {
  BM_CHECK(fp_len > 0);
  return in_len + bmz_pack_auxlen(in_len, fp_len); 
}

size_t
bmz_unpack_worklen(size_t out_len) {
  return out_len + 1;
}

#define BMZ_PACK_BODY(_bm_pack_) \
  size_t tlen = in_len + 1; \
  Byte *dst = (Byte *)work_mem; \
  Byte *aux_mem = dst + tlen; \
  Byte *work_mem_aligned = BM_ALIGN(aux_mem, 8); \
  int ret; \
  \
  /* overlap flag assume the following memory layout    \
   * |=============== input/output =================|   \
   * |<------------- bmz_pack_buflen -------------->|   \
   * |<---------------- in_len ---------------->|       \
   * |<-- *out_len_p -->|                               \
   */                                                   \
  ret = _bm_pack_; \
  if (ret != BMZ_E_OK) return ret; \
  return bmz_lz_pack(dst, tlen, out, out_len_p, work_mem_aligned)

int
bmz_pack_mod(const void *in, size_t in_len, void *out, size_t *out_len_p,
             size_t offset, size_t fp_len, unsigned flags, void *work_mem,
             size_t b, size_t m) {
  BMZ_PACK_BODY(bmz_bm_pack_mod(in, in_len, dst, &tlen,
                offset, fp_len, work_mem_aligned, b, m));
}

int
bmz_pack_mod16x2(const void *in, size_t in_len, void *out, size_t *out_len_p,
                 size_t offset, size_t fp_len, unsigned flags, void *work_mem,
                 size_t b1, size_t b2, size_t m1, size_t m2) {
  BMZ_PACK_BODY(bmz_bm_pack_mod16x2(in, in_len, dst, &tlen,
                offset, fp_len, work_mem_aligned, b1, b2, m1, m2));
}

int
bmz_pack_mask16x2(const void *in, size_t in_len, void *out, size_t *out_len_p,
                  size_t offset, size_t fp_len, unsigned flags, void *work_mem,
                  size_t b1, size_t b2) {
  BMZ_PACK_BODY(bmz_bm_pack_mask16x2(in, in_len, dst, &tlen,
                offset, fp_len, work_mem_aligned, b1, b2));
}

int
bmz_pack_mask(const void *in, size_t in_len, void *out, size_t *out_len_p,
              size_t offset, size_t fp_len, unsigned flags, void *work_mem,
              size_t b) {
  BMZ_PACK_BODY(bmz_bm_pack_mask(in, in_len, dst, &tlen,
                offset, fp_len, work_mem_aligned, b));
}

int
bmz_pack_mask32x2(const void *in, size_t in_len, void *out, size_t *out_len_p,
                  size_t offset, size_t fp_len, unsigned flags, void *work_mem,
                  size_t b1, size_t b2) {
  BMZ_PACK_BODY(bmz_bm_pack_mask32x2(in, in_len, dst, &tlen,
                offset, fp_len, work_mem_aligned, b1, b2));
}

int
bmz_pack(const void *in, size_t in_len, void *out, size_t *out_len_p,
         size_t offset, size_t fp_len, unsigned flags, void *work_mem) {
  switch (flags >> 24) {
  case BMZ_HASH_MOD:
    return bmz_pack_mod(in, in_len, out, out_len_p, offset, fp_len, flags,
                        work_mem, BM_B1, BM_M1);
  case BMZ_HASH_MOD16X2:
    return bmz_pack_mod16x2(in, in_len, out, out_len_p, offset, fp_len, flags,
                            work_mem, BM_B1, BM_B2, BM_M1, BM_M2);
  case BMZ_HASH_MASK16X2:
    return bmz_pack_mask16x2(in, in_len, out, out_len_p, offset, fp_len, flags,
                             work_mem, BM_B1, BM_B2);
  case 0: /* default */
  case BMZ_HASH_MASK:
    return bmz_pack_mask(in, in_len, out, out_len_p, offset, fp_len, flags,
                         work_mem, BM_B1);
  case BMZ_HASH_MASK32X2:
    return bmz_pack_mask32x2(in, in_len, out, out_len_p, offset, fp_len, flags,
                             work_mem, BM_B1, BM_B2);
  default:
    BM_DIE("unknown hash algorithm: %u", (flags >> 24));
  }
  return BMZ_E_ERROR;
}

int
bmz_unpack(const void *in, size_t in_len, void *out, size_t *out_len_p,
           void *work_mem) {
  Byte *dst = (Byte *)work_mem;
  size_t tlen = *out_len_p + 1;
  int ret;

  ret = bmz_lz_unpack((Byte *)in, in_len, dst, &tlen);
  if (ret != BMZ_E_OK) return ret;
  return bmz_bm_unpack(dst, tlen, out, out_len_p);
}


#define BM_DECODE_POS(_cpos_, _n_) do { \
  UInt64 _ipos = _cpos_; \
  int w = BM_INT_WIDTH(_ipos); \
  BM_NEED_IN(w); \
  \
  switch (w) { \
  case 1: \
    _n_ = *ip++; \
    break; \
  case 2: \
    _n_ = (*ip++ << 8); \
    _n_ |= *ip++; \
    break; \
  case 3: \
    _n_ = (*ip++ << 16); \
    _n_ |= (*ip++ << 8); \
    _n_ |= *ip++; \
    break; \
  case 4: \
    _n_ = (*ip++ << 24); \
    _n_ |= (*ip++ << 16); \
    _n_ |= (*ip++ << 8); \
    _n_ |= *ip++; \
    break; \
  case 5: \
    _n_ = ((UInt64)*ip++ << 32); \
    _n_ |= (*ip++ << 24); \
    _n_ |= (*ip++ << 16); \
    _n_ |= (*ip++ << 8); \
    _n_ |= *ip++; \
    break; \
  case 6: \
    _n_ = ((UInt64)*ip++ << 40); \
    _n_ |= ((UInt64)*ip++ << 32); \
    _n_ |= (*ip++ << 24); \
    _n_ |= (*ip++ << 16); \
    _n_ |= (*ip++ << 8); \
    _n_ |= *ip++; \
  default: \
    BM_DIE("%s", "256TB ought to be enough for anyone!"); \
  } \
} while (0)

#define BM_DECODE_LEN(_n_) BM_DECODE_VINT(_n_)

int
bmz_bm_unpack(const void *src, size_t in_len, void *dst, size_t *out_len_p) {
  Byte *in = (Byte *)src, *out = (Byte *)dst;
  Byte *ip = in, *last_ip = ip + 1, *op = out, *cp;
  Byte *in_end = ip + in_len, *out_end = op + *out_len_p;
  int remains;

  if (*ip++ == 0) {
    memcpy(out, ip, --in_len);
    *out_len_p = in_len;
    return BMZ_E_OK;
  }

  while (ip < in_end) {
    if (*ip == BM_ESC) {
      UInt64 len = ip - last_ip, pos = 0;

      if (len) {
        BM_NEED_OUT(len);
        memcpy(op, last_ip, len);
        op += len;
        last_ip = ip;
      }
      BM_NEED_IN(1);

      if (ip[1] == BM_ESC) {
        ++last_ip;
        ip += 2;
      }
      else {
        ++ip;
        BM_DECODE_POS(op - out, pos);
        BM_DECODE_LEN(len);
        BM_NEED_OUT(len);
        last_ip = ip;
        cp = out + pos;
        
        while (len--) *op++ = *cp++;
      }
    }
    else ++ip;
  }
  remains = in_end - last_ip;
  BM_NEED_OUT(remains);
  memcpy(op, last_ip, remains);
  *out_len_p = op - out + remains;

  return BMZ_E_OK;

input_overrun:
  *out_len_p = op - out;
  return BMZ_E_INPUT_OVERRUN;

output_overrun:
  *out_len_p = op - out;
  return BMZ_E_OUTPUT_OVERRUN;
}

int
bmz_bm_dump(const void *src, size_t in_len) {
  Byte *in = (Byte *)src, *ip = in, *in_end = ip + in_len;
  UInt64 pos = 0, len, cpos = 0, tot_pte_sz = 0, tot_ptr_sz = 0, nptrs = 0;
  int is_on = *ip++;

  printf(BM_COLOR_DBG "%d" BM_COLOR_END, is_on);

  if (!is_on) {
    fwrite(ip, 1, in_end - ip, stdout);
    return BMZ_E_OK;
  }

  while (ip < in_end) {
    if (*ip == BM_ESC) {
      Byte *ip0 = ip;
      BM_NEED_IN(1);

      if (ip[1] != BM_ESC) {
        ++ip;
        BM_DECODE_POS(cpos, pos);
        BM_DECODE_LEN(len);
        printf(BM_COLOR_DBG "<%llu,%llu>" BM_COLOR_END,
               (unsigned long long)pos, (unsigned long long)len);
        cpos += len;
        tot_pte_sz += len;
        tot_ptr_sz += ip - ip0;
        ++nptrs;
      }
      else {
        putchar(*ip++);
        putchar(*ip++);
        cpos += 2;
      }
    }
    else {
      putchar(*ip++);
      ++cpos;
    }
  }
  BM_LOG(1, "%llu pointers, avg pointee size: %.3f, avg pointer size: %.3f\n", 
         (unsigned long long)nptrs, (double)tot_pte_sz / nptrs,
         (double)tot_ptr_sz / nptrs);
  return BMZ_E_OK;

input_overrun:
  return BMZ_E_INPUT_OVERRUN;
}

int
bmz_init() {
  int ret = lzo_init();
  return ret == LZO_E_OK ? BMZ_E_OK : BMZ_E_ERROR;
}

int
bmz_lz_pack(const void *in, size_t in_len, void *out, size_t *out_len_p,
            void *work_mem) {
  lzo_uint olen = *out_len_p;
  int ret = lzo1x_1_compress((Byte *)in, in_len, (Byte *)out, &olen, work_mem);
  *out_len_p = olen;
  return ret;
}

size_t
bmz_lz_pack_worklen(size_t in_len) {
  return LZO1X_MEM_COMPRESS;
}

int
bmz_lz_unpack(const void *in, size_t in_len, void *out, size_t *out_len_p) {
  lzo_uint olen = *out_len_p;
  int ret = lzo1x_decompress((Byte *)in, in_len, (Byte *)out, &olen, NULL);
  *out_len_p = olen;
  return ret;
}

unsigned
bmz_checksum(const void *in, size_t in_len) {
  return lzo_adler32(1, (Byte *)in, in_len);
}

unsigned
bmz_update_checksum(unsigned s, const void *in, size_t in_len) {
  return lzo_adler32(s, (Byte *)in, in_len);
}

/* vim: et sw=2
 */
