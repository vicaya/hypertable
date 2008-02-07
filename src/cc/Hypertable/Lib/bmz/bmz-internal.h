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

#ifndef BMZ_INTERNAL_H
#define BMZ_INTERNAL_H

#include "bmz.h"

/* Expert APIs for testing and experiement */

/* MSB of flags */
#define BMZ_HASH_MOD            1
#define BMZ_HASH_MOD16X2        2
#define BMZ_HASH_MASK16X2       3
#define BMZ_HASH_MASK           4
#define BMZ_HASH_MASK32X2       5

HT_EXTERN(size_t)
bmz_hash_mod(const void *in, size_t in_len, size_t b, size_t m);

HT_EXTERN(size_t)
bmz_hash_mod16x2(const void *in, size_t in_len, size_t b1, size_t b2,
                 size_t m1, size_t m2);

HT_EXTERN(size_t)
bmz_hash_mask16x2(const void *in, size_t in_len, size_t b1, size_t b2);

HT_EXTERN(size_t)
bmz_hash_mask(const void *in, size_t in_len, size_t b);

HT_EXTERN(size_t)
bmz_hash_mask32x2(const void *in, size_t in_len, size_t b1, size_t b2);

HT_EXTERN(int)
bmz_check_hash_mod(const void *in, size_t in_len, size_t fp_len,
                   size_t b, size_t m);

HT_EXTERN(int)
bmz_check_hash_mod16x2(const void *in, size_t in_len, size_t fp_len,
                       size_t b1, size_t b2, size_t m1, size_t m2);

HT_EXTERN(int)
bmz_check_hash_mask16x2(const void *in, size_t in_len, size_t fp_len,
                        size_t b1, size_t b2);

HT_EXTERN(int)
bmz_check_hash_mask(const void *in, size_t in_len, size_t fp_len, size_t b);

HT_EXTERN(int)
bmz_check_hash_mask32x2(const void *in, size_t in_len, size_t fp_len,
                        size_t b1, size_t b2);

HT_EXTERN(void)
bmz_bench_hash(const void *in, size_t in_len, unsigned type);

HT_EXTERN(void)
bmz_bench_lut_mod(const void *in, size_t in_len, size_t fp_len, void *mem,
                  size_t b, size_t m);

HT_EXTERN(void)
bmz_bench_lut_mod16x2(const void *in, size_t in_len, size_t fp_len, void *mem,
                      size_t b1, size_t b2, size_t m1, size_t m2);

HT_EXTERN(void)
bmz_bench_lut_mask16x2(const void *in, size_t in_len, size_t fp_len, void *mem,
                       size_t b1, size_t b2);

HT_EXTERN(void)
bmz_bench_lut_mask(const void *in, size_t in_len, size_t fp_len, void *mem,
                   size_t b);

HT_EXTERN(void)
bmz_bench_lut_mask32x2(const void *in, size_t in_len, size_t fp_len, void *mem,
                       size_t b1, size_t b2);

/* Note: for bmz_bm_pack*, unlike in bmz_pack*, work_mem needs to be
 * 64-bit word aligned
 */

HT_EXTERN(int)
bmz_pack_mod(const void *in, size_t in_len, void *out, size_t *out_len_p,
             size_t offset, size_t fp_len, unsigned flags, void *work_mem,
             size_t b, size_t m);

HT_EXTERN(int)
bmz_pack_mod16x2(const void *in, size_t in_len, void *out, size_t *out_len_p,
             size_t offset, size_t fp_len, unsigned flags, void *work_mem,
             size_t b1, size_t b2, size_t m1, size_t m2);

HT_EXTERN(int)
bmz_pack_mask16x2(const void *in, size_t in_len, void *out, size_t *out_len_p,
                  size_t offset, size_t fp_len, unsigned flags, void *work_mem,
                  size_t b1, size_t b2);

HT_EXTERN(int)
bmz_pack_mask(const void *in, size_t in_len, void *out, size_t *out_len_p,
              size_t offset, size_t fp_len, unsigned flags, void *work_mem,
              size_t b);

HT_EXTERN(int)
bmz_pack_mask32x2(const void *in, size_t in_len, void *out, size_t *out_len_p,
                  size_t offset, size_t fp_len, unsigned flags, void *work_mem,
                  size_t b1, size_t b2);

HT_EXTERN(int)
bmz_bm_pack_mod(const void *in, size_t in_len, void *out, size_t *out_len_p,
                size_t offset, size_t fp_len, void *work_mem,
                size_t b, size_t m);

HT_EXTERN(int)
bmz_bm_pack_mod16x2(const void *in, size_t in_len, void *out, size_t *out_len_p,
                    size_t offset, size_t fp_len, void *work_mem,
                    size_t b1, size_t b2, size_t m1, size_t m2);

HT_EXTERN(int)
bmz_bm_pack_mask16x2(const void *in, size_t in_len, void *out,
                     size_t *out_len_p, size_t offset, size_t fp_len,
                     void *work_mem, size_t b1, size_t b2);

HT_EXTERN(int)
bmz_bm_pack_mask(const void *in, size_t in_len, void *out, size_t *out_len_p,
                 size_t offset, size_t fp_len, void *work_mem, size_t b);

HT_EXTERN(int)
bmz_bm_pack_mask32x2(const void *in, size_t in_len, void *out,
                     size_t *out_len_p, size_t offset, size_t fp_len,
                     void *work_mem, size_t b1, size_t b2);

HT_EXTERN(size_t)
bmz_bm_pack_worklen(size_t in_len, size_t fp_len);

HT_EXTERN(int)
bmz_bm_dump(const void *in, size_t in_len);

HT_EXTERN(int)
bmz_bm_unpack(const void *in, size_t in_len, void *out, size_t *out_len_p);

HT_EXTERN(int)
bmz_lz_pack(const void *in, size_t in_len, void *out, size_t *out_len_p,
            void *work_mem);

HT_EXTERN(size_t)
bmz_lz_pack_worklen(size_t in_len);

HT_EXTERN(int)
bmz_lz_unpack(const void *in, size_t in_len, void *out, size_t *out_len_p);

HT_EXTERN(int)
bmz_set_collision_thresh(int thresh);

#endif /* BMZ_INTERNAL_H */
/* vim: et sw=2
 */
