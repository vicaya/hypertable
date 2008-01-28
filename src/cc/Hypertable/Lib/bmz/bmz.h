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

#ifndef HYPERTABLE_BMZ_H
#define HYPERTABLE_BMZ_H

#include "compat-c.h"

#define BMZ_VERSION "0.1.1.0"
#define BMZ_VERNUM 0x0110

/* Error codes */
#define BMZ_E_OK                0
#define BMZ_E_ERROR           (-1)
#define BMZ_E_INPUT_OVERRUN   (-4)
#define BMZ_E_OUTPUT_OVERRUN  (-5)

/* bmz_pack flags */

/** Indicate inplace (de)compression to save some me
 *  so that output will overwrite input 
 */
#define BMZ_F_OVERLAP   1

/* Common APIs */

/** Perform bmz compression
 *
 * @param in - input buffer
 * @param in_len - input buffer length in bytes
 * @param out - output buffer for compressed data
 * @param out_len_p - pointer to the length of output, which specifies the size
 *                    of the output buffer as input and is set to the length of
 *                    compressed data on return 
 * @param offset - starting offset of fingerprints, use 0 if you have to ask
 * @param fp_len - fingerprint length, use 50 if you have to ask
 * @param flags - compression options. See BMZ_F_* defines.
 * @param work_mem - pointer to work memory buffer, cf. bmz_pack_worklen 
 * @return error code
 */
HT_EXTERN(int)
bmz_pack(const void *in, size_t in_len, void *out, size_t *out_len_p,
         size_t offset, size_t fp_len, unsigned flags, void *work_mem);

/** Perform bmz decompression
 *
 * @param in - input buffer (compressed)
 * @param in_len - input buffer length in bytes
 * @param out - output buffer
 * @param out_len_p - pointer to the length of output, which specifies the size
 *                    of the output buffer and is set to length of uncompressed
 *                    data on return;
 * @return error code
 */
HT_EXTERN(int)
bmz_unpack(const void *in, size_t in_len, void *out, size_t *out_len_p);

/** Compute bmz compression output buffer length
 *
 * @param in_len - input buffer length in bytes
 * @return output length in bytes
 */
HT_EXTERN(size_t)
bmz_pack_buflen(size_t in_len);

/** Return size of work memory for bmz compression.
 *
 * so caller doesn't have to allocate memory per invocation 
 * when the input buffer size is fixed.
 *
 * @param in_len - input buffer length in bytes
 * @param fp_len - fingerprint length in bytes
 * @return length in bytes
 */
HT_EXTERN(size_t)
bmz_pack_worklen(size_t in_len, size_t fp_len);

/** Compute bmz decompression output buffer length
 *
 * The output buffer size might be bigger than the
 * data size to accommdate the algorithm
 *
 * @param out_len - uncompressed data length in bytes
 * @return length in bytes 
 */
HT_EXTERN(size_t)
bmz_unpack_buflen(size_t out_len);

/** Set the verbosity of library for testing and debugging 
 *
 * @param verbosity - 0, 1, 2
 * @return old verbosity
 */
HT_EXTERN(int)
bmz_set_verbosity(int verbosity);

/** Signature of the messaging/logging procedure
 */
typedef void (*BmzOutProc)(const char *msg, size_t len);

/** Signature of the fatal procedure
 */
typedef void (*BmzDieProc)(const char *msg) HT_NORETURN;

/** Set messaging/logging procedure
 *
 * @param proc - pointer to a message function
 * @return old proc
 */
HT_EXTERN(BmzOutProc)
bmz_set_out_proc(BmzOutProc proc);

/** Set fatal message procedure
 *
 * @param proc - pointer to a fatal  function
 * @return old proc
 */
HT_EXTERN(BmzDieProc)
bmz_set_die_proc(BmzDieProc proc);

/** A fast checksum (adler32) function that might be useful
 *
 * @param in - input buffer
 * @param in_len - input buffer length in bytes
 */
HT_EXTERN(unsigned int)
bmz_checksum(const void *in, size_t in_len);

#endif /* HYPERTABLE_BMZ_H */
/* vim: et sw=2
 */
