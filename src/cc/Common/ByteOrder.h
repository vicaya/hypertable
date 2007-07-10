/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef HYPERTABLE_BYTEORDER_H
#define HYPERTABLE_BYTEORDER_H

#define ByteOrderSwapInt64(x) ((((uint64_t)(x) & 0xff00000000000000ULL) >> 56) | \
                               (((uint64_t)(x) & 0x00ff000000000000ULL) >> 40) | \
                               (((uint64_t)(x) & 0x0000ff0000000000ULL) >> 24) | \
                               (((uint64_t)(x) & 0x000000ff00000000ULL) >>  8) | \
                               (((uint64_t)(x) & 0x00000000ff000000ULL) <<  8) | \
                               (((uint64_t)(x) & 0x0000000000ff0000ULL) << 24) | \
                               (((uint64_t)(x) & 0x000000000000ff00ULL) << 40) | \
                               (((uint64_t)(x) & 0x00000000000000ffULL) << 56))

#endif // HYPERTABLE_BYTEORDER_H

