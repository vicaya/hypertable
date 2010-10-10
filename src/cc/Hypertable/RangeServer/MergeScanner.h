/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_MERGESCANNER_H
#define HYPERTABLE_MERGESCANNER_H

#include <queue>
#include <string>
#include <vector>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

#include "CellListScanner.h"
#include "CellStoreReleaseCallback.h"


namespace Hypertable {

  class MergeScanner : public CellListScanner {
  public:
    struct ScannerState {
      CellListScanner *scanner;
      Key key;
      ByteString value;
    };

    struct LtScannerState {
      bool operator()(const ScannerState &ss1, const ScannerState &ss2) const {
        return ss1.key.serial > ss2.key.serial;
      }
    };

    MergeScanner(ScanContextPtr &scan_ctx, bool return_everything=true, bool ag_scanner=false);
    virtual ~MergeScanner();
    virtual void forward();
    virtual bool get(Key &key, ByteString &value);
    void add_scanner(CellListScanner *scanner);

    void install_release_callback(CellStoreReleaseCallback &cb) {
      m_release_callback = cb;
    }

    void enable_io_accounting() { m_track_io = true; }

    void get_io_accounting_data(int64_t *inp, int64_t *outp) {
      *inp = m_bytes_input;
      *outp = m_bytes_output;
    }

  private:
    void initialize();
    inline bool matches_deleted_row(const Key& key) const {
      size_t len = key.len_row();

      HT_DEBUG_OUT <<"filtering deleted row '"
          << String((char*)m_deleted_row.base, m_deleted_row.fill()) <<"' vs '"
          << String(key.row, len) <<"'" << HT_END;

      return (m_delete_present && m_deleted_row.fill() > 0
              && m_deleted_row.fill() == len
              && !memcmp(m_deleted_row.base, key.row, len));
    }
    inline bool matches_deleted_column_family(const Key& key) const {
      size_t len = key.len_column_family();

      HT_DEBUG_OUT <<"filtering deleted row-column-family '"
          << String((char*)m_deleted_column_family.base,
                    m_deleted_column_family.fill())
          <<"' vs '"<< String(key.row, len) <<"'" << HT_END;

      return (m_delete_present && m_deleted_column_family.fill() > 0
              && m_deleted_column_family.fill() == len
              && !memcmp(m_deleted_column_family.base, key.row, len));
    }
    inline bool matches_deleted_cell(const Key& key) const {
      size_t len = key.len_cell();

      HT_DEBUG_OUT <<"filtering deleted cell '"
          << String((char*)m_deleted_cell.base, m_deleted_cell.fill())
          <<"' vs '"<< String(key.row, len) <<"'" << HT_END;

      return (m_delete_present && m_deleted_cell.fill() > 0
              &&  m_deleted_cell.fill() == len
              && !memcmp(m_deleted_cell.base, key.row, len));
    }
    inline bool matches_counted_key(const Key& key) const {
      size_t len = key.len_cell();
      size_t len_counted_key = m_counted_key.len_cell();

      return (m_count_present && len == len_counted_key &&
              !memcmp(m_counted_key.row, key.row, len));
    }

    inline void increment_count(const Key &key, const ByteString &value) {
      const uint8_t *decode;
      size_t remain = value.decode_length(&decode);
      // value must be encoded 64 bit int
      if (remain != 8) {
        HT_FATAL_OUT << "Expected counter to be encoded 64 bit int but remain=" << remain
            << " ,key=" << key << " ,value="<< value.str() << HT_END;
      }
      m_count += Serialization::decode_i64(&decode, &remain);
    }
    void finish_count();

    bool          m_done;
    bool          m_initialized;
    std::vector<CellListScanner *>  m_scanners;
    std::priority_queue<ScannerState, std::vector<ScannerState>,
        LtScannerState> m_queue;
    bool          m_delete_present;
    DynamicBuffer m_deleted_row;
    int64_t       m_deleted_row_timestamp;
    DynamicBuffer m_deleted_column_family;
    int64_t       m_deleted_column_family_timestamp;
    DynamicBuffer m_deleted_cell;
    int64_t       m_deleted_cell_timestamp;
    bool          m_return_deletes; // if this is true, return a delete even if
                                    // it doesn't satisfy ScanSpec
                                    // timestamp/version requirement

    bool          m_no_forward;
    bool          m_count_present;
    uint64_t      m_count;
    Key           m_counted_key;
    DynamicBuffer m_counted_value;
    DynamicBuffer m_tmp_count;

    bool          m_ag_scanner;
    bool          m_track_io;
    int32_t       m_row_count;
    int32_t       m_row_limit;
    int32_t       m_cell_count;
    int32_t       m_cell_limit;
    uint32_t      m_revs_count;
    uint32_t      m_revs_limit;
    int64_t       m_cell_cutoff;
    int64_t       m_start_timestamp;
    int64_t       m_end_timestamp;
    int64_t       m_bytes_input;
    int64_t       m_bytes_output;
    int64_t       m_cur_bytes;
    int64_t       m_revision;
    DynamicBuffer m_prev_key;
    int32_t       m_prev_cf;
    CellStoreReleaseCallback m_release_callback;
  };

  typedef boost::intrusive_ptr<MergeScanner> MergeScannerPtr;


} // namespace Hypertable

#endif // HYPERTABLE_MERGESCANNER_H
