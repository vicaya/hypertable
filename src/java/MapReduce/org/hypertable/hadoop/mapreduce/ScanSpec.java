/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

package org.hypertable.hadoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.hypertable.hadoop.util.Base64;
import org.hypertable.hadoop.util.Serialization;

import org.hypertable.thriftgen.RowInterval;
import org.hypertable.thriftgen.CellInterval;

public class ScanSpec extends org.hypertable.thriftgen.ScanSpec {

  public ScanSpec() {
    super();
  }
  
  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ScanSpec(ScanSpec other) {
    super(other);
  }

  /**
   * Deserializes scan spec from DataInput.
   *
   * @param in DataInput object
   */
  public void readFields(final DataInput in) throws IOException {
    byte [] byte_value;
    boolean isset;
    int len;
    /** row_intervals **/
    isset = in.readBoolean();
    if (isset) {
      setRow_intervalsIsSet(true);
      len = in.readInt();
      if (len > 0) {
        RowInterval ri = null;
        row_intervals = new ArrayList<RowInterval>(len);
        for (int i = 0; i<len; i++) {
          ri = new RowInterval();
          ri.start_row = new String( Serialization.readByteArray(in) );
          ri.start_inclusive = in.readBoolean();
          ri.end_row = new String( Serialization.readByteArray(in) );
          ri.end_inclusive = in.readBoolean();
          row_intervals.add(ri);
        }
      }
    }
    /** cell_intervals **/
    isset = in.readBoolean();
    if (isset) {
      setCell_intervalsIsSet(true);
      len = in.readInt();
      if (len > 0) {
        CellInterval ci = null;
        row_intervals = new ArrayList<RowInterval>(len);
        for (int i = 0; i<len; i++) {
          ci = new CellInterval();
          ci.start_row = new String( Serialization.readByteArray(in) );
          ci.start_column = new String( Serialization.readByteArray(in) );
          ci.start_inclusive = in.readBoolean();
          ci.end_row = new String( Serialization.readByteArray(in) );
          ci.end_column = new String( Serialization.readByteArray(in) );
          ci.end_inclusive = in.readBoolean();
          cell_intervals.add(ci);
        }
      }
    }
    /** columns **/
    isset = in.readBoolean();
    if (isset) {
      setColumnsIsSet(true);
      len = in.readInt();
      if (len > 0) {
        columns = new ArrayList<String>(len);
        for (int i = 0; i<len; i++)
          columns.add( new String(Serialization.readByteArray(in)) );
      }
    }
    /** return_deletes **/
    isset = in.readBoolean();
    if (isset) {
      setReturn_deletesIsSet(true);
      return_deletes = in.readBoolean();
    }
    /** revs **/
    isset = in.readBoolean();
    if (isset) {
      setRevsIsSet(true);
      revs = in.readInt();
    }
    /** row_limit **/
    isset = in.readBoolean();
    if (isset) {
      setRow_limitIsSet(true);
      row_limit = in.readInt();
    }
    /** start_time **/
    isset = in.readBoolean();
    if (isset) {
      setStart_timeIsSet(true);
      start_time = in.readLong();
    }
    else
      start_time = Long.MIN_VALUE;
    /** end_time **/
    isset = in.readBoolean();
    if (isset) {
      setEnd_timeIsSet(true);
      end_time = in.readLong();
    }
    else
      end_time = Long.MAX_VALUE;
    /** keys_only **/
    isset = in.readBoolean();
    if (isset) {
      setEnd_timeIsSet(true);
      keys_only = in.readBoolean();
    }
  }

  /**
   * Serializes scan spec to DataOutput.
   *
   * @param out DataOutput object
   */
  public void write(final DataOutput out) throws IOException {
    byte [] empty = new byte [0];
    /** row_intervals **/
    if (isSetRow_intervals()) {
      out.writeBoolean(true);
      out.writeInt(row_intervals.size());
      for (final RowInterval ri : row_intervals) {
        if (ri.isSetStart_row())
          Serialization.writeByteArray(out, ri.start_row.getBytes());
        else
          Serialization.writeByteArray(out, empty);
        if (ri.isSetStart_inclusive())
          out.writeBoolean(ri.start_inclusive);
        else
          out.writeBoolean(true);
        if (ri.isSetEnd_row())
          Serialization.writeByteArray(out, ri.end_row.getBytes());
        else
          Serialization.writeByteArray(out, empty);
        if (ri.isSetEnd_inclusive())
          out.writeBoolean(ri.end_inclusive);
        else
          out.writeBoolean(true);
      }
    }
    else
      out.writeBoolean(false);
    /** cell_intervals **/
    if (isSetCell_intervals()) {
      out.writeBoolean(true);
      out.writeInt(cell_intervals.size());
      for (final CellInterval ci : cell_intervals) {
        if (ci.isSetStart_row())
          Serialization.writeByteArray(out, ci.start_row.getBytes());
        else
          Serialization.writeByteArray(out, empty);
        if (ci.isSetStart_column())
          Serialization.writeByteArray(out, ci.start_column.getBytes());
        else
          Serialization.writeByteArray(out, empty);
        if (ci.isSetStart_inclusive())
          out.writeBoolean(ci.start_inclusive);
        else
          out.writeBoolean(true);
        if (ci.isSetEnd_row())
          Serialization.writeByteArray(out, ci.end_row.getBytes());
        else
          Serialization.writeByteArray(out, empty);
        if (ci.isSetEnd_column())
          Serialization.writeByteArray(out, ci.end_column.getBytes());
        else
          Serialization.writeByteArray(out, empty);
        if (ci.isSetEnd_inclusive())
          out.writeBoolean(ci.end_inclusive);
        else
          out.writeBoolean(true);
      }
    }
    else
      out.writeBoolean(false);
    /** colunns **/
    if (isSetColumns()) {
      out.writeBoolean(true);
      out.writeInt(columns.size());
      for (final String s : columns)
        Serialization.writeByteArray(out, s.getBytes());
    }
    else
      out.writeBoolean(false);
    /** return_deletes **/
    if (isSetReturn_deletes()) {
      out.writeBoolean(true);
      out.writeBoolean(return_deletes);
    }
    else
      out.writeBoolean(false);
    /** revs **/
    if (isSetRevs()) {
      out.writeBoolean(true);
      out.writeInt(revs);
    }
    else
      out.writeBoolean(false);
    /** row_limit **/
    if (isSetRow_limit()) {
      out.writeBoolean(true);
      out.writeInt(row_limit);
    }
    else
      out.writeBoolean(false);
    /** start_time **/
    if (isSetStart_time()) {
      out.writeBoolean(true);
      out.writeLong(start_time);
    }
    else
      out.writeBoolean(false);
    /** end_time **/
    if (isSetEnd_time()) {
      out.writeBoolean(true);
      out.writeLong(end_time);
    }
    else
      out.writeBoolean(false);
    /** keys_only **/
    if (isSetKeys_only()) {
      out.writeBoolean(true);
      out.writeBoolean(keys_only);
    }
    else
      out.writeBoolean(false);
  }

  /**
   * Returns base 64 encoded, serialized representation of this
   * object, suitable for storing in the configuration object.
   *
   * @return base 64 encoded string
   */
  public String toSerializedText() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();  
    DataOutputStream dos = new DataOutputStream(out);
    write(dos);
    return Base64.encodeBytes(out.toByteArray());

  }

  /**
   * Constructs a scan spec object by deserializing base 64 encoded scan spec.
   *
   * @param serializedText base 64 encoded scan spec
   * @return materialized object
   */
  public static ScanSpec serializedTextToScanSpec(String serializedText) throws IOException {
    ScanSpec scan_spec = new ScanSpec();
    if (serializedText != null) {
      ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(serializedText));
      DataInputStream dis = new DataInputStream(bis);
      scan_spec.readFields(dis);
    }
    return scan_spec;
  }
}
