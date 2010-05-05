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

package org.hypertable.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.hypertable.hadoop.util.Serialization;

import org.hypertable.thriftgen.*;

import org.hypertable.hadoop.mapreduce.ScanSpec;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;

/**
 * A table split corresponds to a key range (low, high). All references to row
 * below refer to the key of the row.
 */
public class TextTableSplit
implements InputSplit,Comparable<TextTableSplit> {
  
  private byte [] m_tablename;
  private byte [] m_startrow;
  private byte [] m_endrow;
  private String m_range_location;
  private ScanSpec m_scanspec;

  /** Default constructor. */
  public TextTableSplit() {
      this(new byte [0], new byte [0], new byte [0], "");
  }

  /**
   * Constructs a new instance while assigning all variables.
   * 
   * @param tableName  The name of the current table.
   * @param startRow  The start row of the split.
   * @param endRow  The end row of the split.
   * @param location  The location of the range.
   */
  public TextTableSplit(byte [] tableName, byte [] startRow, byte [] endRow,
      final String location) {
    this.m_tablename = tableName;
    this.m_startrow = startRow;
    this.m_endrow = endRow;
    // Check for END_ROW marker
    if (endRow != null && endRow.length == 2 &&
        endRow[0] == (byte)0xff && endRow[1] == (byte)0xff) {
      this.m_endrow = null;
    }
    try {
        java.net.InetAddress inetAdd =
        java.net.InetAddress.getByName(location); 
        this.m_range_location = inetAdd.getHostName();
    } catch(java.net.UnknownHostException uhe) {
        //handle exception
    }
  }

  /**
   * Returns the table name.
   * 
   * @return The table name. 
   */
  public byte [] getTableName() {
    return m_tablename;
  }

  /**
   * Returns the start row.
   *  
   * @return The start row.
   */ 
  public byte [] getStartRow() {
    return m_startrow;
  }

  /**
   * Returns the end row.
   * 
   * @return The end row. 
   */
  public byte [] getEndRow() {
    return m_endrow;
  }

  /** 
   * Returns the range location.
   * 
   * @return The range's location. 
   */
  public String getRangeLocation() {
    return m_range_location;
  }

  /**
   * Returns the range's location as an array.
   * 
   * @return The array containing the range location.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
   */
  public String[] getLocations() {
    return new String[] {m_range_location};
  }

  /**
   * Returns the length of the split.
   * 
   * @return The length of the split.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  public long getLength() {
    // Not clear how to obtain this... seems to be used only for sorting splits
    return 0;
  }


  /**
   * Updates the ScanSpec by setting the row interval to match this split
   *
   * @param scan_spec The base ScanSpec to start with
   * @return a new scan_spec object with a row interval matching this split
   */
  public ScanSpec createScanSpec(ScanSpec base_spec) {
    ScanSpec scan_spec = new ScanSpec(base_spec);

    RowInterval interval = new RowInterval();

    scan_spec.unsetRow_intervals();

    if(m_startrow != null && m_startrow.length > 0) {
      interval.setStart_row(new String(m_startrow));
      interval.setStart_rowIsSet(true);
      interval.setStart_inclusive(false);
      interval.setStart_inclusiveIsSet(true);
    }

    if(m_endrow != null && m_endrow.length > 0) {
      interval.setEnd_row(new String(m_endrow));
      interval.setEnd_rowIsSet(true);
      interval.setEnd_inclusive(true);
      interval.setEnd_inclusiveIsSet(true);
    }

    for(RowInterval ri: base_spec.getRow_intervals()) {
        if(m_startrow == null) {
            if(ri.isSetStart_row() && m_endrow != null) {
                if(ri.getStart_row().compareTo(new String(m_endrow)) < 0) {
                    interval.setStart_row(ri.getStart_row());
                    interval.setStart_rowIsSet(true);
                    interval.setStart_inclusive(false);
                    interval.setStart_inclusiveIsSet(true);
                }
            }
            
        }
        if(m_endrow == null) {
            if(ri.isSetEnd_row() && m_startrow != null) {
                if(ri.getEnd_row().compareTo(new String(m_startrow)) >= 0) {
                    interval.setEnd_row(ri.getEnd_row());
                    interval.setEnd_rowIsSet(true);
                    interval.setEnd_inclusive(true);
                    interval.setEnd_inclusiveIsSet(true);
                }
            }
            
        }
    }

    if(interval.isSetStart_row() || interval.isSetEnd_row()) {
      scan_spec.addToRow_intervals(interval);
      scan_spec.setRow_intervalsIsSet(true);
    }

    return scan_spec;
  }

  /**
   * Reads the values of each field.
   * 
   * @param in  The input to read from.
   * @throws IOException When reading the input fails.
   */
  public void readFields(DataInput in) throws IOException {
    m_tablename = Serialization.readByteArray(in);
    m_startrow = Serialization.readByteArray(in);
    m_endrow = Serialization.readByteArray(in);
    m_range_location = Serialization.toString(Serialization.readByteArray(in));
  }

  /**
   * Writes the field values to the output.
   * 
   * @param out  The output to write to.
   * @throws IOException When writing the values to the output fails.
   */
  public void write(DataOutput out) throws IOException {
    Serialization.writeByteArray(out, m_tablename);
    Serialization.writeByteArray(out, m_startrow);
    Serialization.writeByteArray(out, m_endrow);
    Serialization.writeByteArray(out, Serialization.toBytes(m_range_location));
  }

  /**
   * Returns the details about this instance as a string.
   * 
   * @return The values of this instance as a string.
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return m_range_location + ":" +
      Serialization.toStringBinary(m_startrow) + "," + Serialization.toStringBinary(m_endrow);
  }

  public int compareTo(TextTableSplit split) {
    return Serialization.compareTo(getStartRow(), split.getStartRow());
  }
  
  /* XXX add equals for columns */
  public boolean equal(TextTableSplit split) {
    return Serialization.equals(m_tablename, split.m_tablename) &&
      Serialization.equals(m_startrow, split.m_startrow) &&
      Serialization.equals(m_endrow, split.m_endrow) &&
      m_range_location.equals(split.m_range_location);
  }

}
