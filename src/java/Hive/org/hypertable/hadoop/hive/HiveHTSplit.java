/**
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

package org.hypertable.hadoop.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import org.apache.hadoop.mapred.InputSplit;

import org.hypertable.hadoop.mapred.TableSplit;

/**
 * HiveHTSplit augments TableSplit with HT column mapping.
 */
public class HiveHTSplit extends FileSplit implements InputSplit {
  private String htColumnMapping;
  private TableSplit split;

  public HiveHTSplit() {
    super((Path) null, 0, 0, (String[]) null);
    htColumnMapping = "";
    split = new TableSplit();
  }

  public HiveHTSplit(TableSplit split, String columnsMapping, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.split = split;
    htColumnMapping = columnsMapping;
  }

  public TableSplit getSplit() {
    return this.split;
  }

  public String getColumnsMapping() {
    return this.htColumnMapping;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    htColumnMapping = in.readUTF();
    split.readFields(in);
  }

  @Override
  public String toString() {
    return "TableSplit: split=" + split + ", columnMapping=" + htColumnMapping + ", FileSplit="+
           super.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(htColumnMapping);
    split.write(out);
  }

  @Override
  public long getLength() {
    return split.getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    return split.getLocations();
  }
}
