package org.hypertable.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.hypertable.mapreduce.InputSplit;

public class TableSplit implements InputSplit {
  private Text m_tableName;
  private Text m_startRow;
  private Text m_endRow;
  private Text m_location;
  
  public TableSplit()
  {
    m_tableName = new Text();
    m_startRow = new Text();
    m_endRow = new Text();
    m_location = new Text();
  }
  
  public TableSplit(Text tableName, Text startRow, Text endRow, Text location) {
    this();
    m_location.set(location);
    m_tableName.set(tableName);
    m_startRow.set(startRow);
    m_endRow.set(endRow);
  }
  
  public Text getTableName() {
    return m_tableName;
  }
  
  public Text getStartRow() {
    return m_startRow;
  }
  
  public Text getEndRow() {
    return m_endRow;
  }
  
  public long getLength() {
    return 0;
  }
  
  public Text getLocation() {
    return m_location;
  }
  
  public void readFields(DataInput in) throws IOException {
    m_tableName.readFields(in);
    m_startRow.readFields(in);
    m_endRow.readFields(in);
  }
  
  public String[] getLocations() {
    return new String[] { };
  }
  public void write(DataOutput out) throws IOException {
    m_tableName.write(out);
    m_startRow.write(out);
    m_endRow.write(out);
  }

  @Override
  public String toString() {
    return m_tableName +"," + m_startRow + "," + m_endRow;
  }
}
