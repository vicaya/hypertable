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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.hypertable.hadoop.util.Row;

/**
 * LazyObject for storing an Hypertable row.  The field of an Hypertable row can be
 * primitive or non-primitive.
 */
public class LazyHTRow extends LazyStruct {

  /**
   * The Hypertable columns mapping of the row.
   */
  private List<String> htColumns;
  private Row htRow;
  private ArrayList<Object> cachedList;

  /**
   * Construct a LazyHTRow object with the ObjectInspector.
   */
  public LazyHTRow(LazySimpleStructObjectInspector oi) {
    super(oi);
  }

  /**
   * Set the Hypertable row data(a Row writable) for this LazyStruct.
   * @see LazyHTRow#init(Row)
   */
  public void init(Row row, List<String> columns) {
    this.htRow = row;
    this.htColumns = columns;
    setParsed(false);
  }

  /**
   * Parse the Row and fill each field.
   * @see LazyStruct#parse()
   */
  private void parse() {

    if (getFields() == null) {
      List<? extends StructField> fieldRefs =
        ((StructObjectInspector)getInspector()).getAllStructFieldRefs();
      setFields(new LazyObject[fieldRefs.size()]);
      for (int i = 0; i < getFields().length; i++) {
        if (i > 0) {
          String htColumn = htColumns.get(i - 1);
          if (htColumn.endsWith(":")) {
            // a column family
            getFields()[i] =
              new LazyHTCellMap(
                (LazyMapObjectInspector)
                fieldRefs.get(i).getFieldObjectInspector());
            continue;
          }
        }

        getFields()[i] = LazyFactory.createLazyObject(
          fieldRefs.get(i).getFieldObjectInspector());
      }
      setFieldInited(new boolean[getFields().length]);
    }
    Arrays.fill(getFieldInited(), false);
    setParsed(true);
  }

  /**
   * Get one field out of the Hypertable row.
   *
   * If the field is a primitive field, return the actual object.
   * Otherwise return the LazyObject.  This is because PrimitiveObjectInspector
   * does not have control over the object used by the user - the user simply
   * directly uses the Object instead of going through
   * Object PrimitiveObjectInspector.get(Object).
   *
   * @param fieldID  The field ID
   * @return         The field as a LazyObject
   */
  public Object getField(int fieldID) {
    if (!getParsed()) {
      parse();
    }
    return uncheckedGetField(fieldID);
  }

  /**
   * Get the field out of the row without checking whether parsing is needed.
   * This is called by both getField and getFieldsAsList.
   * @param fieldID  The id of the field starting from 0.
   * @param nullSequence  The sequence representing NULL value.
   * @return  The value of the field
   */
  private Object uncheckedGetField(int fieldID) {
    if (!getFieldInited()[fieldID]) {
      getFieldInited()[fieldID] = true;

      ByteArrayRef ref = new ByteArrayRef();

      if (fieldID == 0) {
        // the key
        ref.setData(htRow.getRowKey());
        getFields()[fieldID].init(ref, 0, ref.getData().length);
      } else {
        String columnName = htColumns.get(fieldID - 1);
        if (columnName.endsWith(":")) {
          //it is a column family
          ((LazyHTCellMap) getFields()[fieldID]).init(
            htRow, columnName.substring(0, columnName.length()-1));
        } else {
          // it is a column
          if (htRow.containsCol(columnName)) {
            ref.setData(htRow.getValue(columnName));
            getFields()[fieldID].init(ref, 0, ref.getData().length);
          } else {
            return null;
          }
        }
      }
    }
    return getFields()[fieldID].getObject();
  }

  /**
   * Get the values of the fields as an ArrayList.
   * @return The values of the fields as an ArrayList.
   */
  public ArrayList<Object> getFieldsAsList() {
    if (!getParsed()) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < getFields().length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }

  @Override
  public Object getObject() {
    return this;
  }

}
