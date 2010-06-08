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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import org.hypertable.hadoop.util.Row;

/**
 * LazyHTCellMap refines LazyMap with Hypertable column mapping.
 */
public class LazyHTCellMap extends LazyMap {

  private Row htRow;
  private String htColumnFamily;

  /**
   * Construct a LazyCellMap object with the ObjectInspector.
   * @param oi
   */
  public LazyHTCellMap(LazyMapObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    // do nothing
  }

  public void init(Row row, String columnFamily) {
    htRow = row;
    htColumnFamily = columnFamily;
    setParsed(false);
  }

  private void parse() {
    if (cachedMap == null) {
      cachedMap = new LinkedHashMap<Object, Object>();
    } else {
      cachedMap.clear();
    }

    NavigableMap<byte[], byte[]> qualifierToValue =
        htRow.getColFamilyMap(htColumnFamily.getBytes());

    if (qualifierToValue != null) {
      Iterator<byte []> iter = qualifierToValue.keySet().iterator();
      while (iter.hasNext()) {
        byte [] colQualifier = iter.next();
        byte [] colValue = qualifierToValue.get(colQualifier);
        if (colValue == null || colValue.length == 0) {
          continue;
        }

        // Keys are always primitive
        LazyPrimitive<?, ?> key = LazyFactory.createLazyPrimitiveClass(
            (PrimitiveObjectInspector)
            ((MapObjectInspector) getInspector()).getMapKeyObjectInspector());
        ByteArrayRef keyRef = new ByteArrayRef();
        keyRef.setData(colQualifier);
        key.init(keyRef, 0, colQualifier.length);

        // Value
        LazyObject value = LazyFactory.createLazyObject(
          ((MapObjectInspector) getInspector()).getMapValueObjectInspector());
        ByteArrayRef valueRef = new ByteArrayRef();
        valueRef.setData(colValue);
        value.init(valueRef, 0, colValue.length);

        // Put it into the map
        cachedMap.put(key.getObject(), value.getObject());
      }
    }
  }

  /**
   * Get the value in the map for the given key.
   *
   * @param key
   * @return
   */
  public Object getMapValueElement(Object key) {
    if (!getParsed()) {
      parse();
    }

    for (Map.Entry<Object, Object> entry : cachedMap.entrySet()) {
      LazyPrimitive<?, ?> lazyKeyI = (LazyPrimitive<?, ?>) entry.getKey();
      // getWritableObject() will convert LazyPrimitive to actual primitive
      // writable objects.
      Object keyI = lazyKeyI.getWritableObject();
      if (keyI == null) {
        continue;
      }
      if (keyI.equals(key)) {
        // Got a match, return the value
        LazyObject v = (LazyObject) entry.getValue();
        return v == null ? v : v.getObject();
      }
    }

    return null;
  }

  public Map<Object, Object> getMap() {
    if (!getParsed()) {
      parse();
    }
    return cachedMap;
  }

  public int getMapSize() {
    if (!getParsed()) {
      parse();
    }
    return cachedMap.size();
  }

}
