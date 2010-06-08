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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.hypertable.hadoop.util.Row;
import org.hypertable.thrift.SerializedCellsFlag;
import org.hypertable.thrift.SerializedCellsWriter;
import org.hypertable.thriftgen.*;
import org.hypertable.Common.ColumnNameSplit;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * HTSerDe can be used to serialize object into an Hypertable table and
 * deserialize objects from an Hypertable table.
 */
public class HTSerDe implements SerDe {

  public static final String HT_COL_MAPPING = "hypertable.columns.mapping";

  public static final String HT_TABLE_NAME = "hypertable.table.name";

  public static final Log LOG = LogFactory.getLog(
      HTSerDe.class.getName());

  private ObjectInspector cachedObjectInspector;
  private HTSerDeParameters htSerDeParams;
  private LazyHTRow cachedHTRow;
  private Row serializeCache;

  /**
   * HTSerDeParameters defines the parameters used to
   * instantiate HTSerDe.
   */
  public static class HTSerDeParameters {
    private List<String> htColumnNames;
    private SerDeParameters serdeParams;

    public List<String> getHTColumnNames() {
      return htColumnNames;
    }

    public SerDeParameters getSerDeParameters() {
      return serdeParams;
    }
  }

  public String toString() {
    return getClass().toString()
        + "["
        + htSerDeParams.htColumnNames
        + ":"
        + ((StructTypeInfo) htSerDeParams.serdeParams.getRowTypeInfo())
            .getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) htSerDeParams.serdeParams.getRowTypeInfo())
            .getAllStructFieldTypeInfos() + "]";
  }

  public HTSerDe() throws SerDeException {
  }

  /**
   * Initialize the SerDe given parameters.
   * @see SerDe#initialize(Configuration, Properties)
   */
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    htSerDeParams = HTSerDe.initHTSerDeParameters(conf, tbl,
        getClass().getName());

    // We just used columnNames & columnTypes these two parameters
    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        htSerDeParams.serdeParams.getColumnNames(),
        htSerDeParams.serdeParams.getColumnTypes(),
        htSerDeParams.serdeParams.getSeparators(),
        htSerDeParams.serdeParams.getNullSequence(),
        htSerDeParams.serdeParams.isLastColumnTakesRest(),
        htSerDeParams.serdeParams.isEscaped(),
        htSerDeParams.serdeParams.getEscapeChar());

    cachedHTRow = new LazyHTRow(
      (LazySimpleStructObjectInspector) cachedObjectInspector);

    if (LOG.isDebugEnabled()) {
      LOG.debug("HTSerDe initialized with : columnNames = "
        + htSerDeParams.serdeParams.getColumnNames()
        + " columnTypes = "
        + htSerDeParams.serdeParams.getColumnTypes()
        + " htColumnMapping = "
        + htSerDeParams.htColumnNames);
    }
  }

  public static HTSerDeParameters initHTSerDeParameters(
      Configuration job, Properties tbl, String serdeName)
    throws SerDeException {

    HTSerDeParameters serdeParams = new HTSerDeParameters();

    // Read Configuration Parameter
    String htColumnNameProperty =
      tbl.getProperty(HTSerDe.HT_COL_MAPPING);
    String columnTypeProperty =
      tbl.getProperty(Constants.LIST_COLUMN_TYPES);

    // Initial the hypertable column list
    if (htColumnNameProperty != null
      && htColumnNameProperty.length() > 0) {

      serdeParams.htColumnNames =
        Arrays.asList(htColumnNameProperty.split(","));
    } else {
      serdeParams.htColumnNames = new ArrayList<String>();
    }

    // Add the hypertable key to the columnNameList and columnTypeList

    // Build the type property string
    if (columnTypeProperty == null) {
      StringBuilder sb = new StringBuilder();
      sb.append(Constants.STRING_TYPE_NAME);

      for (int i = 0; i < serdeParams.htColumnNames.size(); i++) {
        String colName = serdeParams.htColumnNames.get(i);
        if (colName.endsWith(":"))  {
          sb.append(":").append(
            Constants.MAP_TYPE_NAME + "<"
            + Constants.STRING_TYPE_NAME
            + "," + Constants.STRING_TYPE_NAME + ">");
        } else if (colName.contains(":")){
          throw new SerDeException(serdeName + ": This storage handler only supports " +
              " Hypertable column families for now and doesn't support qualified columns");
        }
        else {
          sb.append(":").append(Constants.STRING_TYPE_NAME);
        }
      }
      tbl.setProperty(Constants.LIST_COLUMN_TYPES, sb.toString());
    }

    serdeParams.serdeParams = LazySimpleSerDe.initSerdeParams(
      job, tbl, serdeName);

    if (serdeParams.htColumnNames.size() + 1
      != serdeParams.serdeParams.getColumnNames().size()) {

      throw new SerDeException(serdeName + ": columns has " +
          serdeParams.serdeParams.getColumnNames().size() +
          " elements while ht.columns.mapping has " +
          serdeParams.htColumnNames.size() + " elements!");
    }

    // check that the mapping schema is right;
    // we just can make sure that "columnfamily:" is mapped to MAP<String,?>
    for (int i = 0; i < serdeParams.htColumnNames.size(); i++) {
      String htColName = serdeParams.htColumnNames.get(i);
      if (htColName.endsWith(":")) {
        TypeInfo typeInfo = serdeParams.serdeParams.getColumnTypes().get(i + 1);
        if ((typeInfo.getCategory() != Category.MAP) ||
          (((MapTypeInfo) typeInfo).getMapKeyTypeInfo().getTypeName()
            !=  Constants.STRING_TYPE_NAME)) {

          throw new SerDeException(
            serdeName + ": ht column family '"
            + htColName
            + "' should be mapped to map<string,?> but is mapped to "
            + typeInfo.getTypeName());
        }
      }
      else if (htColName.contains(":")) {
        throw new SerDeException(serdeName + ": This storage handler only supports " +
            " Hypertable column families for now and doesn't support qualified columns");
      }
    }
    return serdeParams;
  }
  /**
   * Deserialize a row from the HT Row writable to a LazyObject
   * @param Row the Row writable contain a row
   * @return the deserialized object
   * @see SerDe#deserialize(Writable)
   */
  public Object deserialize(Writable row) throws SerDeException {

    if (!(row instanceof Row)) {
      throw new SerDeException(getClass().getName() + ": expects Row!");
    }

    Row rr = (Row)row;

    cachedHTRow.init(rr, htSerDeParams.htColumnNames);
    return cachedHTRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Row.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields =
      (htSerDeParams.serdeParams.getRowTypeInfo() != null &&
        ((StructTypeInfo) htSerDeParams.serdeParams.getRowTypeInfo())
        .getAllStructFieldNames().size() > 0) ?
      ((StructObjectInspector)getObjectInspector()).getAllStructFieldRefs()
      : null;

    int rowkeyOffset=0, rowkeyLen=0;
    int colFamilyOffset=0, colFamilyLen=0;
    int colQualifierOffset=0, colQualifierLen=0;
    int valueOffset=0, valueLen=0;
    byte [] rowkey=null;
    byte [] cf=null;
    byte [] cq=null;
    SerializedCellsWriter cellWriter = new SerializedCellsWriter(0, true);
    ByteStream.Output serializeStream = new ByteStream.Output();

    boolean isNotNull = false;
    String htColumn = "";

    try {
      // Serialize each field
      for (int ii = 0; ii < fields.size(); ii++) {
        // Get the field objectInspector and the field object.
        ObjectInspector ffoi = fields.get(ii).getFieldObjectInspector();
        Object ff = (list == null ? null : list.get(ii));

        if (declaredFields != null && ii >= declaredFields.size()) {
          throw new SerDeException(
              "Error: expecting " + declaredFields.size()
              + " but asking for field " + ii + "\n" + "data=" + obj + "\n"
              + "tableType="
              + htSerDeParams.serdeParams.getRowTypeInfo().toString()
              + "\n"
              + "dataType="
              + TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector));
        }

        if (ff == null) {
          // a null object, we do not serialize it
          continue;
        }

        serializeStream.reset();
        if (ii == 0) {
          // store the rowkey
          isNotNull = serialize(serializeStream, ff, ffoi,
                                htSerDeParams.serdeParams.getSeparators(), 1,
                                htSerDeParams.serdeParams.getNullSequence(),
                                htSerDeParams.serdeParams.isEscaped(),
                                htSerDeParams.serdeParams.getEscapeChar(),
                                htSerDeParams.serdeParams.getNeedsEscape());
          rowkeyLen = serializeStream.getCount();
          rowkey = new byte[rowkeyLen];
          System.arraycopy(serializeStream.getData(), 0, rowkey , 0, rowkeyLen);
          continue;
        }

        htColumn = htSerDeParams.htColumnNames.get(ii-1);

        // This field is a Map in Hive, mapping to a set cells with the same column family
        // in HT
        if (htColumn.endsWith(":")) {
          MapObjectInspector moi = (MapObjectInspector)ffoi;
          ObjectInspector koi = moi.getMapKeyObjectInspector();
          ObjectInspector voi = moi.getMapValueObjectInspector();

          Map<?, ?> map = moi.getMap(ff);
          if (map == null) {
            continue;
          } else {
            for (Map.Entry<?, ?> entry: map.entrySet()) {
              // serialize column qualifier
              colQualifierOffset = serializeStream.getCount();
              serialize(serializeStream, entry.getKey(), koi,
                        htSerDeParams.serdeParams.getSeparators(), 3,
                        htSerDeParams.serdeParams.getNullSequence(),
                        htSerDeParams.serdeParams.isEscaped(),
                        htSerDeParams.serdeParams.getEscapeChar(),
                        htSerDeParams.serdeParams.getNeedsEscape());
              valueOffset = serializeStream.getCount();
              colQualifierLen = valueOffset - colQualifierOffset;
              // serialize value
              isNotNull = serialize(serializeStream, entry.getValue(), voi,
                                   htSerDeParams.serdeParams.getSeparators(), 3,
                                   htSerDeParams.serdeParams.getNullSequence(),
                                   htSerDeParams.serdeParams.isEscaped(),
                                   htSerDeParams.serdeParams.getEscapeChar(),
                                   htSerDeParams.serdeParams.getNeedsEscape());
              valueLen = serializeStream.getCount() - valueOffset;

              // write cell
              if (isNotNull) {

                boolean added = cellWriter.add(rowkey, 0, rowkeyLen,
                    htColumn.getBytes(), 0, htColumn.length() - 1,
                    serializeStream.getData(), colQualifierOffset, colQualifierLen,
                    SerializedCellsFlag.AUTO_ASSIGN,
                    serializeStream.getData(), valueOffset, valueLen);
              }
            }
          }
        }
        // Hive column to HT column
        else {
            valueOffset = serializeStream.getCount();
            isNotNull = serialize(serializeStream, ff, ffoi,
                                  htSerDeParams.serdeParams.getSeparators(), 1,
                                  htSerDeParams.serdeParams.getNullSequence(),
                                  htSerDeParams.serdeParams.isEscaped(),
                                  htSerDeParams.serdeParams.getEscapeChar(),
                                  htSerDeParams.serdeParams.getNeedsEscape());
            if (isNotNull) {
              valueLen = serializeStream.getCount() - valueOffset;
              ColumnNameSplit.split(htColumn, cf, cq);
              cellWriter.add(rowkey, 0, rowkeyLen,
                             cf, 0, cf.length,
                             cq, 0, cq.length,
                             SerializedCellsFlag.AUTO_ASSIGN,
                             serializeStream.getData(), valueOffset, valueLen);
            }
        }
      }
      // set the SerializedCells into a Row object
      serializeCache = new Row(cellWriter.array());
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return serializeCache;
  }

  /**
   * Serialize the row into the StringBuilder.
   * @param out  The StringBuilder to store the serialized data.
   * @param obj The object for the current field.
   * @param objInspector  The ObjectInspector for the current Object.
   * @param separators    The separators array.
   * @param level         The current level of separator.
   * @param nullSequence  The byte sequence representing the NULL value.
   * @param escaped       Whether we need to escape the data when writing out
   * @param escapeChar    Which char to use as the escape char, e.g. '\\'
   * @param needsEscape   Which chars needs to be escaped.
   *                      This array should have size of 128.
   *                      Negative byte values (or byte values >= 128)
   *                      are never escaped.
   * @throws IOException
   * @return true, if serialize a not-null object; otherwise false.
   */
  protected static boolean serialize(ByteStream.Output out, Object obj,
    ObjectInspector objInspector, byte[] separators, int level,
    Text nullSequence, boolean escaped, byte escapeChar,
    boolean[] needsEscape) throws IOException {

    switch (objInspector.getCategory()) {
      case PRIMITIVE: {
        LazyUtils.writePrimitiveUTF8(
          out, obj,
          (PrimitiveObjectInspector) objInspector,
          escaped, escapeChar, needsEscape);
        return true;
      }
      case LIST: {
        char separator = (char) separators[level];
        ListObjectInspector loi = (ListObjectInspector)objInspector;
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              out.write(separator);
            }
            serialize(out, list.get(i), eoi, separators, level + 1,
                nullSequence, escaped, escapeChar, needsEscape);
          }
        }
        return true;
      }
      case MAP: {
        char separator = (char) separators[level];
        char keyValueSeparator = (char) separators[level+1];
        MapObjectInspector moi = (MapObjectInspector) objInspector;
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();

        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
          return false;
        } else {
          boolean first = true;
          for (Map.Entry<?, ?> entry: map.entrySet()) {
            if (first) {
              first = false;
            } else {
              out.write(separator);
            }
            serialize(out, entry.getKey(), koi, separators, level+2,
                nullSequence, escaped, escapeChar, needsEscape);
            out.write(keyValueSeparator);
            serialize(out, entry.getValue(), voi, separators, level+2,
                nullSequence, escaped, escapeChar, needsEscape);
          }
        }
        return true;
      }
      case STRUCT: {
        char separator = (char)separators[level];
        StructObjectInspector soi = (StructObjectInspector)objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> list = soi.getStructFieldsDataAsList(obj);
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i<list.size(); i++) {
            if (i > 0) {
              out.write(separator);
            }
            serialize(out, list.get(i),
                fields.get(i).getFieldObjectInspector(), separators, level + 1,
                nullSequence, escaped, escapeChar, needsEscape);
          }
        }
        return true;
      }
    }

    throw new RuntimeException("Unknown category type: "
        + objInspector.getCategory());
  }

}
