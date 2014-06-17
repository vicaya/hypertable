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
import org.hypertable.hadoop.util.Serialization;
import org.hypertable.thrift.SerializedCellsFlag;
import org.hypertable.thrift.SerializedCellsWriter;
import org.hypertable.thriftgen.*;

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
  public static final String HT_NAMESPACE = "hypertable.table.namespace";
  public static final String HT_TABLE_NAME = "hypertable.table.name";
  public static final String HT_KEY_COL = ":key";

  public static final Log LOG = LogFactory.getLog(HTSerDe.class.getName());

  private ObjectInspector cachedObjectInspector;
  private LazyHTRow cachedHTRow;
  private Row serializeCache;

  private String htColumnsMapping;
  private List<String> htColumnFamilies;
  private List<byte []> htColumnFamiliesBytes;
  private List<String> htColumnQualifiers;
  private List<byte []> htColumnQualifiersBytes;
  private SerDeParameters serdeParams;
  private boolean useJSONSerialize;
  private final ByteStream.Output serializeStream = new ByteStream.Output();
  private int iKey;
  // used for serializing a field
  private byte [] separators;     // the separators array
  private boolean escaped;        // whether we need to escape the data when writing out
  private byte escapeChar;        // which char to use as the escape char, e.g. '\\'
  private boolean [] needsEscape; // which chars need to be escaped. This array should have size
                                  // of 128. Negative byte values (or byte values >= 128) are
                                  // never escaped.

  public String toString() {
    return getClass().toString()
        + "["
        + htColumnsMapping
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
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

    initHTSerDeParameters(conf, tbl, getClass().getName());

    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        serdeParams.getColumnNames(),
        serdeParams.getColumnTypes(),
        serdeParams.getSeparators(),
        serdeParams.getNullSequence(),
        serdeParams.isLastColumnTakesRest(),
        serdeParams.isEscaped(),
        serdeParams.getEscapeChar());

    cachedHTRow = new LazyHTRow(
      (LazySimpleStructObjectInspector) cachedObjectInspector);

    if (LOG.isDebugEnabled()) {
      LOG.debug("HTSerDe initialized with : columnNames = "
        + serdeParams.getColumnNames()
        + " columnTypes = "
        + serdeParams.getColumnTypes()
        + " htColumnMapping = "
        + htColumnsMapping);
    }
  }

  /**
   * Parses the HT columns mapping to identify the column families, qualifiers
   * and also caches the byte arrays corresponding to them. One of the Hive table
   * columns maps to the HT row key, by default the first column.
   *
   * @param columnMapping - the column mapping specification to be parsed
   * @param colFamilies - the list of HBase column family names
   * @param colFamiliesBytes - the corresponding byte array
   * @param colQualifiers - the list of HBase column qualifier names
   * @param colQualifiersBytes - the corresponding byte array
   * @return the row key index in the column names list
   * @throws SerDeException
   */
  public static int parseColumnMapping(
      String columnMapping,
      List<String> colFamilies,
      List<byte []> colFamiliesBytes,
      List<String> colQualifiers,
      List<byte []> colQualifiersBytes) throws SerDeException {

    int rowKeyIndex = -1;

    if (colFamilies == null || colQualifiers == null) {
      throw new SerDeException("Error: caller must pass in lists for the column families " +
          "and qualifiers.");
    }

    colFamilies.clear();
    colQualifiers.clear();

    if (columnMapping == null) {
      throw new SerDeException("Error: hypertable.columns.mapping missing for this" +
                               " Hypertable table.");
    }

    if (columnMapping.equals("") || columnMapping.equals(HT_KEY_COL)) {
      throw new SerDeException("Error: hypertable.columns.mapping specifies only the HT table"
          + " row key. A valid Hive-Hypertable table must specify at least one additional"
          + " column.");
    }

    String [] mapping = columnMapping.split(",");

    for (int i = 0; i < mapping.length; i++) {
      String elem = mapping[i];
      int idxFirst = elem.indexOf(":");
      int idxLast = elem.lastIndexOf(":");

      if (idxFirst < 0 || !(idxFirst == idxLast)) {
        throw new SerDeException("Error: the HT columns mapping contains a badly formed " +
            "column family, column qualifier specification.");
      }

      if (elem.equals(HT_KEY_COL)) {
        rowKeyIndex = i;
        colFamilies.add(elem);
        colQualifiers.add(null);
      } else {
        String [] parts = elem.split(":");
        assert(parts.length > 0 && parts.length <= 2);
        colFamilies.add(parts[0]);

        if (parts.length == 2) {
          colQualifiers.add(parts[1]);
        } else {
          colQualifiers.add(null);
        }
      }
    }

    if (rowKeyIndex == -1) {
      colFamilies.add(0, HT_KEY_COL);
      colQualifiers.add(0, null);
      rowKeyIndex = 0;
    }

    if (colFamilies.size() != colQualifiers.size()) {
      throw new SerDeException("Error in parsing the Hypertable columns mapping.");
    }

    // populate the corresponding byte [] if the client has passed in a non-null list
    if (colFamiliesBytes != null) {
      colFamiliesBytes.clear();

      for (String fam : colFamilies) {
        colFamiliesBytes.add(Serialization.toBytes(fam));
      }
    }

    if (colQualifiersBytes != null) {
      colQualifiersBytes.clear();

      for (String qual : colQualifiers) {
        if (qual == null) {
          colQualifiersBytes.add(null);
        } else {
          colQualifiersBytes.add(Serialization.toBytes(qual));
        }
      }
    }

    if (colFamiliesBytes != null && colQualifiersBytes != null) {
      if (colFamiliesBytes.size() != colQualifiersBytes.size()) {
        throw new SerDeException("Error in caching the bytes for the hypertable column" +
            " families and qualifiers.");
      }
    }

    return rowKeyIndex;
  }

  public static boolean isSpecialColumn(String htColumnName) {
    return htColumnName.equals(HT_KEY_COL);
  }

  private void initHTSerDeParameters(
      Configuration job, Properties tbl, String serdeName)
    throws SerDeException {
    // Read configuration parameters
    htColumnsMapping = tbl.getProperty(HTSerDe.HT_COL_MAPPING);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);

    // Parse the Hypertable columns mapping and initialize the col family & qualifiers
    htColumnFamilies = new ArrayList<String>();
    htColumnFamiliesBytes = new ArrayList<byte []>();
    htColumnQualifiers = new ArrayList<String>();
    htColumnQualifiersBytes = new ArrayList<byte []>();
    iKey = parseColumnMapping(htColumnsMapping, htColumnFamilies,
        htColumnFamiliesBytes, htColumnQualifiers, htColumnQualifiersBytes);

    // Build the type property string if not supplied
    if (columnTypeProperty == null) {
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < htColumnFamilies.size(); i++) {
        if (sb.length() > 0) {
          sb.append(":");
        }
        String colFamily = htColumnFamilies.get(i);
        String colQualifier = htColumnQualifiers.get(i);
        if (isSpecialColumn(colFamily)) {
            // the row key column becomes a STRING
            sb.append(Constants.STRING_TYPE_NAME);
        } else if (colQualifier == null)  {
          // a column family become a MAP
          sb.append(
            Constants.MAP_TYPE_NAME + "<"
            + Constants.STRING_TYPE_NAME
            + "," + Constants.STRING_TYPE_NAME + ">");
        } else {
          // an individual column becomes a STRING
          sb.append(Constants.STRING_TYPE_NAME);
        }
      }
      tbl.setProperty(Constants.LIST_COLUMN_TYPES, sb.toString());
    }

    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);

    if (htColumnFamilies.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " +
        serdeParams.getColumnNames().size() +
        " elements while hypertable.columns.mapping has " +
        htColumnFamilies.size() + " elements" +
        " (counting the key if implicit)");
    }

    separators = serdeParams.getSeparators();
    escaped = serdeParams.isEscaped();
    escapeChar = serdeParams.getEscapeChar();
    needsEscape = serdeParams.getNeedsEscape();

    // check that the mapping schema is right;
    // check that the "column-family:" is mapped to MAP<String,?>
    for (int i = 0; i < htColumnFamilies.size(); i++) {
      String colFamily = htColumnFamilies.get(i);
      String colQualifier = htColumnQualifiers.get(i);
      if (colQualifier == null && !isSpecialColumn(colFamily)) {
        TypeInfo typeInfo = serdeParams.getColumnTypes().get(i);
        if ((typeInfo.getCategory() != Category.MAP) ||
          (((MapTypeInfo) typeInfo).getMapKeyTypeInfo().getTypeName()
            !=  Constants.STRING_TYPE_NAME)) {

          throw new SerDeException(
            serdeName + ": Hypertable column family '"
            + colFamily
            + "' should be mapped to Map<String,?> but is mapped to "
            + typeInfo.getTypeName());
        }
      }
    }
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
    cachedHTRow.init(rr, htColumnFamilies, htColumnFamiliesBytes,
        htColumnQualifiers, htColumnQualifiersBytes);

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
      (serdeParams.getRowTypeInfo() != null &&
        ((StructTypeInfo) serdeParams.getRowTypeInfo())
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
      byte [] key = serializeField(iKey, null, null, fields, list, declaredFields);

      if (key == null) {
        throw new SerDeException("Hypertable row key cannot be NULL");
      }
      // Serialize each field
      for (int i = 0; i < fields.size(); i++) {
        if (i == iKey) {
          // already processed the key above
          continue;
        }
        serializeField(i, key, cellWriter, fields, list, declaredFields);
      }
      // set the SerializedCells into a Row object
      serializeCache = new Row(cellWriter.array());
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return serializeCache;
  }

  private byte [] serializeField(
    int i,
    byte [] rowkey,
    SerializedCellsWriter cellWriter,
    List<? extends StructField> fields,
    List<Object> list,
    List<? extends StructField> declaredFields) throws IOException {

    // column name
    String htColumnFamily = htColumnFamilies.get(i);
    String htColumnQualifier = htColumnQualifiers.get(i);

    // Get the field objectInspector and the field object.
    ObjectInspector foi = fields.get(i).getFieldObjectInspector();
    Object f = (list == null ? null : list.get(i));

    if (f == null) {
      // a null object, we do not serialize it
      return null;
    }

    // If the field corresponds to a column family in Hypertable
    if (htColumnQualifier == null && !isSpecialColumn(htColumnFamily)) {
      MapObjectInspector moi = (MapObjectInspector)foi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(f);
      if (map == null) {
        return null;
      } else {
        for (Map.Entry<?, ?> entry: map.entrySet()) {
          // Get the Key
          serializeStream.reset();
          serialize(entry.getKey(), koi, 3);

          // Get the column-qualifier
          byte [] columnQualifierBytes = new byte[serializeStream.getCount()];
          System.arraycopy(serializeStream.getData(), 0, columnQualifierBytes,
                           0, serializeStream.getCount());

          // Get the Value
          serializeStream.reset();
          boolean isNotNull = serialize(entry.getValue(), voi, 3);
          if (!isNotNull) {
            continue;
          }
          byte [] value = new byte[serializeStream.getCount()];
          System.arraycopy(serializeStream.getData(), 0, value, 0, serializeStream.getCount());

          cellWriter.add(rowkey, 0, rowkey.length,
                         htColumnFamiliesBytes.get(i), 0, htColumnFamiliesBytes.get(i).length,
                         columnQualifierBytes, 0, columnQualifierBytes.length,
                         SerializedCellsFlag.AUTO_ASSIGN,
                         value, 0, value.length, SerializedCellsFlag.FLAG_INSERT);
        }
      }
    } else {
      // If the field that is passed in is NOT a primitive, and either the
      // field is not declared (no schema was given at initialization), or
      // the field is declared as a primitive in initialization, serialize
      // the data to JSON string.  Otherwise serialize the data in the
      // delimited way.
      serializeStream.reset();
      boolean isNotNull;
      if (!foi.getCategory().equals(Category.PRIMITIVE)
          && (declaredFields == null ||
              declaredFields.get(i).getFieldObjectInspector().getCategory()
              .equals(Category.PRIMITIVE) || useJSONSerialize)) {

        isNotNull = serialize(
            SerDeUtils.getJSONString(f, foi),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            1);
      } else {
        isNotNull = serialize(f, foi, 1);
      }
      if (!isNotNull) {
        return null;
      }
      byte [] rowkey_or_value = new byte[serializeStream.getCount()];
      System.arraycopy(serializeStream.getData(), 0, rowkey_or_value, 0,
                       serializeStream.getCount());
      if (i == iKey) {
        return rowkey_or_value;
      }
      cellWriter.add(rowkey, 0, rowkey.length,
                     htColumnFamiliesBytes.get(i), 0, htColumnFamiliesBytes.get(i).length,
                     htColumnQualifiersBytes.get(i), 0, htColumnQualifiersBytes.get(i).length,
                     SerializedCellsFlag.AUTO_ASSIGN,
                     rowkey_or_value, 0, rowkey_or_value.length,
                     SerializedCellsFlag.FLAG_INSERT );
    }

    return null;
  }

  /**
   * Serialize the row into a ByteStream.
   *
   * @param obj           The object for the current field.
   * @param objInspector  The ObjectInspector for the current Object.
   * @param level         The current level of separator.
   * @throws IOException
   * @return true, if serialize is a not-null object; otherwise false.
   */
  private boolean serialize(Object obj, ObjectInspector objInspector, int level)
      throws IOException {

    switch (objInspector.getCategory()) {
      case PRIMITIVE: {
        LazyUtils.writePrimitiveUTF8(
          serializeStream, obj,
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
              serializeStream.write(separator);
            }
            serialize(list.get(i), eoi, level + 1);
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
              serializeStream.write(separator);
            }
            serialize(entry.getKey(), koi, level+2);
            serializeStream.write(keyValueSeparator);
            serialize(entry.getValue(), voi, level+2);
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
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              serializeStream.write(separator);
            }
            serialize(list.get(i), fields.get(i).getFieldObjectInspector(), level + 1);
          }
        }
        return true;
      }
    }

    throw new RuntimeException("Unknown category type: " + objInspector.getCategory());
  }


  /**
   * @return the useJSONSerialize
   */
  public boolean isUseJSONSerialize() {
    return useJSONSerialize;
  }

  /**
   * @param useJSONSerialize the useJSONSerialize to set
   */
  public void setUseJSONSerialize(boolean useJSONSerialize) {
    this.useJSONSerialize = useJSONSerialize;
  }

  /**
   * @return 0-based offset of the key column within the table
   */
  int getKeyColumnOffset() {
    return iKey;
  }

}
