/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */




package org.hypertable.Hypertable.RangeServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.StringTokenizer;
import java.text.ParseException;

import org.hypertable.Common.FileUtils;

import org.hypertable.Hypertable.ColumnFamily;
import org.hypertable.Hypertable.Key;
import org.hypertable.Hypertable.Schema;

import org.hypertable.Common.Usage;

public class TestSource {

    public TestSource(String fname, Schema schema) {
	mSchema = schema;
	mCurLine = 0;
	try {
	    mInput = new BufferedReader(new FileReader(fname));
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public byte [] Next() throws IOException {
	String line;
	ByteBuffer bbuf;
	long timestamp;
	String rowKey;
	String column;
	String value;

	while ((line = mInput.readLine()) != null) {
	    mCurLine++;
	    try {
		line = line.trim();
		if (line.length() == 0)
		    continue;
		StringTokenizer st = new StringTokenizer(line, "\t");

		if (st.hasMoreTokens()) {
		    timestamp = Long.parseLong(st.nextToken());
		}
		else {
		    System.err.println("Mal-formed input on line " + (mCurLine-1));
		    continue;
		}

		if (st.hasMoreTokens()) {
		    rowKey = st.nextToken();
		}
		else {
		    System.err.println("Mal-formed input on line " + (mCurLine-1));
		    continue;
		}

		if (st.hasMoreTokens()) {
		    column = st.nextToken();
		}
		else {
		    System.err.println("Mal-formed input on line " + (mCurLine-1));
		    continue;
		}

		if (column.equals("DELETE"))
		    return CreateRowDelete(rowKey, timestamp);

		if (st.hasMoreTokens()) {
		    value = st.nextToken();
		    if (value.equals("DELETE")) {
			return CreateColumnDelete(rowKey, column, timestamp);
		    }
		}
		else
		    value = null;

		return CreateInsert(rowKey, column, timestamp, value);
		
	    }
	    catch (Exception e) {
		System.err.println("Error on line " + (mCurLine-1) + " " + e.toString());
	    }
	}
	return null;
    }


    /**
     *  keyLen    - 4 bytes
     *  rowKey    - variable
     *  '\0'      - 1 byte (null terminator)
     *  family    - 1 byte
     *  '\0'      - 1 byte (empty qualifier)
     *  flags     - 1 byte
     *  timestamp - 8 bytes
     *  valueLen  - 4 bytes
     */
    private byte [] CreateRowDelete(String row, long timestamp) {
	int rowLen = row.getBytes().length + 12;
	ByteBuffer buf = ByteBuffer.allocate(4 + rowLen + 4);
	buf.order(ByteOrder.LITTLE_ENDIAN);
	buf.putInt(rowLen);
	buf.put(row.getBytes());
	buf.put((byte)0);
	buf.put((byte)0);
	buf.put((byte)0);
	buf.put(Key.FLAG_DELETE_ROW);
	buf.order(ByteOrder.BIG_ENDIAN);
	timestamp = ~timestamp;
	buf.putLong(timestamp);
	buf.putInt(0);
	byte [] data = new byte [ buf.position() ];
	buf.flip();
	buf.get(data);
	return data;
    }

    /**
     *  keyLen    - 4 bytes
     *  rowKey    - variable
     *  '\0'      - 1 byte (null terminator)
     *  family    - 1 byte
     *  qualifier - variable
     *  '\0'      - 1 byte (empty qualifier)
     *  flags     - 1 byte
     *  timestamp - 8 bytes
     *  valueLen  - 4 bytes
     */
    private byte [] CreateColumnDelete(String row, String column, long timestamp) throws ParseException {
	int colon = column.indexOf(':');

	if (colon == -1)
	    throw new ParseException("Bad column specifier (no family) - " + column, 0);

	String family = column.substring(0, colon);
	String qualifier = column.substring(colon+1);

	ColumnFamily cf = mSchema.GetColumnFamily(family);

	if (cf == null)
	    throw new ParseException("Unrecognized column family - " + family, 0);
	    
	int rowLen = row.getBytes().length + qualifier.getBytes().length + 12;

	ByteBuffer buf = ByteBuffer.allocate(4 + rowLen + 4);
	buf.order(ByteOrder.LITTLE_ENDIAN);
	buf.putInt(rowLen);
	buf.put(row.getBytes());
	buf.put((byte)0);
	buf.put((byte)cf.id);
	buf.put(qualifier.getBytes());
	buf.put((byte)0);
	buf.put(Key.FLAG_DELETE_CELL);
	buf.order(ByteOrder.BIG_ENDIAN);
	timestamp = ~timestamp;
	buf.putLong(timestamp);
	buf.putInt(0);
	byte [] data = new byte [ buf.position() ];
	buf.flip();
	buf.get(data);
	return data;
    }


    /**
     *  keyLen    - 4 bytes
     *  rowKey    - variable
     *  '\0'      - 1 byte (null terminator)
     *  family    - 1 byte
     *  qualifier - variable
     *  '\0'      - 1 byte (empty qualifier)
     *  flags     - 1 byte
     *  timestamp - 8 bytes
     *  valueLen  - 4 bytes
     *  value     - variable;
     */
    private byte [] CreateInsert(String row, String column, long timestamp, String value) throws ParseException {
	int colon = column.indexOf(':');

	if (colon == -1)
	    throw new ParseException("Bad column specifier (no family) - " + column, 0);

	String family = column.substring(0, colon);
	String qualifier = column.substring(colon+1);

	ColumnFamily cf = mSchema.GetColumnFamily(family);

	if (cf == null)
	    throw new ParseException("Unrecognized column family - " + family, 0);
	    
	int rowLen = row.getBytes().length + qualifier.getBytes().length + 12;

	ByteBuffer buf = ByteBuffer.allocate(4 + rowLen + 4 + value.getBytes().length);
	buf.order(ByteOrder.LITTLE_ENDIAN);
	buf.putInt(rowLen);
	buf.put(row.getBytes());
	buf.put((byte)0);
	buf.put((byte)cf.id);
	buf.put(qualifier.getBytes());
	buf.put((byte)0);
	buf.put(Key.FLAG_INSERT);
	buf.order(ByteOrder.BIG_ENDIAN);
	timestamp = ~timestamp;
	buf.putLong(timestamp);
	buf.order(ByteOrder.LITTLE_ENDIAN);
	buf.putInt(value.getBytes().length);
	buf.put(value.getBytes());
	byte [] data = new byte [ buf.position() ];
	buf.flip();
	buf.get(data);
	return data;
    }


    static String usage[] = {
	"usage: TestSource <schemaFile> <dataFile>",
	"",
	"This program serves as a test driver for the TestSource class.  Test data is ",
	"read from <dataFile> and converted into Key/Value pairs as required by the Hypertable",
	"system.  Here are some examples of the input file format (note: fields are separated by",
	"\\t character):",
	"",
	"10002	abacinate	DELETE",
	"10004	abacinate	content:foo	Content 4",
	"10006	Batonga	location:baby	Content 6",
	"10007	Batonga	location:aunt	DELETE",
	"",
	"The first field is the timestamp, the second field is the row key.",
	"",
	null
    };


    public static void main(String []args) {

	if (args.length != 2)
	    Usage.DumpAndExit(usage);

	try {
	    byte [] bytes = FileUtils.FileToBuffer(new File(args[0]));
	    String schemaStr = new String(bytes);

	    Schema schema = new Schema(schemaStr);

	    TestSource testSource = new TestSource(args[1], schema);

	    while ((bytes = testSource.Next()) != null) {
		System.out.println("KV length = " + bytes.length);
	    }
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private Schema mSchema;
    private BufferedReader mInput;
    private long mCurLine;
}
