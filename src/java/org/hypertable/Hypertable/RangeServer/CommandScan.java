/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.hypertable.Hypertable.RangeServer;

import java.io.File;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.StringTokenizer;
import java.util.Vector;

import static java.lang.System.out;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.Event;

import org.hypertable.Hypertable.ColumnFamily;
import org.hypertable.Hypertable.Key;
import org.hypertable.Hypertable.ParseCommandLine;
import org.hypertable.Hypertable.Schema;

class CommandScan extends Command {

    private String mCommandLine;

    public CommandScan(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	int msgId;
	ParseCommandLine.Argument arg;

	try {
	    if (mArgs.size() == 0) {
		DisplayUsage();
		return;
	    }

	    arg = mArgs.elementAt(0);

	    if (arg.value != null) {
		DisplayUsage();
		return;
	    }

	    RangeIdentifier range = new RangeIdentifier(arg.name);

	    java.lang.System.out.println("Table Name        = " + range.tableName);
	    java.lang.System.out.println("Start Row         = " + range.startRow);
	    java.lang.System.out.println("End Row           = " + range.endRow);

	    String schemaFile = range.tableName + ".xml";
	    java.lang.System.out.println("Schema File       = " + schemaFile);
	    byte [] schemaBytes = FileUtils.FileToBuffer(new File(schemaFile));
	    msOutstandingSchema = new Schema(new String(schemaBytes));

	    String startKey = null;
	    String endRow = null;
	    byte [] columns = null;
	    long startTime = 0;
	    long endTime = 0;

	    for (int i=1; i<mArgs.size(); i++) {

		arg = mArgs.elementAt(i);

		if (arg.name.equals("start")) {
		    startKey = arg.value;
		}
		else if (arg.name.equals("end")) {
		    endRow = arg.value;
		}
		else if (arg.name.equals("columns")) {
		    if ((columns = ParseColumnsArgument( arg.value )) == null)
			return;
		}
		else if (arg.name.equals("timeInterval")) {
		    if (arg.value != null) {
			if (arg.value.startsWith("..")) {
			    endTime = Long.parseLong(arg.value.substring(2));
			}
			else if (arg.value.endsWith("..")) {
			    startTime = Long.parseLong(arg.value.substring(0, arg.value.length()-2));
			}
			else if (arg.value.indexOf("..") != -1) {
			    int offset = arg.value.indexOf("..");
			    startTime = Long.parseLong(arg.value.substring(0, offset));
			    endTime = Long.parseLong(arg.value.substring(offset+2));
			}
			else {
			    ReportError("Invalid timeInterval specifier");
			    return;
			}
		    }
		}
		else {
		    DisplayUsage();
		    return;
		}
	    }

	    java.lang.System.out.println("Schema Generation = " + msOutstandingSchema.GetGeneration());
	    java.lang.System.out.println("Start Key         = " + startKey);
	    java.lang.System.out.println("End Key           = " + endRow);
	    java.lang.System.out.println("Start Time        = " + startTime);
	    java.lang.System.out.println("End Time          = " + endTime);
	    if (columns != null) {
		for (int i=0; i<columns.length; i++) {
		    java.lang.System.out.println("Column            = " + (int)columns[i]);
		}
	    }

	    Global.client.CreateScanner(msOutstandingSchema.GetGeneration(), range, columns, startKey, endRow, startTime, endTime, mSyncHandler);

	    Event event = mSyncHandler.WaitForEvent();

	    if (event == null)
		System.err.println("ERROR: Timed out waiting for response from Hypertable Master");
	    else if (Global.protocol.ResponseCode(event) != Error.OK) {
		System.err.println(Global.protocol.StringFormatMessage(event));
	    }
	    else {
		event.msg.RewindToProtocolHeader();
		int error = event.msg.buf.getInt();
		short flags = event.msg.buf.getShort();
		int id = event.msg.buf.getInt();
		Command.msOutstandingScanId = (flags == 1) ? -1 : id;
		int dataLen = event.msg.buf.getInt();
		out.println("success");
		out.println("flags = " + flags);
		out.println("scanner id  = " + id);
		out.println("data len = " + dataLen);
		byte [] keyBytes;
		byte [] valueBytes;
		int len;
		while (event.msg.buf.remaining() > 0) {
		    // key
		    len = event.msg.buf.getInt();
		    keyBytes = new byte [ len ];
		    event.msg.buf.get(keyBytes);
		    // value
		    len = event.msg.buf.getInt();
		    valueBytes = new byte [ len ];
		    event.msg.buf.get(valueBytes);
		    Display(keyBytes, valueBytes);
		}
	    }
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
    }

    ByteBuffer mTimestampBuffer = ByteBuffer.allocate(8);

    private void Display(byte [] keyBytes, byte [] valueBytes) {
	int base, i = 0;
	long timestamp;
	byte flag;
	while (keyBytes[i] != 0)
	    i++;
	String rowKey = new String(keyBytes, 0, i);
	i++;
	int family = keyBytes[i];
	i++;
	base = i;
	while (keyBytes[i] != 0)
	    i++;
	String qualifier = new String(keyBytes, base, (i-base));
	i++;
	flag = keyBytes[i];
	i++;

	mTimestampBuffer.order(ByteOrder.BIG_ENDIAN);
	mTimestampBuffer.clear();
	mTimestampBuffer.put(keyBytes, i, 8);
	mTimestampBuffer.flip();
	timestamp = mTimestampBuffer.getLong();
	timestamp = ~timestamp;

	if (flag == Key.FLAG_DELETE_ROW) {
	    out.println(timestamp + " " + rowKey + " DELETE");
	}
	else {
	    if (family > 0) {
		ColumnFamily cf = msOutstandingSchema.GetColumnFamily(family);
		if (flag == Key.FLAG_DELETE_CELL)
		    out.println(timestamp + " " + rowKey + " " + cf.name + ":" + qualifier + " DELETE");
		else
		    out.println(timestamp + " " + rowKey + " " + cf.name + ":" + qualifier);
	    }
	    else {
		out.println("Bad column family (" + (int)family + ") for row key " + rowKey);
	    }
	}
    }

    private byte [] ParseColumnsArgument( String columns ) {
	byte [] tempColumnVect = new byte [ 256 ];
	int nextColumn = 0;
	ColumnFamily cf;
	String name;

	StringTokenizer st = new StringTokenizer(columns, ",");
	while (st.hasMoreTokens()) {
	    name = st.nextToken();
	    if ((cf = msOutstandingSchema.GetColumnFamily(name)) == null) {
		ReportError("Unrecognized column family '" + name + "'");
		return null;
	    }
	    tempColumnVect[nextColumn++] = (byte)cf.id;
	}
	byte [] returnVect = new byte [ nextColumn ];
	System.arraycopy(tempColumnVect, 0, returnVect, 0, nextColumn);
	return returnVect;
    }

    private void DisplayUsage() {
	ReportError("usage: scan <range> [start=<key>] [end=<key>] [columns=<column1>[,<column2>...] [timeInterval=<startTime>..<stopTime>] ");
    }

}



