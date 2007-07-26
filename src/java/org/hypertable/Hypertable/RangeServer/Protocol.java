/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hypertable.Hypertable.RangeServer;

import java.io.UnsupportedEncodingException;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.Message;
import org.hypertable.AsyncComm.HeaderBuilder;

public class Protocol extends org.hypertable.AsyncComm.Protocol {

    /**
     * Builds a LOAD RANGE request
     */
    public CommBuf BuildRequestLoadRange(int tableGeneration, RangeIdentifier range) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 6 + CommBuf.EncodedLength(range.tableName) +
				   CommBuf.EncodedLength(range.startRow) + CommBuf.EncodedLength(range.endRow));
	cbuf.PrependString(range.endRow);
	cbuf.PrependString(range.startRow);
	cbuf.PrependString(range.tableName);
	cbuf.PrependInt(tableGeneration);
	cbuf.PrependShort(COMMAND_LOAD_RANGE);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    /**
     * Builds an UPDATE request
     */
    public CommBuf BuildRequestUpdate(int tableGeneration, RangeIdentifier range, byte [] mods) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 6 + CommBuf.EncodedLength(range.tableName) +
				   CommBuf.EncodedLength(range.startRow) + CommBuf.EncodedLength(range.endRow) + 4 + mods.length);
	cbuf.PrependByteArray(mods);
	cbuf.PrependString(range.endRow);
	cbuf.PrependString(range.startRow);
	cbuf.PrependString(range.tableName);
	cbuf.PrependInt(tableGeneration);
	cbuf.PrependShort(COMMAND_UPDATE);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    /**
     * Builds a CREATE SCANNER request
     */
    public CommBuf BuildRequestCreateScanner(int tableGeneration, RangeIdentifier range, byte [] columns,
					     String startKey, String endKey, long startTime, long endTime) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 6 + CommBuf.EncodedLength(range.tableName) +
				   CommBuf.EncodedLength(range.startRow) + CommBuf.EncodedLength(range.endRow) +
				   4 + ((columns == null) ? 0 : columns.length) +
				   CommBuf.EncodedLength(startKey) + CommBuf.EncodedLength(endKey) + 16);
	cbuf.PrependLong(endTime);
	cbuf.PrependLong(startTime);
	cbuf.PrependString(endKey);
	cbuf.PrependString(startKey);
	cbuf.PrependByteArray(columns);
	cbuf.PrependString(range.endRow);
	cbuf.PrependString(range.startRow);
	cbuf.PrependString(range.tableName);
	cbuf.PrependInt(tableGeneration);
	cbuf.PrependShort(COMMAND_CREATE_SCANNER);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }


    /**
     * Builds a FETCH SCANBLOCK request
     */
    public CommBuf BuildRequestFetchScanblock(int scannerId) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 6);
	cbuf.PrependInt(scannerId);
	cbuf.PrependShort(COMMAND_FETCH_SCANBLOCK);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }


    /**
     * Builds a COMPACT request
     */
    public CommBuf BuildRequestCompact(int tableGeneration, RangeIdentifier range, boolean major, String localityGroup) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 6 + CommBuf.EncodedLength(range.tableName) +
				   CommBuf.EncodedLength(range.startRow) + CommBuf.EncodedLength(range.endRow) +
				   1 + CommBuf.EncodedLength(localityGroup));
	cbuf.PrependString(localityGroup);
	if (major)
	    cbuf.PrependByte((byte)1);
	else
	    cbuf.PrependByte((byte)0);
	cbuf.PrependString(range.endRow);
	cbuf.PrependString(range.startRow);
	cbuf.PrependString(range.tableName);
	cbuf.PrependInt(tableGeneration);
	cbuf.PrependShort(COMMAND_COMPACT);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public CommBuf BuildRequestBadCommand(short command) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2);
	cbuf.PrependShort(command);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_RANGESERVER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public static final short COMMAND_LOAD_RANGE     = 0;
    public static final short COMMAND_UPDATE          = 1;
    public static final short COMMAND_CREATE_SCANNER  = 2;
    public static final short COMMAND_FETCH_SCANBLOCK = 3;
    public static final short COMMAND_COMPACT         = 4;
    public static final short COMMAND_MAX             = 5;

    private static String msCommandStrings[] = {
	"load range",
	"update",
	"create scanner",
	"fetch scanblock",
	"compact"
    };
    
    public String CommandText(short command) {
	if (command < 0 || command >= COMMAND_MAX)
	    return "UNKNOWN (" + command + ")";
	return msCommandStrings[command];
    }

}

