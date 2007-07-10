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


package org.hypertable.Hypertable.Master;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.Message;
import org.hypertable.AsyncComm.MessageBuilderSimple;

public class Protocol extends org.hypertable.AsyncComm.Protocol {

    public CommBuf BuildRequestCreateTable(String tableName, String schema) {
	MessageBuilderSimple mbuilder = new MessageBuilderSimple();
	CommBuf cbuf = new CommBuf(mbuilder.HeaderLength() + 2 + CommBuf.EncodedLength(tableName) + CommBuf.EncodedLength(schema));
	cbuf.PrependString(schema);
	cbuf.PrependString(tableName);
	cbuf.PrependShort(COMMAND_CREATE_TABLE);
	mbuilder.Reset(Message.PROTOCOL_HYPERTABLE_MASTER);
	mbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public CommBuf BuildRequestGetSchema(String tableName) {
	MessageBuilderSimple mbuilder = new MessageBuilderSimple();
	CommBuf cbuf = new CommBuf(mbuilder.HeaderLength() + 2 + CommBuf.EncodedLength(tableName));
	cbuf.PrependString(tableName);
	cbuf.PrependShort(COMMAND_GET_SCHEMA);
	mbuilder.Reset(Message.PROTOCOL_HYPERTABLE_MASTER);
	mbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public CommBuf BuildRequestBadCommand(short command) {
	MessageBuilderSimple mbuilder = new MessageBuilderSimple();
	CommBuf cbuf = new CommBuf(mbuilder.HeaderLength() + 2);
	cbuf.PrependShort(command);
	mbuilder.Reset(Message.PROTOCOL_HYPERTABLE_MASTER);
	mbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public static final short COMMAND_CREATE_TABLE = 0;
    public static final short COMMAND_GET_SCHEMA   = 1;
    public static final short COMMAND_MAX          = 2;

    private static String msCommandStrings[] = {
	"create table",
	"get schema"
    };
    
    public String CommandText(short command) {
	if (command < 0 || command >= COMMAND_MAX)
	    return "UNKNOWN (" + command + ")";
	return msCommandStrings[command];
    }

}

