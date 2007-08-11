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

package org.hypertable.Hypertable.Master;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.Message;
import org.hypertable.AsyncComm.HeaderBuilder;

public class Protocol extends org.hypertable.AsyncComm.Protocol {

    public CommBuf BuildRequestCreateTable(String tableName, String schema) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2 + CommBuf.EncodedLength(tableName) + CommBuf.EncodedLength(schema));
	cbuf.PrependString(schema);
	cbuf.PrependString(tableName);
	cbuf.PrependShort(COMMAND_CREATE_TABLE);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_MASTER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public CommBuf BuildRequestGetSchema(String tableName) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2 + CommBuf.EncodedLength(tableName));
	cbuf.PrependString(tableName);
	cbuf.PrependShort(COMMAND_GET_SCHEMA);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_MASTER);
	hbuilder.Encapsulate(cbuf);
	return cbuf;
    }

    public CommBuf BuildRequestBadCommand(short command) {
	HeaderBuilder hbuilder = new HeaderBuilder();
	CommBuf cbuf = new CommBuf(hbuilder.HeaderLength() + 2);
	cbuf.PrependShort(command);
	hbuilder.Reset(Message.PROTOCOL_HYPERTABLE_MASTER);
	hbuilder.Encapsulate(cbuf);
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

