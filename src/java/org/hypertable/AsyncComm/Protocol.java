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


package org.hypertable.AsyncComm;

import org.hypertable.Common.Error;

public abstract class Protocol {

    public static int ResponseCode(Event event) {
	event.msg.RewindToProtocolHeader();
	if (event.msg.buf.remaining() < 4)
	    return Error.RESPONSE_TRUNCATED;
	return event.msg.buf.getInt();
    }

    public static String StringFormatMessage(Event event) {
	event.msg.RewindToProtocolHeader();
	if (event.type != Event.Type.MESSAGE)
	    return event.toString();

	if (event.msg.buf.remaining() < 4)
	    return "Message Truncated";

	int error = event.msg.buf.getInt();

	if (error == Error.OK)
	    return Error.GetText(error);
	else {
	    String str = Serialization.DecodeString(event.msg.buf);

	    if (str == null)
		return Error.GetText(error) + " - truncated";

	    return Error.GetText(error) + " : " + str;
	}
    }

    public abstract String CommandText(short command);

    public static CommBuf CreateErrorMessage(HeaderBuilder hbuilder, int error, String msg) {
	CommBuf cbuf = new CommBuf(hbuilder, 4 + Serialization.EncodedLengthString(msg));
	cbuf.AppendInt(error);
	cbuf.AppendString(msg);
	return cbuf;
    }

}
