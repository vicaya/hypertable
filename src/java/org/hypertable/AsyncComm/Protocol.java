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


package org.hypertable.AsyncComm;

import org.hypertable.Common.Error;

public abstract class Protocol {

    public static int ResponseCode(Event event) {
	event.msg.RewindToProtocolHeader();
	if (event.msg.buf.remaining() < 4)
	    return Error.RESPONSE_TRUNCATED;
	return event.msg.buf.getInt();
    }

    public String StringFormatMessage(Event event) {
	event.msg.RewindToProtocolHeader();
	if (event.type != Event.Type.MESSAGE)
	    return event.toString();

	if (event.msg.buf.remaining() < 4)
	    return "Message Truncated";

	int error = event.msg.buf.getInt();

	if (event.msg.buf.remaining() < 2)
	    return Error.GetText(error) + " - truncated";

	short command = event.msg.buf.getShort();

	if (error == Error.OK)
	    return "command='" + CommandText(command) + "'"; 
	else {
	    String str = CommBuf.DecodeString(event.msg.buf);

	    if (str == null)
		return Error.GetText(error) + " command='" + CommandText(command) + "' - truncated";

	    return "ERROR command='" + CommandText(command) + "' [" + Error.GetText(error) + "] " + str;
	}
    }

    public abstract String CommandText(short command);

    public CommBuf CreateErrorMessage(short command, int error, String msg, int extraSpace) {
	CommBuf cbuf = new CommBuf(extraSpace + 6 + CommBuf.EncodedLength(msg));
	cbuf.PrependString(msg);
	cbuf.PrependShort(command);
	cbuf.PrependInt(error);
	return cbuf;
    }

}
