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

package org.hypertable.Hyperspace;

import java.net.ProtocolException;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.Event;

public class RequestFactory {

    public Request newInstance(Event event, short command) throws ProtocolException {

	if (command < 0 || command >= Protocol.COMMAND_MAX)
	    throw new ProtocolException("Invalid command (" + command + ")");

	switch (command) {
	case Protocol.COMMAND_CREATE:
	    return new RequestCreate(event);
	case Protocol.COMMAND_MKDIRS:
	    return new RequestMkdirs(event);
	case Protocol.COMMAND_ATTRSET:
	    return new RequestAttrSet(event);
	case Protocol.COMMAND_ATTRGET:
	    return new RequestAttrGet(event);
	case Protocol.COMMAND_ATTRDEL:
	    return new RequestAttrDel(event);
	case Protocol.COMMAND_EXISTS:
	    return new RequestExists(event);
	default:
	    throw new ProtocolException("Command '" + Global.protocol.CommandText(command) + "' not implemented");
	}
    }

}
