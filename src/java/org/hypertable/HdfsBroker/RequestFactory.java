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


package org.hypertable.HdfsBroker;

import java.net.ProtocolException;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.Event;

public class RequestFactory {

    public RequestFactory(OpenFileMap ofmap) {
	mOpenFileMap = ofmap;
    }

    public Request newInstance(Event event, short command) throws ProtocolException {

	switch (command) {
	case Protocol.COMMAND_OPEN:
	    return new RequestOpen(mOpenFileMap, event);
	case Protocol.COMMAND_CREATE:
	    return new RequestCreate(mOpenFileMap, event);
	case Protocol.COMMAND_CLOSE:
	    return new RequestClose(mOpenFileMap, event);
	case Protocol.COMMAND_READ:
	    return new RequestRead(mOpenFileMap, event);
	case Protocol.COMMAND_WRITE:
	    return new RequestWrite(mOpenFileMap, event);
	case Protocol.COMMAND_SEEK:
	    return new RequestSeek(mOpenFileMap, event);
	case Protocol.COMMAND_REMOVE:
	    return new RequestRemove(mOpenFileMap, event);
	case Protocol.COMMAND_SHUTDOWN:
	    return new RequestShutdown(mOpenFileMap, event);
	case Protocol.COMMAND_LENGTH:
	    return new RequestLength(mOpenFileMap, event);
	case Protocol.COMMAND_PREAD:
	    return new RequestPositionRead(mOpenFileMap, event);
	case Protocol.COMMAND_MKDIRS:
	    return new RequestMkdirs(mOpenFileMap, event);
	default:
	    throw new ProtocolException("Command '" + Global.protocol.CommandText(command) + "' not implemented");
	}
    }

    private OpenFileMap mOpenFileMap;
}
