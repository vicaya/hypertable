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
