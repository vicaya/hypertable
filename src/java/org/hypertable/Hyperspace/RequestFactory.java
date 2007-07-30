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
