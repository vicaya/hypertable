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

package org.hypertable.Hyperspace;

import java.net.ProtocolException;
import java.util.logging.Logger;
import org.hypertable.AsyncComm.ApplicationHandler;
import org.hypertable.AsyncComm.ApplicationQueue;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.DispatchHandler;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.Common.Error;

/**
 */
public class ConnectionHandler implements DispatchHandler {

    static final Logger log = Logger.getLogger("org.hypertable.Hyperspace");

    public ConnectionHandler(Comm comm, ApplicationQueue appQueue, Hyperspace hyperspace) {
	mComm = comm;
	mAppQueue = appQueue;
	mHyperspace = hyperspace;
    }

    public void handle(Event event) {
	short command;

	if (event.type == Event.Type.MESSAGE) {
	    //log.info(event.toString());

	    ApplicationHandler requestHandler;

	    event.msg.buf.position(event.msg.headerLen);
	    command = event.msg.buf.getShort();

	    switch (command) {
	    case Protocol.COMMAND_CREATE:
		requestHandler = new RequestHandlerCreate(mComm, mHyperspace, event);
		break;
	    case Protocol.COMMAND_DELETE:
		requestHandler = new RequestHandlerDelete(mComm, mHyperspace, event);
		break;
	    case Protocol.COMMAND_MKDIRS:
		requestHandler = new RequestHandlerMkdirs(mComm, mHyperspace, event);
		break;
	    case Protocol.COMMAND_ATTRSET:
		requestHandler = new RequestHandlerAttrSet(mComm, mHyperspace, event);
		break;
	    case Protocol.COMMAND_ATTRGET:
		requestHandler = new RequestHandlerAttrGet(mComm, mHyperspace, event);
		break;
	    case Protocol.COMMAND_ATTRDEL:
		requestHandler = new RequestHandlerAttrDel(mComm, mHyperspace, event);
		break;
	    case Protocol.COMMAND_EXISTS:
		requestHandler = new RequestHandlerExists(mComm, mHyperspace, event);
		break;
	    case Protocol.COMMAND_STATUS:
		requestHandler = new RequestHandlerStatus(mComm, mHyperspace, event);
		break;
	    default:
		ResponseCallback cb = new ResponseCallback(mComm, event);
		log.severe("Command code " + command + " not implemented");
		cb.error(Error.PROTOCOL_ERROR, "Command code " + command + " not implemented");
		return;
	    }

	    mAppQueue.Add(requestHandler);

	}
	else
	    log.info(event.toString());
    }

    private Comm mComm;
    private ApplicationQueue mAppQueue;
    private Hyperspace mHyperspace;
}

