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
import java.util.logging.Logger;

import org.hypertable.AsyncComm.DispatchHandler;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.HeaderBuilder;

import org.hypertable.Common.Error;


/**
 */
public class ConnectionHandler implements DispatchHandler {

    static final Logger log = Logger.getLogger("org.hypertable.Hyperspace");

    public void handle(Event event) {
	short command = -1;

	if (event.type == Event.Type.MESSAGE) {
	    //log.info(event.toString());
	    try {
		event.msg.buf.position(event.msg.headerLen);
		command = event.msg.buf.getShort();
		Global.workQueue.AddRequest( mRequestFactory.newInstance(event, command) );
	    }
	    catch (ProtocolException e) {
		HeaderBuilder hbuilder = new HeaderBuilder();
		CommBuf cbuf = Global.protocol.CreateErrorMessage(command, Error.PROTOCOL_ERROR, e.getMessage(), hbuilder.HeaderLength());

		// Encapsulate with Comm message response header
		hbuilder.LoadFromMessage(event.msg);
		hbuilder.Encapsulate(cbuf);

		Global.comm.SendResponse(event.addr, cbuf);
	    }
	}
	else
	    log.info(event.toString());
    }

    private RequestFactory mRequestFactory = new RequestFactory();
}
