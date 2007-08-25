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

package org.hypertable.DfsBroker.hadoop;

import java.net.ProtocolException;
import java.util.logging.Logger;
import org.hypertable.AsyncComm.ApplicationHandler;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.Common.Error;

public class RequestHandlerRead extends ApplicationHandler {

    static final Logger log = Logger.getLogger("org.hypertable.DfsBroker.hadoop");

    public RequestHandlerRead(Comm comm, HdfsBroker broker, Event event) {
	super(event);
	mComm = comm;
	mBroker = broker;
    }

    public void run() {
	int     fd, amount;
	ResponseCallbackRead cb = new ResponseCallbackRead(mComm, mEvent);

	try {

	    if (mEvent.msg.buf.remaining() < 8)
		throw new ProtocolException("Truncated message");

	    fd = mEvent.msg.buf.getInt();

	    amount = mEvent.msg.buf.getInt();

	    mBroker.Read(cb, fd, amount);

	}
	catch (ProtocolException e) {
	    int error = cb.error(Error.PROTOCOL_ERROR, e.getMessage());
	    log.severe("Protocol error (READ) - " + e.getMessage());
	    if (error != Error.OK)
		log.severe("Problem sending (READ) error back to client - " + Error.GetText(error));
	}
    }

    private Comm       mComm;
    private HdfsBroker mBroker;
}
