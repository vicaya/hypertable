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
import org.hypertable.AsyncComm.ApplicationQueue;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.Common.Error;

public class RequestHandlerShutdown extends ApplicationHandler {

    static final Logger log = Logger.getLogger("org.hypertable.DfsBroker.hadoop");

    public RequestHandlerShutdown(Comm comm, ApplicationQueue appQueue, Event event) {
	super(event);
	mComm = comm;
	mAppQueue = appQueue;
    }

    public void run() {
	ResponseCallback cb = new ResponseCallback(mComm, mEvent);

	try {

	    if (mEvent.msg.buf.remaining() < 2)
		throw new ProtocolException("Truncated message");

	    short flags = mEvent.msg.buf.getShort();

	    if ((flags & Protocol.SHUTDOWN_FLAG_IMMEDIATE) != 0) {
		log.info("Immediate shutdown.");
		System.exit(0);
	    }

	    mAppQueue.Shutdown();

	    cb.response_ok();

	    wait(2000);

	    System.exit(0);

	}
	catch (ProtocolException e) {
	    int error = cb.error(Error.PROTOCOL_ERROR, e.getMessage());
	    log.severe("Protocol error (SHUTDOWN) - " + e.getMessage());
	    if (error != Error.OK)
		log.severe("Problem sending (SHUTDOWN) error back to client - " + Error.GetText(error));
	}
	catch (InterruptedException e) {
	}
    }

    private Comm              mComm;
    private ApplicationQueue  mAppQueue;
}
