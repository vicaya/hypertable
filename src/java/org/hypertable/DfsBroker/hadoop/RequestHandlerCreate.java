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
import org.hypertable.AsyncComm.Serialization;
import org.hypertable.AsyncComm.Event;
import org.hypertable.Common.Error;

public class RequestHandlerCreate extends ApplicationHandler {

    static final Logger log = Logger.getLogger("org.hypertable.DfsBroker.hadoop");

    public RequestHandlerCreate(Comm comm, HdfsBroker broker, Event event) {
	super(event);
	mComm = comm;
	mBroker = broker;
    }

    public void run() {
	ResponseCallbackCreate cb = new ResponseCallbackCreate(mComm, mEvent);
	short sval, replication;
	int   fd, ival, bufferSize;
	long  blockSize;
	boolean overwrite;
	String  fileName;

	try {

	    if (mEvent.msg.buf.remaining() < 18)
		throw new ProtocolException("Truncated message");

	    sval = mEvent.msg.buf.getShort();
	    overwrite = (sval == 0) ? false : true;

	    ival = mEvent.msg.buf.getInt();
	    replication = (short)ival;

	    bufferSize = mEvent.msg.buf.getInt();
	
	    blockSize = mEvent.msg.buf.getLong();

	    if ((fileName = Serialization.DecodeString(mEvent.msg.buf)) == null)
		throw new ProtocolException("Filename not properly encoded in request packet");

	    mBroker.Create(cb, fileName, overwrite, bufferSize, replication, blockSize);
	}
	catch (ProtocolException e) {
	    int error = cb.error(Error.PROTOCOL_ERROR, e.getMessage());
	    log.severe("Protocol error (CREATE) - " + e.getMessage());
	    if (error != Error.OK)
		log.severe("Problem sending (CREATE) error back to client - " + Error.GetText(error));
	}
    }

    private Comm       mComm;
    private HdfsBroker mBroker;
}
