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

import org.apache.hadoop.fs.Path;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.ProtocolException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.Message;
import org.hypertable.AsyncComm.HeaderBuilder;

import org.hypertable.Common.Error;


public class RequestMkdirs extends Request {

    public RequestMkdirs(OpenFileMap ofmap, Event event) throws ProtocolException {
	super(ofmap, event);

	if ((mFilename = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Filename not properly encoded in request packet");

    }

    public void run() {
	int error;
	CommBuf cbuf = null;
	HeaderBuilder hbuilder = new HeaderBuilder();

	try {

	    if (Global.verbose)
		log.info("Making directory '" + mFilename + "'");

	    if (!Global.fileSystem.mkdirs(new Path(mFilename)))
		throw new IOException("Problem creating directory '" + mFilename + "'");

	    cbuf = new CommBuf(hbuilder.HeaderLength() + 6);
	    cbuf.PrependShort(Protocol.COMMAND_CREATE);
	    cbuf.PrependInt(Error.OK);

	    // Encapsulate with Comm message response header
	    hbuilder.LoadFromMessage(mEvent.msg);
	    hbuilder.Encapsulate(cbuf);
	    
	    if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
		log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	    return;
	}
	catch (IOException e) {
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_OPEN, Error.HDFSBROKER_IO_ERROR,
						      e.getMessage(), hbuilder.HeaderLength());
	}

	// Encapsulate with Comm message response header
	hbuilder.LoadFromMessage(mEvent.msg);
	hbuilder.Encapsulate(cbuf);

	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	
    }

    private String    mFilename;
}
