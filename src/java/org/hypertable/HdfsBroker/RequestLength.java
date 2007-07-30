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


public class RequestLength extends Request {

    public RequestLength(OpenFileMap ofmap, Event event) throws ProtocolException {
	super(ofmap, event);

	if ((mFilename = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Filename not properly encoded in request packet");

    }

    public void run() {
	int error;
	CommBuf cbuf = null;
	long length;
	HeaderBuilder hbuilder = new HeaderBuilder();

	try {

	    if (Global.verbose)
		log.info("Getting length of file '" + mFilename);

	    length = Global.fileSystem.getLength(new Path(mFilename));

	    cbuf = new CommBuf(hbuilder.HeaderLength() + 14);
	    cbuf.PrependLong(length);
	    cbuf.PrependShort(Protocol.COMMAND_LENGTH);
	    cbuf.PrependInt(Error.OK);

	    // Encapsulate with Comm message response header
	    hbuilder.LoadFromMessage(mEvent.msg);
	    hbuilder.Encapsulate(cbuf);
	    
	    if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
		log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	    return;
	}
	catch (FileNotFoundException e) {
	    log.log(Level.WARNING, "File not found: " + mFilename);
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_LENGTH, Error.HDFSBROKER_FILE_NOT_FOUND,
						      e.getMessage(), hbuilder.HeaderLength());
	}
	catch (IOException e) {
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_LENGTH, Error.HDFSBROKER_IO_ERROR,
						      e.getMessage(), hbuilder.HeaderLength());
	}

	// Encapsulate with Comm message response header
	hbuilder.LoadFromMessage(mEvent.msg);
	hbuilder.Encapsulate(cbuf);

	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	
    }

    private String        mFilename;
}
