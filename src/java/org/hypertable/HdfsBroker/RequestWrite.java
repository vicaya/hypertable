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

import org.apache.hadoop.fs.Path;

import java.nio.BufferUnderflowException;
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


public class RequestWrite extends Request {

    public RequestWrite(OpenFileMap ofmap, Event event) throws ProtocolException {
	super(ofmap, event);

	if (event.msg.buf.remaining() < 4)
	    throw new ProtocolException("Truncated message");

	mFileId = event.msg.buf.getInt();
	mAmount = event.msg.buf.getInt();

	mOpenFileData = mOpenFileMap.Get(mFileId);
    }

    public void run() {
	int error = Error.HDFSBROKER_IO_ERROR;
	CommBuf cbuf = null;
	HeaderBuilder hbuilder = null;

	try {

	    /**
	    if (Global.verbose)
		log.info("Write request handle=" + mFileId + " amount=" + mAmount);
	    */

	    if (mOpenFileData == null) {
		hbuilder = new HeaderBuilder();
		error = Error.HDFSBROKER_BAD_FILE_HANDLE;
		throw new IOException("Invalid file handle " + mFileId);
	    }
	    else
		hbuilder = mOpenFileData.hbuilder;
	    
	    if (mOpenFileData.os == null)
		throw new IOException("File handle " + mFileId + " not open for writing");

	    long offset = mOpenFileData.os.getPos();

	    byte [] data = new byte [ mAmount ];

	    mEvent.msg.buf.get(data);

	    mOpenFileData.os.write(data, 0, data.length);
	    
	    cbuf = new CommBuf(mOpenFileData.hbuilder.HeaderLength() + 22);
	    cbuf.PrependInt(mAmount);
	    cbuf.PrependLong(offset);
	    cbuf.PrependInt(mFileId);
	    cbuf.PrependShort(Protocol.COMMAND_WRITE);
	    cbuf.PrependInt(Error.OK);

	    // Encapsulate with Comm message response header
	    mOpenFileData.hbuilder.LoadFromMessage(mEvent.msg);
	    mOpenFileData.hbuilder.Encapsulate(cbuf);
	    
	    if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
		log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	    return;
	}
	catch (IOException e) {
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_READ, error,
						      e.getMessage(), hbuilder.HeaderLength());
	}
	catch (BufferUnderflowException e) {
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_READ, Error.PROTOCOL_ERROR,
						      e.getMessage(), mOpenFileData.hbuilder.HeaderLength());
	}

	// Encapsulate with Comm message response header
	hbuilder.LoadFromMessage(mEvent.msg);
	hbuilder.Encapsulate(cbuf);

	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
    }

    private int mAmount;

}
