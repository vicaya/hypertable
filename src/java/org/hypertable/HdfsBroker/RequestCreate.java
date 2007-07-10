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

import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.ProtocolException;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.Message;
import org.hypertable.AsyncComm.MessageBuilderSimple;

import org.hypertable.Common.Error;


public class RequestCreate extends Request {


    public RequestCreate(OpenFileMap ofmap, Event event) throws ProtocolException {
	super(ofmap, event);
	short sval;
	int   ival;

	if (event.msg.buf.remaining() < 4)
	    throw new ProtocolException("Truncated message");

	sval = event.msg.buf.getShort();
	mOverwrite = (sval == 0) ? false : true;

	ival = event.msg.buf.getInt();
	mReplication = (short)ival;

	mBufferSize = event.msg.buf.getInt();

	mBlockSize = event.msg.buf.getLong();

	byte [] fileBytes = new byte [ event.msg.buf.remaining() ];
	event.msg.buf.get(fileBytes);

	mFilename = new String(fileBytes);

	mFileId = msUniqueId.incrementAndGet();

	mOpenFileData = mOpenFileMap.Create(mFileId);
    }

    public void run() {
	int error;
	CommBuf cbuf = null;

	try {

	    if (Global.verbose)
		log.info("Creating file '" + mFilename + "' handle = " + mFileId);

	    if (mReplication == -1)
		mReplication = Global.fileSystem.getDefaultReplication();

	    if (mBufferSize == -1)
		mBufferSize = Global.conf.getInt("io.file.buffer.size", 4096);

	    if (mBlockSize == -1)
		mBlockSize = Global.fileSystem.getDefaultBlockSize();

	    mOpenFileData.os = Global.fileSystem.create(new Path(mFilename),
							     mOverwrite, mBufferSize,
							     mReplication, mBlockSize);

	    cbuf = new CommBuf(mOpenFileData.mbuilder.HeaderLength() + 10);
	    cbuf.PrependInt(mFileId);
	    cbuf.PrependShort(Protocol.COMMAND_CREATE);
	    cbuf.PrependInt(Error.OK);

	    // Encapsulate with Comm message response header
	    mOpenFileData.mbuilder.LoadFromMessage(mEvent.msg);
	    mOpenFileData.mbuilder.Encapsulate(cbuf);
	    
	    if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
		log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	    return;
	}
	catch (FileNotFoundException e) {
	    log.log(Level.WARNING, "File not found: " + mFilename);
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_OPEN, Error.HDFSBROKER_FILE_NOT_FOUND,
						      e.getMessage(), mOpenFileData.mbuilder.HeaderLength());
	}
	catch (IOException e) {
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_OPEN, Error.HDFSBROKER_IO_ERROR,
						      e.getMessage(), mOpenFileData.mbuilder.HeaderLength());
	}

	// Encapsulate with Comm message response header
	mOpenFileData.mbuilder.LoadFromMessage(mEvent.msg);
	mOpenFileData.mbuilder.Encapsulate(cbuf);

	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	
    }

    private String    mFilename;
    private boolean   mOverwrite;
    private short     mReplication;
    private int       mBufferSize;
    private long      mBlockSize;
}
