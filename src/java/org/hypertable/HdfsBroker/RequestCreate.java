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
import java.util.logging.Logger;
import java.util.logging.Level;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.Message;

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

	    cbuf = new CommBuf(mOpenFileData.hbuilder.HeaderLength() + 10);
	    cbuf.PrependInt(mFileId);
	    cbuf.PrependShort(Protocol.COMMAND_CREATE);
	    cbuf.PrependInt(Error.OK);

	    // Encapsulate with Comm message response header
	    mOpenFileData.hbuilder.LoadFromMessage(mEvent.msg);
	    mOpenFileData.hbuilder.Encapsulate(cbuf);
	    
	    if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
		log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	    return;
	}
	catch (FileNotFoundException e) {
	    log.log(Level.WARNING, "File not found: " + mFilename);
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_OPEN, Error.HDFSBROKER_FILE_NOT_FOUND,
						      e.getMessage(), mOpenFileData.hbuilder.HeaderLength());
	}
	catch (IOException e) {
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_OPEN, Error.HDFSBROKER_IO_ERROR,
						      e.getMessage(), mOpenFileData.hbuilder.HeaderLength());
	}

	// Encapsulate with Comm message response header
	mOpenFileData.hbuilder.LoadFromMessage(mEvent.msg);
	mOpenFileData.hbuilder.Encapsulate(cbuf);

	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	
    }

    private String    mFilename;
    private boolean   mOverwrite;
    private short     mReplication;
    private int       mBufferSize;
    private long      mBlockSize;
}
