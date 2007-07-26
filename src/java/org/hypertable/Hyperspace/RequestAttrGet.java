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
package org.hypertable.Hyperspace;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;

import org.hypertable.Common.Error;


public class RequestAttrGet extends Request {

    public RequestAttrGet(Event event) throws ProtocolException {
	super(event);

	if ((mFilename = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Filename not encoded properly in request packet");

	if ((mAttrName = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Attribute name not encoded properly in request packet");	    
    }

    public void run() {
	int error;
	CommBuf cbuf = null;
	ReentrantReadWriteLock rwlock = Global.lockMap.Get(mFilename);
	File attrFile;

	try {

	    if (Global.verbose)
		log.info("AttrSet file=" + mFilename + " attr=" + mAttrName);

	    rwlock.readLock().lock();

	    String baseDir = mFilename.startsWith("/") ? Global.baseDir : Global.baseDir + "/";

	    if (!(new File(baseDir + mFilename)).exists())
		throw new FileNotFoundException(mFilename);

	    attrFile = new File(baseDir + mFilename + ".attr." + mAttrName);

	    if (!attrFile.exists()) {
		cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_ATTRGET, Error.HYPERTABLEFS_ATTR_NOT_FOUND,
							  mAttrName, mHeaderBuilder.HeaderLength());
		mHeaderBuilder.LoadFromMessage(mEvent.msg);
		mHeaderBuilder.Encapsulate(cbuf);
		if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
		    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
		return;
	    }

	    long length = attrFile.length();

	    byte [] attrBytes = new byte [ (int)length ];

	    FileInputStream is = new FileInputStream(attrFile);
	    is.read(attrBytes, 0, attrBytes.length);
	    is.close();

	    String attrValue = new String(attrBytes);

	    cbuf = new CommBuf(mHeaderBuilder.HeaderLength() + 6 + CommBuf.EncodedLength(attrValue));
	    cbuf.PrependString(attrValue);
	    cbuf.PrependShort(Protocol.COMMAND_ATTRGET);
	    cbuf.PrependInt(Error.OK);

	    // Encapsulate with Comm message response header
	    mHeaderBuilder.LoadFromMessage(mEvent.msg);
	    mHeaderBuilder.Encapsulate(cbuf);
	    
	    if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
		log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	    rwlock.readLock().unlock();
	    return;
	}
	catch (FileNotFoundException e) {
	    rwlock.readLock().unlock();
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_ATTRGET, Error.HYPERTABLEFS_FILE_NOT_FOUND,
						      e.getMessage(), mHeaderBuilder.HeaderLength());
	}
	catch (IOException e) {
	    rwlock.readLock().unlock();
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_ATTRGET, Error.HYPERTABLEFS_IO_ERROR,
						      e.getMessage(), mHeaderBuilder.HeaderLength());
	}

	// Encapsulate with Comm message response header
	mHeaderBuilder.LoadFromMessage(mEvent.msg);
	mHeaderBuilder.Encapsulate(cbuf);

	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
    }

    private String  mFilename;
    private String  mAttrName;
}
