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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;

import org.hypertable.Common.Error;


public class RequestAttrDel extends Request {

    public RequestAttrDel(Event event) throws ProtocolException {
	super(event);

	if ((mFilename = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Filename not found in request packet");

	if ((mAttrName = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Attribute name not found in request packet");
    }

    public void run() {
	int error;
	CommBuf cbuf = null;
	ReentrantReadWriteLock rwlock = Global.lockMap.Get(mFilename);
	File attrFile;

	try {

	    if (Global.verbose)
		log.info("AttrDel file=" + mFilename + " attr=" + mAttrName);

	    rwlock.writeLock().lock();

	    String baseDir = mFilename.startsWith("/") ? Global.baseDir : Global.baseDir + "/";

	    if (!(new File(baseDir + mFilename)).exists())
		throw new FileNotFoundException(mFilename);

	    attrFile = new File(baseDir + mFilename + ".attr." + mAttrName);

	    attrFile.delete();

	    cbuf = new CommBuf(mHeaderBuilder.HeaderLength() + 6);
	    cbuf.PrependShort(Protocol.COMMAND_ATTRDEL);
	    cbuf.PrependInt(Error.OK);

	    // Encapsulate with Comm message response header
	    mHeaderBuilder.LoadFromMessage(mEvent.msg);
	    mHeaderBuilder.Encapsulate(cbuf);
	    
	    if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
		log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	    rwlock.writeLock().unlock();
	    return;
	}
	catch (FileNotFoundException e) {
	    rwlock.writeLock().unlock();
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_ATTRDEL, Error.HYPERTABLEFS_FILE_NOT_FOUND,
						      e.getMessage(), mHeaderBuilder.HeaderLength());
	}
	catch (IOException e) {
	    rwlock.writeLock().unlock();
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_ATTRDEL, Error.HYPERTABLEFS_IO_ERROR,
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
