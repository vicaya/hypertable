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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.net.ProtocolException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;

import org.hypertable.Common.Error;


public class RequestCreate extends Request {

    public RequestCreate(Event event) throws ProtocolException {
	super(event);
	if ((mFilename = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Filename not properly encoded in request packet");
    }

    public void run() {
	int error;
	CommBuf cbuf = null;
	ReentrantReadWriteLock rwlock = Global.lockMap.Get(mFilename);

	try {

	    if (Global.verbose)
		log.info("Creating file '" + mFilename + "'");

	    rwlock.writeLock().lock();

	    if (mFilename.indexOf(".attr.") != -1)
		throw new IOException("Bad filename (" + mFilename + ")");

	    String baseDir = mFilename.startsWith("/") ? Global.baseDir : Global.baseDir + "/";

	    int lastSlash = mFilename.lastIndexOf("/");

	    // make sure parent exists, throw an informative message if not
	    if (lastSlash > 0) {
		String relParent = mFilename.substring(0, lastSlash);
		File absParent = new File(baseDir, relParent);
		if (!absParent.exists()) 
		    throw new FileNotFoundException(relParent);
	    }

	    (new File(baseDir + mFilename)).createNewFile();

	    cbuf = new CommBuf(mHeaderBuilder.HeaderLength() + 6);
	    cbuf.PrependShort(Protocol.COMMAND_CREATE);
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
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_CREATE, Error.HYPERTABLEFS_FILE_NOT_FOUND,
						      e.getMessage(), mHeaderBuilder.HeaderLength());
	}
	catch (IOException e) {
	    rwlock.writeLock().unlock();
	    e.printStackTrace();
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_CREATE, Error.HYPERTABLEFS_IO_ERROR,
						      e.getMessage(), mHeaderBuilder.HeaderLength());
	}

	// Encapsulate with Comm message response header
	mHeaderBuilder.LoadFromMessage(mEvent.msg);
	mHeaderBuilder.Encapsulate(cbuf);

	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
    }

    private String mFilename;
}
