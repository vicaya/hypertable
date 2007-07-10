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


public class RequestExists extends Request {

    public RequestExists(Event event) throws ProtocolException {
	super(event);
	if ((mFilename = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Filename not properly encoded in request packet");
    }

    public void run() {
	int error;
	CommBuf cbuf = null;
	ReentrantReadWriteLock rwlock = Global.lockMap.Get(mFilename);
	boolean exists = false;

	rwlock.readLock().lock();

	if (mFilename.lastIndexOf("/") == -1)
	    exists = (new File(Global.baseDir + "/" + mFilename)).exists();
	else
	    exists = (new File(Global.baseDir + mFilename)).exists();

	if (Global.verbose)
	    log.info("Checking for existance of file '" + mFilename + "' : " + exists);

	cbuf = new CommBuf(mMessageBuilder.HeaderLength() + 6);
	cbuf.PrependShort(Protocol.COMMAND_EXISTS);
	if (exists)
	    cbuf.PrependInt(Error.OK);
	else
	    cbuf.PrependInt(Error.HYPERTABLEFS_FILE_NOT_FOUND);

	// Encapsulate with Comm message response header
	mMessageBuilder.LoadFromMessage(mEvent.msg);
	mMessageBuilder.Encapsulate(cbuf);
	    
	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));

	rwlock.readLock().unlock();
    }

    private String mFilename;
}
