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
import java.io.File;
import java.net.ProtocolException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;

import org.hypertable.Common.Error;


public class RequestMkdirs extends Request {

    public RequestMkdirs(Event event) throws ProtocolException {
	super(event);
	if ((mDirname = CommBuf.DecodeString(event.msg.buf)) == null)
	    throw new ProtocolException("Directory name not encoded properly in request packet");
    }

    public void run() {
	int error;
	CommBuf cbuf = null;
	ReentrantReadWriteLock rwlock = Global.lockMap.Get(mDirname);

	if (Global.verbose)
	    log.info("Creating directory '" + mDirname + "'");

	rwlock.writeLock().lock();

	if (mDirname.indexOf(".attr.") != -1) {
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_MKDIRS, Error.HYPERTABLEFS_IO_ERROR,
						      "Bad filename -> " + mDirname, mMessageBuilder.HeaderLength());
	}
	else if (!(new File(Global.baseDir + mDirname)).mkdirs()) {
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_MKDIRS, Error.HYPERTABLEFS_CREATE_FAILED,
						      mDirname, mMessageBuilder.HeaderLength());
	}
	else {
	    cbuf = new CommBuf(mMessageBuilder.HeaderLength() + 6);
	    cbuf.PrependShort(Protocol.COMMAND_MKDIRS);
	    cbuf.PrependInt(Error.OK);
	}

	// Encapsulate with Comm message response header
	mMessageBuilder.LoadFromMessage(mEvent.msg);
	mMessageBuilder.Encapsulate(cbuf);
	    
	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	rwlock.writeLock().unlock();
    }

    private String  mDirname;
}
