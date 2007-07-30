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
						      "Bad filename -> " + mDirname, mHeaderBuilder.HeaderLength());
	}
	else if (!(new File(Global.baseDir + mDirname)).mkdirs()) {
	    cbuf = Global.protocol.CreateErrorMessage(Protocol.COMMAND_MKDIRS, Error.HYPERTABLEFS_CREATE_FAILED,
						      mDirname, mHeaderBuilder.HeaderLength());
	}
	else {
	    cbuf = new CommBuf(mHeaderBuilder.HeaderLength() + 6);
	    cbuf.PrependShort(Protocol.COMMAND_MKDIRS);
	    cbuf.PrependInt(Error.OK);
	}

	// Encapsulate with Comm message response header
	mHeaderBuilder.LoadFromMessage(mEvent.msg);
	mHeaderBuilder.Encapsulate(cbuf);
	    
	if ((error = Global.comm.SendResponse(mEvent.addr, cbuf)) != Error.OK)
	    log.log(Level.SEVERE, "Comm.SendResponse returned " + Error.GetText(error));
	rwlock.writeLock().unlock();
    }

    private String  mDirname;
}
