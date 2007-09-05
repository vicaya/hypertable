/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.hypertable.DfsBroker.hadoop;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.Common.Error;

public class ResponseCallbackWrite extends ResponseCallback {

    ResponseCallbackWrite(Comm comm, Event event) {
	super(comm, event);
    }

    int response(long offset, int amount) {
	mHeaderBuilder.InitializeFromRequest(mEvent.msg);
	CommBuf cbuf = new CommBuf(mHeaderBuilder, 16);
	cbuf.AppendInt(Error.OK);
	cbuf.AppendLong(offset);
	cbuf.AppendInt(amount);
	return mComm.SendResponse(mEvent.addr, cbuf);
    }
}

