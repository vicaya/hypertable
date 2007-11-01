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

import java.nio.ByteBuffer;
import java.util.Vector;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.AsyncComm.Serialization;
import org.hypertable.Common.Error;

public class ResponseCallbackReaddir extends ResponseCallback {

    ResponseCallbackReaddir(Comm comm, Event event) {
	super(comm, event);
    }

    int response(String [] listing) {
	mHeaderBuilder.InitializeFromRequest(mEvent.msg);
	int len = 8;
	for (int i=0; i<listing.length; i++)
	    len += Serialization.EncodedLengthString(listing[i]);
	CommBuf cbuf = new CommBuf(mHeaderBuilder, len);
	cbuf.AppendInt(Error.OK);
	cbuf.AppendInt(listing.length);
	for (int i=0; i<listing.length; i++)
	    cbuf.AppendString(listing[i]);
	return mComm.SendResponse(mEvent.addr, cbuf);
    }
}

