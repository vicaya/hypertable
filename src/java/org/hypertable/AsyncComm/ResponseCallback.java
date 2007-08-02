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

package org.hypertable.AsyncComm;

import java.net.InetSocketAddress;
import org.hypertable.Common.Error;

/**
 * 
 */
public class ResponseCallback {

    public ResponseCallback(Comm comm, Event event) {
	mComm = comm;
	mEvent = event;
    }

    public InetSocketAddress GetAddress() {
	return mEvent.addr;
    }

    public int error(int error, String msg) {
	// Build protocol message
	CommBuf cbuf = Protocol.CreateErrorMessage((short)0, error, msg, mHeaderBuilder.HeaderLength());
	// Encapsulate with Comm message response header
	mHeaderBuilder.LoadFromMessage(mEvent.msg);
	mHeaderBuilder.Encapsulate(cbuf);
	return mComm.SendResponse(mEvent.addr, cbuf);
    }

    public int response_ok() {
	CommBuf cbuf = new CommBuf( mHeaderBuilder.HeaderLength() + 4 );
	cbuf.PrependInt(Error.OK);
	mHeaderBuilder.LoadFromMessage(mEvent.msg);
	mHeaderBuilder.Encapsulate(cbuf);
	return mComm.SendResponse(mEvent.addr, cbuf);
    }

    protected Comm mComm;
    protected Event mEvent;
    protected HeaderBuilder mHeaderBuilder = new HeaderBuilder();
}
