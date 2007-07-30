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

import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;

public class HeaderBuilder {

    protected static AtomicInteger msNextId = new AtomicInteger();

    public void Reset(byte protocol) {
	mId = msNextId.incrementAndGet();
	mGroupId = 0;
	mProtocol = protocol;
    }

    public void Reset(byte protocol, byte flags) {
	mId = msNextId.incrementAndGet();
	mGroupId = 0;
	mProtocol = protocol;
	mFlags = flags;
    }

    public void LoadFromMessage(Message msg) {
	mId = msg.id;
	mGroupId = msg.gid;
	mProtocol = msg.protocol;
	mFlags = msg.flags;
    }

    public int HeaderLength() {
	return Message.HEADER_LENGTH;
    }

    public void Encapsulate(CommBuf cbuf) {
	cbuf.data.position(cbuf.data.position()-HeaderLength());
	int totalLen = cbuf.data.remaining();
	if (cbuf.ext != null)
	    totalLen += cbuf.ext.remaining();
	cbuf.data.mark();
	cbuf.data.put(Message.VERSION);
	cbuf.data.put(mProtocol);
	cbuf.data.put(mFlags);
	cbuf.data.put(Message.HEADER_LENGTH);
	cbuf.data.putInt(mId);
	cbuf.data.putInt(mGroupId);
	cbuf.data.putInt(totalLen);
	cbuf.data.reset();
	cbuf.id = mId;
    }

    public void SetFlags(byte flags) { mFlags = flags; }

    public void AddFlag(byte flag) { mFlags |= flag; }

    public void SetProtocol(byte protocol) { mProtocol = protocol; }

    public void SetGroupId(int gid) { mGroupId = gid; }

    protected int mId;
    protected int mGroupId;
    protected byte mProtocol;
    protected byte mFlags;
}

