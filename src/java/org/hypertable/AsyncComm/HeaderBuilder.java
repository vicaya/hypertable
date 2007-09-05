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

package org.hypertable.AsyncComm;

import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;

public class HeaderBuilder {

    protected static AtomicInteger msNextId = new AtomicInteger();

    public HeaderBuilder() {
	return;
    }

    public HeaderBuilder(byte protocol, int gid) {
	mGroupId = gid;
	mProtocol = protocol;
	return;
    }

    public void InitializeFromRequest(Message msg) {
	mId = msg.id;
	mGroupId = msg.gid;
	mProtocol = msg.protocol;
	mFlags = msg.flags;
	mTotalLen = 0;
    }

    public int HeaderLength() {
	return Message.HEADER_LENGTH;
    }

    public void Encode(ByteBuffer buf) {
	buf.put(Message.VERSION);
	buf.put(mProtocol);
	buf.put(mFlags);
	buf.put(Message.HEADER_LENGTH);
	buf.putInt(mId);
	buf.putInt(mGroupId);
	buf.putInt(mTotalLen);
    }


    public int AssignUniqueId() { mId = msNextId.incrementAndGet(); return mId; }

    public void SetFlags(byte flags) { mFlags = flags; }

    public void AddFlag(byte flag) { mFlags |= flag; }

    public void SetProtocol(byte protocol) { mProtocol = protocol; }

    public void SetGroupId(int gid) { mGroupId = gid; }

    public void SetTotalLen(int totalLen) { mTotalLen = totalLen; }

    protected int mId = 0;
    protected int mGroupId = 0;
    protected int mTotalLen = 0;
    protected byte mProtocol = 0;
    protected byte mFlags = 0;
}

