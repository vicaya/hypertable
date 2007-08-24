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

import java.nio.ByteBuffer;

public class Message {

    public static final byte HEADER_LENGTH = 16;

    public static final byte VERSION = 1;

    public static final byte PROTOCOL_NONE                   = 0;
    public static final byte PROTOCOL_DFSBROKER              = 1;
    public static final byte PROTOCOL_HTFS                   = 2;
    public static final byte PROTOCOL_HYPERTABLE_MASTER      = 3;
    public static final byte PROTOCOL_HYPERTABLE_RANGESERVER = 4;
    public static final byte PROTOCOL_MAX                    = 5;

    public static final byte FLAGS_MASK_REQUEST  = (byte)0x01;
    public static final byte FLAGS_MASK_RESPONSE = (byte)0xFE;

    public void ReadHeader(ByteBuffer buf, int connectionId) {
	version     = buf.get();
	protocol    = buf.get();
	flags       = buf.get();
	headerLen   = buf.get();
	id          = buf.getInt();
	gid         = buf.getInt();
	totalLen    = buf.getInt();
	if (gid != 0)
	    threadGroup = ((long)connectionId << 32) | (long)gid;
	else
	    threadGroup = 0;
    }

    public void RewindToProtocolHeader() {
	buf.position(headerLen);
    }

    public byte   version;
    public byte   protocol;
    public byte   flags;
    public byte   headerLen;
    public int    id;
    public int    gid;
    public int    totalLen;
    public ByteBuffer buf;
    public long   threadGroup;
}
