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


package org.hypertable.AsyncComm;

import java.nio.ByteBuffer;

public class Message {

    public static final byte HEADER_LENGTH = 16;

    public static final byte VERSION = 1;

    public static final byte PROTOCOL_NONE                   = 0;
    public static final byte PROTOCOL_HDFS                   = 1;
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
