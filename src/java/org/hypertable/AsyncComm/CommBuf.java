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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CommBuf {

    public CommBuf(HeaderBuilder hbuilder, int len) {
	len += hbuilder.HeaderLength();
	data = ByteBuffer.allocate(len);
	data.order(ByteOrder.LITTLE_ENDIAN);
	hbuilder.SetTotalLen(len);
	hbuilder.Encode(data);
    }

    public CommBuf(HeaderBuilder hbuilder, int len, byte [] extBytes, int extBytesLen) {
	len += hbuilder.HeaderLength();
	data = ByteBuffer.allocate(len);
	data.order(ByteOrder.LITTLE_ENDIAN);
	if (extBytesLen > 0) {
	    ext = ByteBuffer.allocate(extBytesLen);
	    ext.order(ByteOrder.LITTLE_ENDIAN);
	    ext.put(extBytes, 0, extBytesLen);
	}
	hbuilder.SetTotalLen(len+extBytesLen);
	hbuilder.Encode(data);
    }

    /**
     * Resets the primary and extended data pointers to point to the
     * beginning of their respective buffers.  The AsyncComm layer
     * uses these pointers to track how much data has been sent and
     * what is remaining to be sent.
     */
    public void ResetDataPointers() {
	data.flip();
	if (ext != null)
	    ext.flip();
    }

    /**
     * Append a byte of data to the primary buffer
     */
    public void AppendByte(byte bval) { data.put(bval); }

    /**
     * Appends a byte buffer to the primary buffer.
     */
    public void AppendBytes(ByteBuffer buf) { data.put(buf); }

    /**
     * Appends a short integer (16 bit) to the the primary buffer,
     * advancing the primary buffer pointer
     */
    public void AppendShort(short sval) { data.putShort(sval); }
    
    /**
     * Appends an integer (32 bit) to the the primary buffer,
     * advancing the primary buffer pointer
     */
    public void AppendInt(int ival) { data.putInt(ival); }

    /**
     * Appends a long integer (64 bit) to the the primary buffer,
     * advancing the primary buffer pointer
     */
    public void AppendLong(long lval) { data.putLong(lval); }

    /**
     * Appends a byte array to the primary buffer.  A byte array
     * is encoded as a length followd by the data.
     *
     * @see Serialization::EncodeByteArray
     */
    public void AppendByteArray(byte [] bytes, int len) { Serialization.EncodeByteArray(data, bytes, len); }
    
    /**
     * Appends a string to the primary buffer.  A string is
     * encoded as a length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @see Serialization::EncodeString
     */
    public void AppendString(String str) { Serialization.EncodeString(data, str); }

    public ByteBuffer data = null;
    public ByteBuffer ext = null;
}

