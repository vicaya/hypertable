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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CommBuf {
    public CommBuf(int dataLen) {
	data = ByteBuffer.allocate(dataLen);
	data.order(ByteOrder.LITTLE_ENDIAN);
	data.position(data.limit());
    }
    public void PrependData(byte [] dataBytes) {
	data.position(data.position()-dataBytes.length);
	data.mark();
	data.put(dataBytes);
	data.reset();
    }
    public void PrependData(ByteBuffer buf) {
	data.position(data.position()-buf.remaining());
	data.mark();
	data.put(buf);
	data.reset();
    }
    public void PrependByte(byte b) {
	data.position(data.position()-1);
	data.put(b);
	data.position(data.position()-1);
    }
    public void PrependShort(short s) {
	data.position(data.position()-2);
	data.putShort(s);
	data.position(data.position()-2);
    }
    public void PrependInt(int i) {
	data.position(data.position()-4);
	data.putInt(i);
	data.position(data.position()-4);
    }
    public void PrependLong(long l) {
	data.position(data.position()-8);
	data.putLong(l);
	data.position(data.position()-8);
    }

    public void PrependByteArray(byte [] dataBytes) {
	if (dataBytes != null) {
	    data.position(data.position()-(dataBytes.length+4));
	    data.mark();
	    data.putInt(dataBytes.length);
	    data.put(dataBytes);
	    data.reset();
	}
	else
	    PrependInt(0);
    }

    public void PrependString(String str) {
	try {
	    if (str == null) {
		data.position(data.position()-3);
		data.put((byte)0);
		data.put((byte)0);
		data.put((byte)0);
		data.position(data.position()-3);
	    }
	    else {
		byte [] strBytes = str.getBytes("UTF-8");
		data.position(data.position()-(strBytes.length+3));
		data.putShort((short)strBytes.length);
		data.put(strBytes);
		data.put((byte)0);
		data.position(data.position()-(strBytes.length+3));
	    }
	}
	catch (UnsupportedEncodingException e) {
	}
    }

    public static int EncodedLength(String str) {
	try {
	    return (str == null) ? 3 : str.getBytes("UTF-8").length + 3;
	}
	catch (UnsupportedEncodingException e) {
	}
	return 0;
    }

    public static String DecodeString(ByteBuffer buf) {
	try {
	    if (buf.remaining() < 2)
		return null;
	    short len = buf.getShort();
	    if (buf.remaining() < len+1)
		return null;
	    byte [] strBytes = new byte [ len ];
	    buf.get(strBytes);
	    buf.get(); // skip trailing '\0'
	    return new String(strBytes, "UTF-8");
	}
	catch (UnsupportedEncodingException e) {
	}
	return null;
    }

    public ByteBuffer data;
    public ByteBuffer ext;
    public int id;
}

