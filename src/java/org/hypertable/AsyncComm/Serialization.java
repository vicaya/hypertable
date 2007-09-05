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

public class Serialization {

    /**
     * Computes the encoded length of a byte array
     *
     * @param len length of the byte array to be encoded
     * @return the encoded length of a byte array of length len
     */
    public static int EncodedLengthByteArray(int len) {
	return len + 4;
    }


    /**
     * Encodes a variable sized byte array into the given buffer.  Encoded as a
     * 4 byte length followed by the data.  Assumes there is enough space
     * available.
     *
     * @param buf byte buffer to write encoded array to
     * @param data byte array to encode
     * @param len number of bytes of data to encode
     */
    public static void EncodeByteArray(ByteBuffer buf, byte [] data, int len) {
	buf.putInt(len);
	buf.put(data, 0, len);
    }


    /**
     * Decodes a variable sized byte array from the given buffer.  Byte array is
     * encoded as a 4 byte length followed by the data.
     *
     * @param buf byte buffer holding encoded byte array
     * @return decoded byte array, or null on error
     */
    public static byte [] DecodeByteArray(ByteBuffer buf) {
	byte [] rbytes;
	int len;
	if (buf.remaining() < 4)
	    return null;
	len = buf.getInt();
	rbytes = new byte [ len ];
	if (len > 0) {
	    buf.get(rbytes, 0, len);
	}
	return rbytes;
    }

    /**
     * Computes the encoded length of a c-style null-terminated string
     *
     * @param str string to encode
     * @return the encoded length of str
     */
    public static int EncodedLengthString(String str) {
	if (str == null)
	    return 3;
	try {
	    return 3 + str.getBytes("UTF-8").length;
	}
	catch (UnsupportedEncodingException e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
	return 0;
    }


    /**
     * Encodes a string into the given buffer.  Encoded as a 2 byte length
     * followed by the UTF-8 string data, followed by a '\0' termination byte.
     * The length value does not include the '\0'.  Assumes there is enough
     * space available.
     *
     * @param buf destination byte buffer to hold encoded string
     * @param str string to encode
     */
    public static void EncodeString(ByteBuffer buf, String str) {
	try {
	    short len = (str == null) ? 0 : (short)str.getBytes("UTF-8").length;

	    // 2-byte length
	    buf.putShort(len);
      
	    // string characters
	    if (len > 0)
		buf.put(str.getBytes("UTF-8"));

	    // '\0' terminator
	    buf.put((byte)0);
	}
	catch (UnsupportedEncodingException e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
    }


    /**
     * Decodes a string from the given buffer.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\0'
     * termination byte.  The length does not include the '\0' terminator.
     *
     * @param buf the source byte buffer holding the encoded string
     * @return decoded string on success, null otherwise
     */
    public static String DecodeString(ByteBuffer buf) {
	if (buf.remaining() < 3)
	    return null;
	short len = buf.getShort();
	if (len == 0)
	    return new String("");
	byte [] sbytes = new byte [ len ];
	buf.get(sbytes);
	// skip '\0' terminator
	buf.get();
	try {
	    return new String(sbytes, "UTF-8");
	}
	catch (UnsupportedEncodingException e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
	return null;
    }

}
