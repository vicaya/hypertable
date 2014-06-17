/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

package org.hypertable.examples.PerformanceTest;

import java.nio.ByteBuffer;

import org.hypertable.AsyncComm.Serialization;

public class MessageRegister extends Message {
  public MessageRegister() {
    super(Message.Type.REGISTER);
  }
  public int encodedLength() {
    return Serialization.EncodedLengthString(mHostName) +
      Serialization.EncodedLengthString(mHostAddress) +
      Serialization.EncodedLengthString(mClientName);
  }
  public void encode(ByteBuffer buf) {
    Serialization.EncodeString(buf, mHostName);
    Serialization.EncodeString(buf, mHostAddress);
    Serialization.EncodeString(buf, mClientName);
  }
  public void decode(ByteBuffer buf) {
    mHostName = Serialization.DecodeString(buf);
    mHostAddress = Serialization.DecodeString(buf);
    mClientName = Serialization.DecodeString(buf);
  }

  public String toString() {
    return new String("MESSAGE:INIT { host=" + mHostName + ", addr=" + mHostAddress + ", name=" + mClientName + "}");
  }

  public void setHostName(String name) { mHostName = name; }
  public String getHostName() { return mHostName; }

  public void setHostAddress(String address) { mHostAddress = address; }
  public String getHostAddress() { return mHostAddress; }

  public void setClientName(String name) { mClientName = name; }
  public String getClientName() { return mClientName; }

  private String mHostName;
  private String mHostAddress;
  private String mClientName;
}
