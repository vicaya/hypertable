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

public class Result {

  public int encodedLength() {
    return 32;
  }
  public void encode(ByteBuffer buf) {
    buf.putLong(itemsSubmitted);
    buf.putLong(itemsReturned);
    buf.putLong(valueBytesReturned);
    buf.putLong(elapsedMillis);
  }
  public void decode(ByteBuffer buf) {
    itemsSubmitted = buf.getLong();
    itemsReturned = buf.getLong();
    valueBytesReturned = buf.getLong();
    elapsedMillis = buf.getLong();
  }

  public String toString() {
    return new String("(items-submitted=" + itemsSubmitted + ", items-returned=" + itemsReturned +
                      "value-bytes-returned=" + valueBytesReturned + ", elapsed-millis=" + elapsedMillis + ")");
  }

  public long itemsSubmitted;
  public long itemsReturned;
  public long valueBytesReturned;
  public long elapsedMillis;
}
