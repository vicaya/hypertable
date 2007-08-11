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

package org.hypertable.Hypertable.RangeServer;

import java.lang.StringBuilder;

import org.hypertable.AsyncComm.CommBuf;

public class ScanSpecification {

    public static final short ONLY_LATEST_CELLS = 0x01;

    public String toString() {
	StringBuilder sb = new StringBuilder();
	if (flags == ONLY_LATEST_CELLS)
	    sb.append("Flags     = ONLY_LATEST_CELLS\n");
	else
	    sb.append("Flags     = " + flags + "\n");
	sb.append("CellCount = " + cellCount + "\n");
	sb.append("StartRow  = " + startRow + "\n");
	sb.append("EndRow    = " + endRow + "\n");
	if (columns != null) {
	    sb.append("Columns   = ");
	    for (int i=0; i<columns.length; i++)
		sb.append(columns[i] +  " ");
	    sb.append("\n");
	}
	sb.append("MinTime   = " + minTime + "\n");
	sb.append("MaxTime   = " + maxTime + "\n");
	return sb.toString();
    }

    int SerializedLength() {
	byte [] startRowBytes = (startRow != null) ? startRow.getBytes() : null;
	byte [] endRowBytes = (endRow != null) ? endRow.getBytes() : null;
	return 6 +
	    4 + ((columns == null) ? 0 : columns.length) +
	    4 + ((startRowBytes == null) ? 0 : startRowBytes.length) + 
	    4 + ((endRowBytes == null) ? 0 : endRowBytes.length) +
	    16;
    }

    void Prepend(CommBuf cbuf) {
	byte [] startRowBytes = (startRow != null) ? startRow.getBytes() : null;
	byte [] endRowBytes = (endRow != null) ? endRow.getBytes() : null;
	cbuf.PrependLong(maxTime);
	cbuf.PrependLong(minTime);
	cbuf.PrependByteArray(endRowBytes);
	cbuf.PrependByteArray(startRowBytes);
	cbuf.PrependByteArray(columns);
	cbuf.PrependInt(cellCount);
	cbuf.PrependShort(flags);
    }

    public short flags = 0;
    public int cellCount = 0;
    public String startRow = null;
    public String endRow = null;
    public byte [] columns = null;
    public long minTime = 0;
    public long maxTime = 0;
}

