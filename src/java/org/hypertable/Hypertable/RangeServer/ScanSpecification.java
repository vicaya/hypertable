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
import java.util.Vector;
import java.util.Enumeration;

import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.Serialization;

public class ScanSpecification {

    public static final short ONLY_LATEST_CELLS = 0x01;

    public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append("RowLimit  = " + rowLimit + "\n");
	sb.append("CellLimit = " + cellLimit + "\n");
	sb.append("StartRow  = " + startRow + "\n");
	sb.append("EndRow    = " + endRow + "\n");
	if (columns != null) {
	    sb.append("Columns   = ");
	    for (Enumeration<String> e = columns.elements() ; e.hasMoreElements() ;) {
		System.out.println(e.nextElement() + " ");
	    }	    
	    sb.append("\n");
	}
	sb.append("MinTime   = " + minTime + "\n");
	sb.append("MaxTime   = " + maxTime + "\n");
	return sb.toString();
    }

    int SerializedLength() {
	int length = 0;

	// rowLimit
	length += 4;  

	// cellLimit
	length += 4;  

	// startRow
	length += Serialization.EncodedLengthString(startRow);

	// endRow
	length += Serialization.EncodedLengthString(endRow);

	// columns
	if (columns != null) {
	    for (Enumeration<String> e = columns.elements() ; e.hasMoreElements() ;) {
		length += Serialization.EncodedLengthString(e.nextElement());
	    }
	}
	// column count
	length += 2;  

	// minTime
	length += 8;

	// maxTime
	length += 8;

	return length;
    }

    void Append(CommBuf cbuf) {
	cbuf.AppendInt(rowLimit);
	cbuf.AppendInt(cellLimit);
	cbuf.AppendString(startRow);
	cbuf.AppendString(endRow);
	if (columns != null) {
	    cbuf.AppendShort((short)columns.size());
	    for (Enumeration<String> e = columns.elements() ; e.hasMoreElements() ;) {
		cbuf.AppendString(e.nextElement());
	    }
	}
	else
	    cbuf.AppendShort((short)0);
	cbuf.AppendLong(minTime);
	cbuf.AppendLong(maxTime);
    }

    public int rowLimit = 0;
    public int cellLimit = 0;
    public String startRow = null;
    public String endRow = null;
    Vector<String> columns;
    public long minTime = 0;
    public long maxTime = 0;
}

