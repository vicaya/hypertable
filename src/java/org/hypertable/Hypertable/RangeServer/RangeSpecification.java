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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

import org.hypertable.Common.FileUtils;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.Hypertable.Schema;


public class RangeSpecification {
    public RangeSpecification(String idStr) throws ParseException, IOException, ParserConfigurationException, SAXException {
	int openBracket = idStr.indexOf('[');
	if (openBracket == -1)
	    tableName = idStr;
	else {
	    tableName = idStr.substring(0, openBracket);
	    int closeBracket = idStr.lastIndexOf(']');
	    if (closeBracket == -1)
		throw new ParseException("Bad range specifier (no closing brace)", 0);
	    if (closeBracket-openBracket > 1) {
		String rangeStr = idStr.substring(openBracket+1, closeBracket);
		int colon = rangeStr.indexOf(':');
		if (colon == -1)
		    endRow = rangeStr;
		else if (colon == 0) {
		    endRow = rangeStr.substring(1);
		}
		else {
		    startRow = rangeStr.substring(0, colon);
		    if (colon+1 < rangeStr.length())
			endRow = rangeStr.substring(colon+1);
		}
	    }
	}

	String schemaFile = tableName + ".xml";
	byte [] bytes = FileUtils.FileToBuffer(new File(schemaFile));
	String schemaStr = new String(bytes);
	schema = new Schema(schemaStr);
	generation = schema.GetGeneration();
    }

    public int SerializedLength() {
	return 4 + CommBuf.EncodedLength(tableName) + CommBuf.EncodedLength(startRow) + CommBuf.EncodedLength(endRow);
    }

    public void Prepend(CommBuf cbuf) {
	cbuf.PrependString(endRow);
	cbuf.PrependString(startRow);
	cbuf.PrependString(tableName);
	cbuf.PrependInt(generation);
    }

    public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append("TableGeneration = " + generation + "\n");
	sb.append("TableName       = " + tableName + "\n");
	sb.append("StartRow        = " + startRow + "\n");
	sb.append("EndRow          = " + endRow + "\n");
	return sb.toString();
    }

    public int generation = 0;
    public String tableName = null;
    public String startRow = null;
    public String endRow = null;
    public Schema schema = null;
}

