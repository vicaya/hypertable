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

package org.hypertable.Hypertable.RangeServer;

import java.text.ParseException;

public class RangeIdentifier {
    public RangeIdentifier(String idStr) throws ParseException {
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
    }
    public String tableName = null;
    public String startRow = null;
    public String endRow = null;
}

