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



package org.hypertable.Hypertable;

import java.util.Vector;

public class ParseCommandLine {

    public static class Argument {
	public String name = null;
	public String value = null;
    }

    static public boolean parse( String line, Vector<Argument> args ) {
	char [] lineChars = new char [ line.length() ];
	line.getChars(0, line.length(), lineChars, 0);
	int idx=0;
	int nameStart;
	int nameLen;
	int valueStart;
	int valueLen;
	Argument arg = new Argument();

	while (idx<line.length()) {

	    // skip whitespace
	    while (idx<line.length() && Character.isWhitespace(lineChars[idx]))
		idx++;

	    nameStart = idx;

	    while (idx<line.length() && !Character.isWhitespace(lineChars[idx]) && lineChars[idx] != '=')
		idx++;

	    nameLen = idx-nameStart;

	    // skip whitespace
	    while (idx<line.length() && Character.isWhitespace(lineChars[idx]))
		idx++;

	    if (idx==line.length() || lineChars[idx] != '=') {
		if (nameLen > 0) {
		    arg.name = new String(lineChars, nameStart, nameLen);
		    args.add(arg);
		    arg = new Argument();
		}
		continue;
	    }

	    idx++;

	    // skip whitespace
	    while (idx<line.length() && Character.isWhitespace(lineChars[idx]))
		idx++;

	    if (idx==line.length()) {
		arg.name = new String(lineChars, nameStart, nameLen);
		args.add(arg);
		break;
	    }

	    if (lineChars[idx] == '"') {
		idx++;
		valueStart = idx;
		while (idx<line.length() && lineChars[idx] != '"')
		    idx++;
		valueLen = idx-valueStart;
		arg.name = new String(lineChars, nameStart, nameLen);
		if (valueLen > 0)
		    arg.value = new String(lineChars, valueStart, valueLen);
		args.add(arg);
		arg = new Argument();
		if (idx<line.length())
		    idx++;
	    }
	    else {
		valueStart = idx;
		while (idx<line.length() && !Character.isWhitespace(lineChars[idx]))
		    idx++;
		valueLen = idx-valueStart;
		arg.name = new String(lineChars, nameStart, nameLen);
		arg.value = new String(lineChars, valueStart, valueLen);
		args.add(arg);
		arg = new Argument();
	    }
	}

	return true;
    }
}
