/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
