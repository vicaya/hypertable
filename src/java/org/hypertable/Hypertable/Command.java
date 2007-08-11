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



package org.hypertable.Hypertable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.Vector;

import org.hypertable.AsyncComm.Comm;

abstract class Command implements Runnable {

    /**
     */
    public Command(String remainingLine) throws IOException {
	Reader r = new InputStreamReader(new ByteArrayInputStream(remainingLine.getBytes()));
	StreamTokenizer tokenizer = new StreamTokenizer(r);
	int ttype;
	CommandLineArg arg;

	mArgs.clear();

	while ((ttype = tokenizer.nextToken()) != StreamTokenizer.TT_EOF) {
	    if (ttype == StreamTokenizer.TT_WORD || ttype == '"') {
		arg = new CommandLineArg();
		arg.ttype = ttype;
		arg.sval = tokenizer.sval;
		arg.nval = 0.0;
		mArgs.add(arg);
	    }
	    else if (ttype == StreamTokenizer.TT_NUMBER) {
		arg = new CommandLineArg();
		arg.ttype = ttype;
		arg.sval = tokenizer.sval;
		arg.nval = tokenizer.nval;
		mArgs.add(arg);
	    }
	}

	return;
    }

    protected void ReportError(String message) {
	System.out.println(message);
	if (!Global.interactive)
	    System.exit(1);
    }

    protected static class CommandLineArg {
	public int ttype;
	public double nval;
	public String sval;
    }

    protected Vector<CommandLineArg> mArgs = new Vector<CommandLineArg>();


}
