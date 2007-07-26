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
