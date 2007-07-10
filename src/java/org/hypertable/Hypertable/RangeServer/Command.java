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




package org.hypertable.Hypertable.RangeServer;

import java.util.Vector;

import org.hypertable.AsyncComm.CallbackHandlerSynchronizer;

import org.hypertable.Hypertable.ParseCommandLine;
import org.hypertable.Hypertable.Schema;

abstract class Command implements Runnable {

    public Command(String commandLine) {
	ParseCommandLine.parse(commandLine, mArgs);
    }

    protected void ReportError(String message) {
	System.out.println(message);
	if (!Global.interactive)
	    System.exit(1);
    }

    protected CallbackHandlerSynchronizer mSyncHandler = new CallbackHandlerSynchronizer(60);
    protected Vector<ParseCommandLine.Argument> mArgs = new Vector<ParseCommandLine.Argument>();

    static public int msOutstandingScanId = -1;
    static public Schema msOutstandingSchema = null;

}
