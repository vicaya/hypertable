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

import java.io.File;
import java.io.IOException;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.CallbackHandlerSynchronizer;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.MsgId;

class CommandBad extends Command {

    //static final Logger log = Logger.getLogger("org.hypertable.Hypertable");

    public CommandBad(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	CallbackHandlerSynchronizer syncHandler = new CallbackHandlerSynchronizer(30);

	if (mArgs.size() != 1)
	    ReportError("error: wrong number of arguments.");
	else {
	    if (mArgs.elementAt(0).sval != null)
		ReportError("error: argument must be command number");

	    try {
		short command = (short)mArgs.elementAt(0).nval;
		MsgId msgId = new MsgId();

		int error = Global.master.BadCommand(command, syncHandler, msgId);
		if (error == Error.OK) {
		    Event event = syncHandler.WaitForEvent();
		    if (event == null)
			System.err.println("ERROR: Timed out waiting for response from Hypertable Master");		    
		    if (Global.masterProtocol.ResponseCode(event) != Error.OK)
			System.out.println(Global.masterProtocol.StringFormatMessage(event));
		}
	    }
	    catch (Exception e) {
		e.printStackTrace();
		System.exit(1);
	    }
	}
    }
}
