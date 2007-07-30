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

import java.io.File;
import java.io.IOException;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.Event;

class CommandBad extends Command {

    public CommandBad(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	/**
	int msgId;
	CallbackHandlerSynchronizer syncHandler = new CallbackHandlerSynchronizer(30);

	if (mArgs.size() != 1)
	    ReportError("error: wrong number of arguments.");
	else {
	    if (mArgs.elementAt(0).sval != null)
		ReportError("error: argument must be command number");

	    try {
		short command = (short)mArgs.elementAt(0).nval;

		msgId = Global.client.BadCommand(command, syncHandler);
		Event event = syncHandler.WaitForEvent();
		if (event == null)
		    System.err.println("ERROR: Timed out waiting for response from Hypertable Master");		    
		if (Global.protocol.ResponseCode(event) != Error.OK)
		    System.out.println(Global.protocol.StringFormatMessage(event));
	    }
	    catch (Exception e) {
		e.printStackTrace();
		System.exit(1);
	    }
	}
	*/
    }
}
