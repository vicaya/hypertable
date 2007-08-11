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

import java.io.IOException;
import java.io.StreamTokenizer;
import java.util.Vector;

import static java.lang.System.out;

import org.hypertable.Common.Error;

import org.hypertable.AsyncComm.Event;

import org.hypertable.Hypertable.ParseCommandLine;

class CommandLoadRange extends Command {

    private String mCommandLine;

    public CommandLoadRange(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	int msgId;
	ParseCommandLine.Argument arg;

	try {

	    if (mArgs.size() == 0) {
		DisplayUsage();
		return;
	    }

	    arg = mArgs.elementAt(0);

	    if (arg.value != null) {
		DisplayUsage();
		return;
	    }

	    RangeSpecification rangeSpec = new RangeSpecification(arg.name);

	    out.println(rangeSpec);

	    Global.client.LoadRange(rangeSpec, mSyncHandler);

	    Event event = mSyncHandler.WaitForEvent();

	    if (event == null)
		System.err.println("ERROR: Timed out waiting for response from Hypertable Master");
	    else if (Global.protocol.ResponseCode(event) != Error.OK) {
		System.err.println(Global.protocol.StringFormatMessage(event));
	    }
	    else
		System.out.println("success");
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
    }
    
    private void DisplayUsage() {
	ReportError("usage:  load range <range>");
    }
}
