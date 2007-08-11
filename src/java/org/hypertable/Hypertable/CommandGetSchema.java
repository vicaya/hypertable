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

import java.io.File;
import java.io.IOException;
import java.lang.StringBuilder;
import java.util.logging.Logger;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.DispatchHandlerSynchronizer;
import org.hypertable.AsyncComm.Event;

class CommandGetSchema extends Command {

    static final Logger log = Logger.getLogger("org.hypertable.Hypertable.Master");

    public CommandGetSchema(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	int msgId;
	DispatchHandlerSynchronizer syncHandler = new DispatchHandlerSynchronizer(30);

	if (mArgs.size() != 1)
	    ReportError("error: wrong number of arguments.");
	else {
	    String tableName = mArgs.elementAt(0).sval;
	    if (tableName == null) {
		ReportError("error: Bad table name");
	    }
	    else {
		try {
		    StringBuilder schema = new StringBuilder();

		    int error = Global.master.GetSchema(tableName, schema);
		    if (error != Error.OK)
			log.severe("Error getting schema for table '" + tableName + "' - " + Error.GetText(error));
		    else {
			System.out.println();
			System.out.println(schema.toString());
		    }
		}
		catch (Exception e) {
		    e.printStackTrace();
		    System.exit(1);
		}
	    }
	}
    }
}
