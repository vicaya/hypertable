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

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.DispatchHandlerSynchronizer;
import org.hypertable.AsyncComm.Event;

class CommandCreateTable extends Command {

    static final Logger log = Logger.getLogger("org.hypertable.Hypertable.Master");

    public CommandCreateTable(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	int msgId;
	DispatchHandlerSynchronizer syncHandler = new DispatchHandlerSynchronizer(30);

	if (mArgs.size() != 2)
	    ReportError("error: wrong number of arguments (expected 2, got " + mArgs.size() + ")");
	else {
	    String tableName = mArgs.elementAt(0).sval;
	    String schemaFile = mArgs.elementAt(1).sval;
	    if (tableName == null) {
		ReportError("error: Bad table name");
	    }
	    else if (schemaFile == null) {
		ReportError("error: Bad schema file name");
	    }
	    else {
		try {

		    File sfile = new File(schemaFile);

		    if (!sfile.exists()) {
			ReportError("error: Schema file '" + schemaFile + "' does not exist");
			return;
		    }

		    byte [] bytes = FileUtils.FileToBuffer(sfile);
		    String schemaStr = new String(bytes);

		    int error = Global.master.CreateTable(tableName, schemaStr);
		    if (error != Error.OK) {
			log.severe("Error creating table - " + Error.GetText(error));
			return;
		    }

		    /**
		     *  For Testing
		     */
		    // ask local tablet server to load uber tablet
		    // insert location of uber tablet into losation cache
		    //Global.locationCache.Insert(String table, String startRow, String endRow, InetSocketAddress location);

		    System.out.println("success");
		}
		catch (Exception e) {
		    e.printStackTrace();
		    System.exit(1);
		}
	    }
	}
    }
}
