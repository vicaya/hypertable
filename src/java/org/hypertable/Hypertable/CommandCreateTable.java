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
import java.util.logging.Logger;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.CallbackHandlerSynchronizer;
import org.hypertable.AsyncComm.Event;

class CommandCreateTable extends Command {

    static final Logger log = Logger.getLogger("org.hypertable.Hypertable.Master");

    public CommandCreateTable(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	int msgId;
	CallbackHandlerSynchronizer syncHandler = new CallbackHandlerSynchronizer(30);

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
