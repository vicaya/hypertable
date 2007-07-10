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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Properties;

import static java.lang.System.out;

import org.hypertable.AsyncComm.Comm;

class CommandExecutor implements Runnable {

    static String usage[] = {
	"",
	"load range <range>",
	"  This comand sends a LOAD RANGE request to the range server running",
	"  on localhost.  It assumes that there exists a schema file for <table>",
	"  in the current directory named '<table>.xml'",
	"",
	"update <range> <dataFile>",
	"  This comand sends a series of UPDATE requests with the data in <dataFie>",
	"  to the range server running on localhost.  It assumes that there exists a",
	"  schema file for <table> in the current directory named '<table>.xml'.",
	"",
	"scan <range> [start=<key>] [end=<key>] [columns=<column1>[,<column2>...]] [timeInterval=<startTime>-<stopTime>]",
	"  This comand sends a CREATE SCANNER and FETCH NEXT SCANBLOCK requests to the",
	"  range server running on localhost.  It assumes that there exists a schema",
	"  file for <table> in the current directory named '<table>.xml'",
	"",
	"compact <range> [type=major|minor] [lg=<localityGroup>]",
	"  This comand sends a COMPACT command to the range server running on localhost.",
	"  It will cause either a major or minor compaction (default=major) for the range",
	"  identified by <table> and <endRow>.  If a locality group is supplied, the only",
	"  the locality group named will get compacted.",
	"",
	"quit",
	"  This command terminates the program.",
	"",
	null
    };

    public void run() {
	Command command;
	String str;
	StringBuilder line = new StringBuilder();

	try {
	    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

	    if (Global.interactive) {
		out.println();
		out.println("RangeServer Test Driver (type 'help' for list of commands)");
		out.println();
		out.print("RangeServer> ");
		out.flush();
	    }
	    while ((str = in.readLine()) != null) {
		if (str.length() > 0 && str.charAt(str.length()-1) == '\\') {
		    line.append(str.substring(0, str.length()-1));
		    if (Global.interactive) {
			out.print("> ");
			out.flush();
		    }
		}
		else {
		    line.append(str);
		    String commandStr = line.toString().trim();

		    if (commandStr.equals("quit") || commandStr.equals("exit")) {
			in.close();
			System.exit(0);
		    }
		    else if (commandStr.equals("help")) {
			for (int i=0; usage[i] != null; i++)
			    out.println(usage[i]);
		    }
		    else if (commandStr.length() != 0) {
			command = CommandFactory.newInstance(commandStr);
			if (command != null)
			    command.run();
		    }
		    else if (Command.msOutstandingScanId != -1) {
			command = new CommandFetchScanblock("");
			command.run();
		    }

		    if (Global.interactive) {
			out.print("RangeServer> ");
			out.flush();
		    }
		    line.setLength(0);
		}
	    }
	    in.close();
	}
	catch (IOException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }

}
