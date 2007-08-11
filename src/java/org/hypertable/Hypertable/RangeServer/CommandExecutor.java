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
	"scan <range> [OPTIONS]",
	"OPTIONS:",
	"  --latest-cells  Return only the most recent version of each cell",
	"  count=<n>       Return at most <n> cells",
	"  start=<row>     Start the scan at <row>",
	"  end=<row>       End the scan at <row> inclusive",
	"  columns=<column1>[,<column2>...]  Include only specified columns",
	"  timeInterval=<start>..<stop>  Include only cells within given time interval",
	"",
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
