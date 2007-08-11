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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Properties;

import static java.lang.System.out;

import org.hypertable.AsyncComm.Comm;

class CommandExecutor implements Runnable {

    static String usage[] = {
	"",
	"create table <tableName> <schemaFile>",
	"  This comand sends a CREATE TABLE command to the Master with the table",
	"  name <tableName> and the schema from <schemaFile>",
	"",
	"get schema <tableName>",
	"  This command sends a GET SCHEMA request to the Master and displays the",
	"  returned schema",
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
		out.print("hypertable> ");
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
		    else {
			command = CommandFactory.newInstance(commandStr);
			command.run();
		    }

		    if (Global.interactive) {
			out.print("hypertable> ");
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
