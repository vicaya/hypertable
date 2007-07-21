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
