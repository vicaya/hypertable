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

import java.io.IOException;

class CommandFactory {

    public static Command newInstance(String commandLine) throws IOException {
	String lowerLine = commandLine.toLowerCase();

	if (lowerLine.startsWith("create table"))
	    return new CommandCreateTable(commandLine.substring(13));
	else if (lowerLine.startsWith("get schema"))
	    return new CommandGetSchema(commandLine.substring(10));
	else if (lowerLine.startsWith("open table"))
	    return new CommandOpenTable(commandLine.substring(10));
	else if (lowerLine.startsWith("bad command"))
	    return new CommandBad(commandLine.substring(12));
	else {
	    System.out.println("Unrecognized command '" + commandLine + "'");
	    System.exit(1);
	}
	return null;
    }

}
