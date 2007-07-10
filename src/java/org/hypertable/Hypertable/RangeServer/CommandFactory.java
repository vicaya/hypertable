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

import java.io.IOException;

class CommandFactory {

    public static Command newInstance(String commandLine) throws IOException {
	String lowerLine = commandLine.toLowerCase();

	if (lowerLine.startsWith("load range"))
	    return new CommandLoadRange(commandLine.substring(11));
	else if (lowerLine.startsWith("update"))
	    return new CommandUpdate(commandLine.substring(6));
	else if (lowerLine.startsWith("scan"))
	    return new CommandScan(commandLine.substring(4));
	else if (lowerLine.startsWith("fetch scanblock"))
	    return new CommandFetchScanblock(commandLine.substring(15));
	else if (lowerLine.startsWith("compact"))
	    return new CommandCompact(commandLine.substring(7));
	else if (lowerLine.startsWith("bad command"))
	    return new CommandBad(commandLine.substring(12));
	else {
	    System.out.println("Unrecognized command '" + commandLine + "'");
	}
	return null;
    }

}
