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
