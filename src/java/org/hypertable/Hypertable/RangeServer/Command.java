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

import java.util.Vector;

import org.hypertable.AsyncComm.DispatchHandlerSynchronizer;

import org.hypertable.Hypertable.ParseCommandLine;
import org.hypertable.Hypertable.Schema;

abstract class Command implements Runnable {

    public Command(String commandLine) {
	ParseCommandLine.parse(commandLine, mArgs);
    }

    protected void ReportError(String message) {
	System.out.println(message);
	if (!Global.interactive)
	    System.exit(1);
    }

    protected DispatchHandlerSynchronizer mSyncHandler = new DispatchHandlerSynchronizer(60);
    protected Vector<ParseCommandLine.Argument> mArgs = new Vector<ParseCommandLine.Argument>();

    static public int msOutstandingScanId = -1;
    static public Schema msOutstandingSchema = null;

}
