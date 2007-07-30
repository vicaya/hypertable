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

import java.io.FileInputStream;
import java.util.logging.Logger;
import java.util.Properties;

import org.hypertable.Common.Usage;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.ReactorFactory;


public class Client {

    static final Logger log = Logger.getLogger("org.hypertable.Hypertable");

    static String usage[] = {
	"usage: Hypertable.Client [OPTIONS]",
	"",
	"OPTIONS:",
	"  --batch          Disable interactive behavior",
	"  --config=<file>  Load configuration properties from <file>",
	"  --help           Display this help text and exit",
	"",
	"This program serves as the interactive client for Hypertable.",
	null
    };


    public static void main(String [] args) {
	String str;
	boolean continuation = false;
	String configFile = null;
	Properties props = new Properties();

	if (args.length == 1 && args[0].equals("--help"))
	    Usage.DumpAndExit(usage);

	for (int i=0; i<args.length; i++) {
	    if (args[i].startsWith("--config="))
		configFile = args[i].substring(9);
	    else if (args[i].equals("--batch"))
		Global.interactive = false;
	    else
		Usage.DumpAndExit(usage);
	}

	if (configFile == null)
	    configFile = org.hypertable.Common.System.installDir + "/conf/hypertable.cfg";

	try {

	    ReactorFactory.Initialize((short)org.hypertable.Common.System.processorCount);

	    FileInputStream fis = new FileInputStream(configFile);
	    props.load(fis);

	    Global.comm = new Comm(0);
	    Global.master = new org.hypertable.Hypertable.Master.Client(Global.comm, props);

	    if (!Global.master.WaitForReady())
		java.lang.System.exit(1);

	    CommandExecutor exec = new CommandExecutor();
	    exec.run();
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
    }
}

  
  
  


