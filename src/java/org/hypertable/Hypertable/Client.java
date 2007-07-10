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

  
  
  


