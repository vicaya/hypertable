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
package org.hypertable.Hyperspace;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.hypertable.Common.Error;
import org.hypertable.Common.System;
import org.hypertable.Common.Usage;

import org.hypertable.AsyncComm.ApplicationQueue;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.ConnectionHandlerFactory;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ReactorFactory;
import org.hypertable.AsyncComm.DispatchHandler;
import org.hypertable.AsyncComm.Message;

public class NameServer {

    static final Logger log = Logger.getLogger("org.hypertable.Hyperspace");

    private static class HandlerFactory implements ConnectionHandlerFactory {
	public DispatchHandler newInstance() {
	    return new ConnectionHandler();
	}
    }

    static String usage[] = {
	"",
	"usage: Hyperspace.NameServer [OPTIONS]",
	"",
	"OPTIONS:",
	"  --config=<file>  Read configuration from <file>",
	"  --help           Display this help text and exit",
	"  --verbose        Produce verbose logging output",
	"",
	"This program serves as the name server for Hyperspace.",
	"",
	null
    };


    static final String DEFAULT_PORT = "38547";

    public static void main(String [] args) throws IOException, InterruptedException {
	int port;
	short reactorCount;
	int   workerCount;
	String configFile = null;
	Properties props = new Properties();
	HandlerFactory handlerFactory;

	if (args.length == 1 && args[0].equals("--help"))
	    Usage.DumpAndExit(usage);

	for (int i=0; i<args.length; i++) {
	    if (args[i].startsWith("--config="))
		configFile = args[i].substring(9);
	    else if (args[i].equals("--verbose"))
		Global.verbose = true;
	    else
		Usage.DumpAndExit(usage);
	}

	if (configFile == null)
	    configFile = System.installDir + "/conf/hypertable.cfg";

	FileInputStream fis = new FileInputStream(configFile);
	props.load(fis);

	String str;

	// Determine listen port
	str  = props.getProperty("Hyperspace.Server.port", DEFAULT_PORT);
	port = Integer.parseInt(str);

	// Determine reactor count
	str = props.getProperty("Hyperspace.Server.reactors");
	reactorCount = (str == null) ? (short)System.processorCount : Short.parseShort(str);

	// Determine worker count
	str = props.getProperty("Hyperspace.Server.workers");
	workerCount = (str == null) ? (short)System.processorCount : Integer.parseInt(str);

	// Determine working directory
	if ((Global.baseDir = props.getProperty("Hyperspace.Server.dir")) == null) {
	    java.lang.System.err.println("Required config property 'Hyperspace.dir' not found.");
	    java.lang.System.exit(1);
	}
	if (Global.baseDir.endsWith("/"))
	    Global.baseDir = Global.baseDir.substring(0, Global.baseDir.length()-1);

	ReactorFactory.Initialize(reactorCount);

	Global.comm = new Comm(0);
	Global.requestQueue = new ApplicationQueue(workerCount);
	Global.protocol = new Protocol();

	if (Global.verbose) {
	    java.lang.System.out.println("Total CPUs: " + System.processorCount);
	    java.lang.System.out.println("Hyperspace.Server.port=" + port);
	    java.lang.System.out.println("Hyperspace.Server.reactors=" + reactorCount);
	    java.lang.System.out.println("Hyperspace.Server.workers=" + workerCount);
	    java.lang.System.out.println("Hyperspace.Server.dir=" + Global.baseDir);
	}

	handlerFactory = new HandlerFactory();

	Global.comm.Listen(port, handlerFactory, null);
    }

}
