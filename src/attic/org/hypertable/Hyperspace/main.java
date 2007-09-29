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

public class main {

    static final Logger log = Logger.getLogger("org.hypertable.Hyperspace");

    private static class HandlerFactory implements ConnectionHandlerFactory {
	public HandlerFactory(Comm comm, ApplicationQueue appQueue, Hyperspace hyperspace) {
	    mComm = comm;
	    mAppQueue = appQueue;
	    mHyperspace = hyperspace;
	}
	public DispatchHandler newInstance() {
	    return new ConnectionHandler(mComm, mAppQueue, mHyperspace);
	}

	private Comm  mComm;
	private ApplicationQueue mAppQueue;
	private Hyperspace mHyperspace;
    }
    
    static String usage[] = {
	"",
	"usage: java org.hypertable.Hyperspace.main [OPTIONS]",
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
	String str, configFile = null;
	Properties props = new Properties();
	HandlerFactory handlerFactory;
	boolean verbose = false;
	Comm comm;
	ApplicationQueue requestQueue;
	Hyperspace hyperspace;

	if (args.length == 1 && args[0].equals("--help"))
	    Usage.DumpAndExit(usage);

	for (int i=0; i<args.length; i++) {
	    if (args[i].startsWith("--config="))
		configFile = args[i].substring(9);
	    else if (args[i].equals("--verbose"))
		verbose = true;
	    else
		Usage.DumpAndExit(usage);
	}

	if (configFile == null)
	    configFile = System.installDir + "/conf/hypertable.cfg";

	FileInputStream fis = new FileInputStream(configFile);
	props.load(fis);

	if (verbose)
	    props.setProperty("verbose", "true");

	// Determine listen port
	str  = props.getProperty("Hyperspace.port", DEFAULT_PORT);
	port = Integer.parseInt(str);

	// Determine reactor count
	str = props.getProperty("Hyperspace.reactors");
	reactorCount = (str == null) ? (short)System.processorCount : Short.parseShort(str);

	// Determine worker count
	str = props.getProperty("Hyperspace.workers");
	workerCount = (str == null) ? (short)System.processorCount : Integer.parseInt(str);

	if (verbose) {
	    java.lang.System.out.println("Num CPUs=" + System.processorCount);
	    java.lang.System.out.println("Hyperspace.port=" + port);
	    java.lang.System.out.println("Hyperspace.reactors=" + reactorCount);
	    java.lang.System.out.println("Hyperspace.workers=" + workerCount);
	}

	ReactorFactory.Initialize(reactorCount);

	comm = new Comm(0);
	//Global.protocol = new Protocol();
	requestQueue = new ApplicationQueue(workerCount);

	hyperspace = new Hyperspace(comm, props);

	handlerFactory = new HandlerFactory(comm, requestQueue, hyperspace);

	comm.Listen(port, handlerFactory, null);

	requestQueue.Join();

    }

}
