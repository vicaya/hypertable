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

package org.hypertable.HdfsBroker;

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

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.ConnectionHandlerFactory;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ReactorFactory;
import org.hypertable.AsyncComm.CallbackHandler;
import org.hypertable.AsyncComm.Message;
import org.hypertable.AsyncComm.HeaderBuilder;

public class Server {

    static final Logger log = Logger.getLogger("org.hypertable.HdfsBroker");

    /**
     */
    private static class ConnectionHandler implements CallbackHandler {

	public ConnectionHandler() {
	    mRequestFactory = new RequestFactory(mOpenFileMap);
	}

	public void handle(Event event) {
	    short command = -1;

	    if (event.type == Event.Type.MESSAGE) {
		//log.info(event.toString());
		try {
		    event.msg.buf.position(event.msg.headerLen);
		    command = event.msg.buf.getShort();
		    Global.workQueue.AddRequest( mRequestFactory.newInstance(event, command) );
		}
		catch (ProtocolException e) {
		    HeaderBuilder hbuilder = new HeaderBuilder();

		    // Build protocol message
		    CommBuf cbuf = Global.protocol.CreateErrorMessage(command, Error.PROTOCOL_ERROR, e.getMessage(), hbuilder.HeaderLength());

		    // Encapsulate with Comm message response header
		    hbuilder.LoadFromMessage(event.msg);
		    hbuilder.Encapsulate(cbuf);

		    Global.comm.SendResponse(event.addr, cbuf);
		}
	    }
	    else if (event.type == Event.Type.DISCONNECT) {
		if (Global.verbose)
		    log.info(event.toString() + " : Closing all open handles");		
		mOpenFileMap.RemoveAll();
	    }
	    else
		log.info(event.toString());
	}

	private RequestFactory mRequestFactory;
	private OpenFileMap mOpenFileMap = new OpenFileMap();
    }


    private static class HandlerFactory implements ConnectionHandlerFactory {
	public CallbackHandler newInstance() {
	    return new ConnectionHandler();
	}
    }

    static String usage[] = {
	"",
	"usage: HdfsBroker.Server [OPTIONS]",
	"",
	"OPTIONS:",
	"  --help           Display this help text and exit",
	"  --config=<file>  Load configuration properties from <file>",
	"  --verbose        Generate verbose logging output",
	"",
	"This program brokers Hadoop Distributed Filesystem (HDFS) requests",
	"on behalf of remote clients.  It is intended to provide an efficient",
	"way for C++ programs to use HDFS.",
	"",
	null
    };


    static final String DEFAULT_PORT = "38546";

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
	str  = props.getProperty("HdfsBroker.Server.port", DEFAULT_PORT);
	port = Integer.parseInt(str);

	// Determine reactor count
	str = props.getProperty("HdfsBroker.Server.reactors");
	reactorCount = (str == null) ? (short)System.processorCount : Short.parseShort(str);

	// Determine worker count
	str = props.getProperty("HdfsBroker.Server.workers");
	workerCount = (str == null) ? (short)System.processorCount : Integer.parseInt(str);

	ReactorFactory.Initialize(reactorCount);

	Global.comm = new Comm(0);
	Global.protocol = new Protocol();

	str = props.getProperty("HdfsBroker.Server.fs.default.name");
	if (str == null) {
	    java.lang.System.err.println("'HdfsBroker.Server.fs.default.name' property not specified");
	    java.lang.System.exit(1);
	}

	Global.conf = new Configuration();
	Global.conf.set("fs.default.name", str);
	Global.conf.set("dfs.client.buffer.dir", "/tmp");
	Global.conf.setInt("dfs.client.block.write.retries", 3);
	Global.fileSystem = FileSystem.get(Global.conf);
	Global.workQueue = new WorkQueue(workerCount);

	if (Global.verbose) {
	    java.lang.System.out.println("Total CPUs: " + System.processorCount);
	    java.lang.System.out.println("HdfsBroker.Server.port=" + port);
	    java.lang.System.out.println("HdfsBroker.Server.reactors=" + reactorCount);
	    java.lang.System.out.println("HdfsBroker.Server.workers=" + workerCount);
	    java.lang.System.out.println("HdfsBroker.Server.fs.default.name=" + str);
	}

	handlerFactory = new HandlerFactory();

	Global.comm.Listen(port, handlerFactory, null);

	Global.workQueue.Join();

	java.lang.System.exit(0);
    }

}
