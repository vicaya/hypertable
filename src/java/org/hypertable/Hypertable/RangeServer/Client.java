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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.util.logging.Logger;
import java.util.Properties;
import java.util.Vector;

import org.hypertable.Common.Error;
import org.hypertable.Common.System;
import org.hypertable.Common.Usage;

import org.hypertable.AsyncComm.CallbackHandler;
import org.hypertable.AsyncComm.CallbackHandlerSynchronizer;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.ConnectionManager;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.MessageBuilder;
import org.hypertable.AsyncComm.MessageBuilderSimple;
import org.hypertable.AsyncComm.ReactorFactory;


public class Client {

    static final Logger log = Logger.getLogger("org.hypertable.Hypertable.RangeServer");

    static final int DEFAULT_RANGESERVER_PORT = 38549;
    static final String DEFAULT_CLIENT_TIMEOUT = "120";

    public Client(Comm comm, InetSocketAddress addr, Properties props) {

	mComm = comm;
	mAddr = addr;

	// Get Client timeout
	String str = props.getProperty("Hypertable.Client.timeout", DEFAULT_CLIENT_TIMEOUT);
	mTimeout = Long.parseLong(str);

	mProtocol = new Protocol();

	mMasterManager = new ConnectionManager("Waiting for Hypertable.RangeServer...");
	mMasterManager.Initiate(mComm, addr, mTimeout);
    }

    public boolean WaitForReady() throws InterruptedException {
	if (!mMasterManager.WaitForConnection(mTimeout)) {
	    log.warning("Timed out waiting for connection to master at " + mAddr);
	    return false;
	}
	return true;
    }

 
    /**
     */
    public int LoadRange(int tableGeneration, RangeIdentifier range, CallbackHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestLoadRange(tableGeneration, range);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int Update(int tableGeneration, RangeIdentifier range, byte [] mods, CallbackHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestUpdate(tableGeneration, range, mods);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int CreateScanner(int tableGeneration, RangeIdentifier range, byte [] columns, String startRow,
			     String endRow, long startTime, long endTime, CallbackHandler handler) {
	
	CommBuf cbuf = mProtocol.BuildRequestCreateScanner(tableGeneration, range, columns, startRow, endRow, startTime, endTime);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int FetchScanblock(int scannerId, CallbackHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestFetchScanblock(scannerId);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int Compact(int tableGeneration, RangeIdentifier range, boolean major, String localityGroup, CallbackHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestCompact(tableGeneration, range, major, localityGroup);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int BadCommand(short command, CallbackHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestBadCommand(command);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     * 
     */
    private int SendRangeServerMessage(CommBuf cbuf, CallbackHandler handler) {
	int error;

	if ((error = mComm.SendRequest(mAddr, cbuf, handler)) != Error.OK) {
	    log.severe("Client.SendRangeServerMessage to " + mAddr + " failed - " + Error.GetText(error));
	    return 0;
	}

	return cbuf.id;
    }
 
    private Comm mComm;
    private long mTimeout;
    private Protocol    mProtocol;
    private ConnectionManager mMasterManager;
    private InetSocketAddress mAddr;
 
    static String usage[] = {
	"usage: Hypertable.RangeServer.Client [OPTIONS] [<host>:[<port>]]",
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
	String address = null;
	String host = null;
	int port = 0;
	InetSocketAddress addr = null;

	if (args.length == 1 && args[0].equals("--help"))
	    Usage.DumpAndExit(usage);

	for (int i=0; i<args.length; i++) {
	    if (args[i].startsWith("--config="))
		configFile = args[i].substring(9);
	    else if (args[i].equals("--batch"))
		Global.interactive = false;
	    else if (args[i].equals("--help"))
		Usage.DumpAndExit(usage);
	    else if (address == null)
		address = args[i];
	    else
		Usage.DumpAndExit(usage);
	}

	if (configFile == null)
	    configFile = System.installDir + "/conf/hypertable.cfg";

	try {

	    if (address != null) {
		int colon = address.indexOf(':');
		if (colon == -1) {
		    host = address;
		    port = DEFAULT_RANGESERVER_PORT;
		}
		else {
		    host = address.substring(0, colon);
		    port = Integer.parseInt(address.substring(colon+1));
		}
	    }
	    else {
		host = "localhost";
		port = DEFAULT_RANGESERVER_PORT;
	    }

	    java.lang.System.out.println("host=" + host);
	    java.lang.System.out.println("port=" + port);

	    addr = new InetSocketAddress(host, port);

	    ReactorFactory.Initialize((short)1);

	    FileInputStream fis = new FileInputStream(configFile);
	    props.load(fis);

	    Global.comm = new Comm(0);
	    Global.client = new Client(Global.comm, addr, props);

	    if (!Global.client.WaitForReady())
		java.lang.System.exit(1);

	    CommandExecutor exec = new CommandExecutor();
	    exec.run();
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
    }
}

  
  
  


