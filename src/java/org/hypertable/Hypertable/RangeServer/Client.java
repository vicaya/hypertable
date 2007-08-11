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

import org.hypertable.AsyncComm.DispatchHandler;
import org.hypertable.AsyncComm.DispatchHandlerSynchronizer;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.ConnectionManager;
import org.hypertable.AsyncComm.Event;
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
    public int LoadRange(RangeSpecification rangeSpec, DispatchHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestLoadRange(rangeSpec);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int Update(RangeSpecification rangeSpec, byte [] mods, DispatchHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestUpdate(rangeSpec, mods);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int CreateScanner(RangeSpecification rangeSpec, ScanSpecification spec, DispatchHandler handler) {
	
	CommBuf cbuf = mProtocol.BuildRequestCreateScanner(rangeSpec, spec);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int FetchScanblock(int scannerId, DispatchHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestFetchScanblock(scannerId);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int Compact(RangeSpecification rangeSpec, boolean major, DispatchHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestCompact(rangeSpec, major);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     */
    public int BadCommand(short command, DispatchHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestBadCommand(command);
	return SendRangeServerMessage(cbuf, handler);
    }

    /**
     * 
     */
    private int SendRangeServerMessage(CommBuf cbuf, DispatchHandler handler) {
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

  
  
  


