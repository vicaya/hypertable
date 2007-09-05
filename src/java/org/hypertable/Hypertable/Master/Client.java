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

package org.hypertable.Hypertable.Master;

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
import org.hypertable.AsyncComm.Serialization;

public class Client {

    static final Logger log = Logger.getLogger("org.hypertable.Bigtable.Master");

    static final String DEFAULT_MASTER_PORT = "38548";
    static final String DEFAULT_CLIENT_TIMEOUT = "30";

    public Client(Comm comm, Properties props) {
	String str, mhost;
	int mport;

	mComm = comm;

	// Get Master host
	mhost = props.getProperty("Bigtable.Master.host", "localhost");

	// Get Master port
	str  = props.getProperty("Bigtable.Master.port", DEFAULT_MASTER_PORT);
	mport = Integer.parseInt(str);

	// Get Client timeout
	str  = props.getProperty("Bigtable.Client.timeout", DEFAULT_CLIENT_TIMEOUT);
	mTimeout = Long.parseLong(str);

	try {
	    mAddr = new InetSocketAddress(mhost, mport);
	}
	catch (Throwable e) {
	    log.severe("Problem creating socket address for " + mhost + ":" + mport);
	    e.printStackTrace();
	    java.lang.System.exit(1);
	}

	mProtocol = new Protocol();

	mConnectionManager = new ConnectionManager(mComm);
	mConnectionManager.Add(mAddr, 10, "Hypertable.Master");

    }

    public boolean WaitForReady() throws InterruptedException {
	if (!mConnectionManager.WaitForConnection(mAddr, mTimeout)) {
	    log.warning("Timed out waiting for connection to Hypertable.Master at " + mAddr);
	    return false;
	}
	return true;
    }

 
   /**
     */
    public int CreateTable(String tableName, String schema, DispatchHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestCreateTable(tableName, schema);
	return SendMasterMessage(cbuf, handler);
    }

    /**
     */
    public int CreateTable(String tableName, String schema) throws InterruptedException {
	DispatchHandlerSynchronizer handler = new DispatchHandlerSynchronizer(mTimeout);
	CommBuf cbuf = mProtocol.BuildRequestCreateTable(tableName, schema);
	int error = SendMasterMessage(cbuf, handler);
	if (error == Error.OK) {
	    Event event = handler.WaitForEvent();
	    if (event == null)
		return Error.REQUEST_TIMEOUT;
	    error = mProtocol.ResponseCode(event);
	    if (error != Error.OK)
		log.severe(mProtocol.StringFormatMessage(event));
	}
	return error;
    }


   /**
    * Get schema, non-blocking
    */
    public int GetSchema(String tableName, StringBuilder schema, DispatchHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestGetSchema(tableName);
	return SendMasterMessage(cbuf, handler);
    }

    /**
     * Get schema, blocking
     */
    public int GetSchema(String tableName, StringBuilder schema) throws InterruptedException {
	DispatchHandlerSynchronizer handler = new DispatchHandlerSynchronizer(mTimeout);
	CommBuf cbuf = mProtocol.BuildRequestGetSchema(tableName);
	int error = SendMasterMessage(cbuf, handler);
	if (error == Error.OK) {
	    Event event = handler.WaitForEvent();
	    if (event == null)
		return Error.REQUEST_TIMEOUT;
	    error = mProtocol.ResponseCode(event);
	    if (error != Error.OK)
		log.severe(mProtocol.StringFormatMessage(event));
	    else {
		event.msg.RewindToProtocolHeader();
		event.msg.buf.getInt();   // skip error
		String schemaString = Serialization.DecodeString(event.msg.buf);
		if (schemaString == null) {
		    log.severe("GetSchema failure - problem decoding response packet");
		    return Error.PROTOCOL_ERROR;
		}
		schema.append(schemaString);
	    }
	}
	return error;
    }


   /**
    */
    public int BadCommand(short command, DispatchHandler handler) {
	CommBuf cbuf = mProtocol.BuildRequestBadCommand(command);
	return SendMasterMessage(cbuf, handler);
    }


    /**
     * 
     */
    private int SendMasterMessage(CommBuf cbuf, DispatchHandler handler) {
	int error;
	if ((error = mComm.SendRequest(mAddr, cbuf, handler)) != Error.OK) {
	    log.severe("Client.SendMasterMessage to " + mAddr + " failed - " + Error.GetText(error));
	    return error;
	}
	return Error.OK;
    }
 
    private Comm     mComm;
    private long     mTimeout;
    private Protocol mProtocol;
    private ConnectionManager mConnectionManager;
    private InetSocketAddress mAddr;

}

  
  
  


