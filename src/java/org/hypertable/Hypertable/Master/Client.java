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

import org.hypertable.AsyncComm.CallbackHandler;
import org.hypertable.AsyncComm.CallbackHandlerSynchronizer;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.ConnectionManager;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.MessageBuilder;
import org.hypertable.AsyncComm.MessageBuilderSimple;
import org.hypertable.AsyncComm.MsgId;
import org.hypertable.AsyncComm.ReactorFactory;


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

	mManager = new ConnectionManager("Waiting for Bigtable.Master...");
	mManager.Initiate(mComm, mAddr, mTimeout);
    }

    public boolean WaitForReady() throws InterruptedException {
	if (!mManager.WaitForConnection(mTimeout)) {
	    log.warning("Timed out waiting for connection to master at " + mAddr);
	    return false;
	}
	return true;
    }

 
   /**
     */
    public int CreateTable(String tableName, String schema, CallbackHandler handler, MsgId msgId) {
	CommBuf cbuf = mProtocol.BuildRequestCreateTable(tableName, schema);
	return SendMasterMessage(cbuf, handler, msgId);
    }

    /**
     */
    public int CreateTable(String tableName, String schema) throws InterruptedException {
	CallbackHandlerSynchronizer handler = new CallbackHandlerSynchronizer(mTimeout);
	CommBuf cbuf = mProtocol.BuildRequestCreateTable(tableName, schema);
	MsgId msgId = new MsgId();
	int error = SendMasterMessage(cbuf, handler, msgId);
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
    public int GetSchema(String tableName, StringBuilder schema, CallbackHandler handler, MsgId msgId) {
	CommBuf cbuf = mProtocol.BuildRequestGetSchema(tableName);
	return SendMasterMessage(cbuf, handler, msgId);
    }

    /**
     * Get schema, blocking
     */
    public int GetSchema(String tableName, StringBuilder schema) throws InterruptedException {
	CallbackHandlerSynchronizer handler = new CallbackHandlerSynchronizer(mTimeout);
	CommBuf cbuf = mProtocol.BuildRequestGetSchema(tableName);
	MsgId msgId = new MsgId();
	int error = SendMasterMessage(cbuf, handler, msgId);
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
		event.msg.buf.getShort(); // skip command
		String schemaString = CommBuf.DecodeString(event.msg.buf);
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
    public int BadCommand(short command, CallbackHandler handler, MsgId msgId) {
	CommBuf cbuf = mProtocol.BuildRequestBadCommand(command);
	return SendMasterMessage(cbuf, handler, msgId);
    }


    /**
     * 
     */
    private int SendMasterMessage(CommBuf cbuf, CallbackHandler handler, MsgId msgId) {
	int error;
	if ((error = mComm.SendRequest(mAddr, cbuf, handler)) != Error.OK) {
	    log.severe("Client.SendMasterMessage to " + mAddr + " failed - " + Error.GetText(error));
	    return error;
	}
	msgId.set(cbuf.id);
	return Error.OK;
    }
 
    private Comm     mComm;
    private long     mTimeout;
    private Protocol mProtocol;
    private ConnectionManager mManager;
    private InetSocketAddress mAddr;

}

  
  
  


