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

package org.hypertable.AsyncComm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import org.hypertable.Common.Error;

class CommTestThreadFunction implements Runnable {

    public CommTestThreadFunction(Comm comm, InetSocketAddress addr, String input) {
	mComm = comm;
	mAddr = addr;
	mInputFile = input;
    }

    public void SetOutputFile(String output) { 
	mOutputFile = output;
    }
    
    public void run() {
	CommBuf cbuf;
	Event event;
	HeaderBuilder hbuilder = new HeaderBuilder();
	ResponseHandler respHandler = new ResponseHandler();
	String str;
	int error;
	int maxOutstanding = 50;
	int outstanding = 0;
	int retries;

	try {
	    BufferedReader infile  = new BufferedReader(new FileReader(mInputFile));
	    BufferedWriter outfile = new BufferedWriter(new FileWriter(mOutputFile));

	    while ((str = infile.readLine()) != null) {
		hbuilder.Reset(Message.PROTOCOL_NONE);
		cbuf = new CommBuf(CommBuf.EncodedLength(str) + Message.HEADER_LENGTH);
		cbuf.PrependString(str);
		hbuilder.Encapsulate(cbuf);
		retries = 0;
		while ((error = mComm.SendRequest(mAddr, cbuf, respHandler)) != Error.OK) {
		    if (error == Error.COMM_NOT_CONNECTED) {
			if (retries == 5) {
			    System.out.println("Connection timeout.");
			    System.exit(1);
			}
			Integer intObj = new Integer(0);
			synchronized (intObj) {
			    intObj.wait(1000);
			}
			retries++;
		    }
		    else {
			System.err.println("CommEngine.SendMessage returned '" + Error.GetText(error) + "'");
			System.exit(1);
		    }
		}
		outstanding++;

		if (outstanding  > maxOutstanding) {
		    if ((event = respHandler.GetResponse()) == null)
			break;
		    event.msg.RewindToProtocolHeader();
		    str = CommBuf.DecodeString(event.msg.buf);
		    outfile.write(str);
		    outfile.write("\n");
		    outstanding--;
		}
	    }

	    while (outstanding > 0 && (event = respHandler.GetResponse()) != null) {
		event.msg.RewindToProtocolHeader();
		str = CommBuf.DecodeString(event.msg.buf);
		outfile.write(str);
		outfile.write("\n");
		outstanding--;
	    }
	    outfile.close();
	    infile.close();
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private static class ResponseHandler implements CallbackHandler {

	public void handle(Event event) {
	    synchronized (this) {
		if (event.type == Event.Type.MESSAGE) {
		    mQueue.add(event);
		    notify();
		}
		else {
		    System.err.println(event.toString());
		    mConnected = false;
		    notify();
		}
	    }
	}

	public synchronized Event GetResponse() throws InterruptedException {
	    while (mQueue.isEmpty()) {
		wait();
		if (mConnected == false)
		    return null;
	    }
	    return mQueue.remove();
	}

	private LinkedList<Event> mQueue = new LinkedList<Event>();
	private boolean           mConnected = true;
    };

    private Comm mComm;
    private InetSocketAddress mAddr;
    String mInputFile;
    String mOutputFile;
};

