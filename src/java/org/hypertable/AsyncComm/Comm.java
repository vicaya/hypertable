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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.io.IOException;

import org.hypertable.Common.Error;

public class Comm {

    public Comm(int handlerCount) {
	if (handlerCount != 0)
	    throw new AssertionError("handlerCount != 0");
	mEventQueue = null;
	mConnMap = new ConnectionMap();
    }

    public int Connect(InetSocketAddress addr, long timeout, CallbackHandler defaultHandler) {
	try {
	    SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(addr);
	    IOHandlerData handler = new IOHandlerData(channel, defaultHandler, mConnMap, mEventQueue);
	    handler.SetTimeout(timeout);
	    handler.SetRemoteAddress(addr);
	    mConnMap.Put(addr, handler);
	    handler.SetInterest(SelectionKey.OP_CONNECT);
	}
	catch (IOException e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
	return Error.OK;
    }

    public int Listen(int port, ConnectionHandlerFactory handlerFactory, CallbackHandler acceptHandler) {
	try {
	    ServerSocketChannel channel = ServerSocketChannel.open();
	    channel.socket().setReuseAddress(true);
	    channel.socket().bind(new InetSocketAddress(port));
	    channel.configureBlocking(false);
	    IOHandlerAccept handler = new IOHandlerAccept(channel, acceptHandler, mConnMap, mEventQueue, handlerFactory);
	    handler.SetInterest(SelectionKey.OP_ACCEPT);
	}		
	catch (IOException e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
	return Error.OK;
    }
    
    public int SendRequest(InetSocketAddress addr, CommBuf cbuf, CallbackHandler responseHandler) {
	IOHandlerData handler = (IOHandlerData)mConnMap.Get(addr);
	int id;
	if (handler == null)
	    return Error.COMM_NOT_CONNECTED;

	// Set the REQUEST flag
	ByteBuffer headerBuf = (cbuf.data != null) ? cbuf.data : cbuf.ext;
	headerBuf.mark();
	headerBuf.position(headerBuf.position()+2);  // skip to flags
	byte flags = headerBuf.get();
	flags |= Message.FLAGS_MASK_REQUEST;
	headerBuf.get(); // skip headerLen
	id = headerBuf.getInt();
	headerBuf.position(headerBuf.position()-6);  // skip back to flags
	headerBuf.put(flags);
	headerBuf.reset();

	handler.RegisterRequest(id, responseHandler);

	int error = handler.SendMessage(cbuf);
	if (error == Error.COMM_BROKEN_CONNECTION)
	    mConnMap.Remove(addr);
	return error;
    }

    public int SendResponse(InetSocketAddress addr, CommBuf cbuf) {
	IOHandlerData handler = (IOHandlerData)mConnMap.Get(addr);
	if (handler == null)
	    return Error.COMM_NOT_CONNECTED;

	// Clear the REQUEST flag
	ByteBuffer headerBuf = (cbuf.data != null) ? cbuf.data : cbuf.ext;
	headerBuf.mark();
	headerBuf.position(headerBuf.position()+2);  // skip to flags
	byte flags = headerBuf.get();
	flags &= Message.FLAGS_MASK_RESPONSE;
	headerBuf.position(headerBuf.position()-1);  // skip back to flags
	headerBuf.put(flags);
	headerBuf.reset();

	int error = handler.SendMessage(cbuf);
	if (error == Error.COMM_BROKEN_CONNECTION)
	    mConnMap.Remove(addr);
	return error;
    }

    public void SetHandler(InetSocketAddress addr, CallbackHandler defaultHandler) {
	IOHandlerData handler = (IOHandlerData)mConnMap.Get(addr);
	if (handler != null)
	    handler.SetCallback(defaultHandler);
    }

    public Event.Queue mEventQueue;
    public ConnectionMap mConnMap;
}

