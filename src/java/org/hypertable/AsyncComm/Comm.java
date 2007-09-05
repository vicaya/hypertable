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
	mConnMap = new ConnectionMap();
    }

    public int Connect(InetSocketAddress addr, long timeout, DispatchHandler defaultHandler) {
	try {
	    SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(addr);
	    IOHandlerData handler = new IOHandlerData(channel, defaultHandler, mConnMap);
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

    public int Listen(int port, ConnectionHandlerFactory handlerFactory, DispatchHandler acceptHandler) {
	try {
	    ServerSocketChannel channel = ServerSocketChannel.open();
	    channel.socket().setReuseAddress(true);
	    channel.socket().bind(new InetSocketAddress(port));
	    channel.configureBlocking(false);
	    IOHandlerAccept handler = new IOHandlerAccept(channel, acceptHandler, mConnMap, handlerFactory);
	    handler.SetInterest(SelectionKey.OP_ACCEPT);
	}		
	catch (IOException e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
	return Error.OK;
    }
    
    public int SendRequest(InetSocketAddress addr, CommBuf cbuf, DispatchHandler responseHandler) {
	IOHandlerData handler = (IOHandlerData)mConnMap.Get(addr);
	int id;
	if (handler == null)
	    return Error.COMM_NOT_CONNECTED;

	cbuf.ResetDataPointers();

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

	cbuf.ResetDataPointers();

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

    public ConnectionMap mConnMap;
}

