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

import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SelectionKey;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.hypertable.Common.Error;

class IOHandlerAccept extends IOHandler {
    
    public IOHandlerAccept(ServerSocketChannel chan, CallbackHandler cb, ConnectionMap cm, Event.Queue eq, ConnectionHandlerFactory chf) {
	super(chan, cb, cm, eq);
	mServerSocketChannel = chan;
	mHandlerFactory = chf;
    }

    public void run(SelectionKey selkey) {
	try {
	    if (selkey.isAcceptable()) {
		SocketChannel newChannel = mServerSocketChannel.accept();
		newChannel.configureBlocking(false);
		IOHandlerData handler = new IOHandlerData(newChannel, mHandlerFactory.newInstance(), mConnMap, mEventQueue);
		Socket socket = newChannel.socket();
		InetSocketAddress addr = new InetSocketAddress(socket.getInetAddress(), socket.getPort());
		handler.SetRemoteAddress(addr);
		handler.SetInterest(SelectionKey.OP_READ);
		mConnMap.Put(addr, handler);
		DeliverEvent( new Event(Event.Type.CONNECTION_ESTABLISHED, addr, Error.OK) );
	    }
	}
	catch (Throwable e) {
	    e.printStackTrace();
	}
    }

    private ServerSocketChannel mServerSocketChannel;
    private ConnectionHandlerFactory mHandlerFactory;
}

