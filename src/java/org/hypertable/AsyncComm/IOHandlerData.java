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

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypertable.Common.Error;

class IOHandlerData extends IOHandler {

    private static AtomicInteger msNextId = new AtomicInteger(1);
    
    public IOHandlerData(SocketChannel chan, DispatchHandler dh, ConnectionMap cm) {
	super(chan, dh, cm);
	mSocketChannel = chan;
	header = ByteBuffer.allocate(1024);
	header.order(ByteOrder.LITTLE_ENDIAN);
	header.limit(Message.HEADER_LENGTH);
	mGotHeader = false;
	mSendQueue = new LinkedList<CommBuf>();
	mShutdown = false;
	mId = msNextId.getAndIncrement();
    }

    public void SetRemoteAddress(InetSocketAddress addr) {
	mAddr = addr;
    }

    public void SetTimeout(long timeout) {
      mTimeout = timeout;
    }

    InetSocketAddress GetAddress() { return mAddr; }

    public void run(SelectionKey selkey) {
	try {
	    Socket socket = mSocketChannel.socket();

	    if (!selkey.isValid()) {
		selkey.cancel();
		if (mAddr != null)
		    mConnMap.Remove(mAddr);
		DeliverEvent( new Event(Event.Type.DISCONNECT, mAddr, Error.COMM_BROKEN_CONNECTION) );
		mReactor.CancelRequests(this);
	    }

	    if (selkey.isConnectable()) {
		try {
		    if (mSocketChannel.finishConnect() == false) {
			selkey.cancel();
			System.err.println("Connection error");
			return;
		    }
		    mAddr = new InetSocketAddress(socket.getInetAddress(), socket.getPort());
		    DeliverEvent( new Event(Event.Type.CONNECTION_ESTABLISHED, mAddr, Error.OK) );
		    SetInterest(SelectionKey.OP_READ);
		    mConnMap.Put(mAddr, this);
		}
		catch (ConnectException e) {
		    DeliverEvent( new Event(Event.Type.DISCONNECT, mAddr, Error.COMM_CONNECT_ERROR) );
		    mReactor.CancelRequests(this);
		}
		return;
	    }
	    
	    if (selkey.isReadable()) {
		int nread;
		while (true) {
		    if (!mGotHeader) {
			nread = mSocketChannel.read(header);
			if (nread == -1) {
			    selkey.cancel();
			    mConnMap.Remove(mAddr);
			    DeliverEvent( new Event(Event.Type.DISCONNECT, mAddr, Error.COMM_BROKEN_CONNECTION) );
			    mReactor.CancelRequests(this);
			    return;
			}
			if (header.hasRemaining())
			    return;
			mGotHeader = true;
			header.flip();
			message = new Message();
			message.ReadHeader(header, mId);
			message.buf = ByteBuffer.allocate(message.totalLen);
			message.buf.order(ByteOrder.LITTLE_ENDIAN);
			header.flip();
			message.buf.put(header);
		    }
		    if (mGotHeader) {
			nread = mSocketChannel.read(message.buf);
			if (nread == -1) {
			    selkey.cancel();
			    mConnMap.Remove(mAddr);
			    DeliverEvent( new Event(Event.Type.DISCONNECT, mAddr, Error.COMM_BROKEN_CONNECTION) );
			    return;
			}
			if (message.buf.hasRemaining())
			    return;
			message.buf.flip();
			if ((message.flags & Message.FLAGS_MASK_REQUEST) == 0) {
			    DispatchHandler handler = mReactor.RemoveRequest(message.id);
			    if (handler == null)
				log.info("Received response for non-pending event (id=" + message.id + ")");
			    else
				DeliverEvent( new Event(Event.Type.MESSAGE, mAddr, Error.OK, message), handler );
			}
			else
			    DeliverEvent( new Event(Event.Type.MESSAGE, mAddr, Error.OK, message) );
			mGotHeader = false;
			header.clear();
			header.limit(Message.HEADER_LENGTH);
		    }
		}
	    }

	    if (selkey.isWritable()) {
		synchronized (this) {
		    if (!mSendQueue.isEmpty()) {
			CommBuf cbuf;
			int nwritten;
			// try to drain the output queue
			while (!mSendQueue.isEmpty()) {
			    cbuf = mSendQueue.peek();
			    if (!SendBuf(cbuf))
				break;
			    mSendQueue.remove();
			}
			if (mSendQueue.isEmpty())
			    RemoveInterest(SelectionKey.OP_WRITE);
		    }
		}
	    }
	}
	catch (IOException e) {
	    selkey.cancel();
	    if (mAddr != null)
		mConnMap.Remove(mAddr);
	    DeliverEvent( new Event(Event.Type.DISCONNECT, mAddr, Error.COMM_BROKEN_CONNECTION) );
	    e.printStackTrace();
	}
    }

    synchronized void RegisterRequest(int id, DispatchHandler responseHandler) {
	long now  = System.currentTimeMillis();
	mReactor.AddRequest(id, this, responseHandler, now + mTimeout);
    }

    boolean SendBuf(CommBuf cbuf) throws IOException {
	int nwritten;
	if (cbuf.data != null && cbuf.data.remaining() > 0) {
	    nwritten = mSocketChannel.write(cbuf.data);
	    if (cbuf.data.remaining() > 0)
		return false;
	}
	if (cbuf.ext != null && cbuf.ext.remaining() > 0) {
	    nwritten = mSocketChannel.write(cbuf.ext);
	    if (cbuf.ext.remaining() > 0)
		return false;
	}
	return true;
    }

    synchronized int SendMessage(CommBuf cbuf) {
	CommBuf tbuf;
	boolean initiallyEmpty = mSendQueue.isEmpty();

	try {
	    // try to drain the send queue
	    while (!mSendQueue.isEmpty()) {
		tbuf = mSendQueue.peek();
		if (!SendBuf(tbuf))
		    break;
		mSendQueue.remove();
	    }

	    // try to send the message directly
	    if (mSendQueue.isEmpty()) {
		if (SendBuf(cbuf)) {
		    if (!initiallyEmpty)
			RemoveInterest(SelectionKey.OP_WRITE);
		    return Error.OK;
		}
	    }

	    mSendQueue.add(cbuf);
	    if (initiallyEmpty)
		AddInterest(SelectionKey.OP_WRITE);
	}
	catch (Exception e) {
	    e.printStackTrace();
	    Shutdown();
	    return Error.COMM_BROKEN_CONNECTION;
	}
	return Error.OK;
    }

    private SocketChannel mSocketChannel;
    private ByteBuffer header;
    private boolean mGotHeader;
    private Message message;
    private LinkedList<CommBuf> mSendQueue;
    private long mTimeout;
    private boolean mShutdown;
    private int mId;
}
