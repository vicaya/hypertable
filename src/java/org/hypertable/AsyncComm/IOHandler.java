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
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public abstract class IOHandler {

    protected static final Logger log = Logger.getLogger("org.hypertable");

    public IOHandler(AbstractSelectableChannel chan, CallbackHandler cb, ConnectionMap cm, Event.Queue eq) {
	mChannel = chan;
	mCallback = cb;
	mConnMap = cm;
	mEventQueue = eq;
	mReactor = ReactorFactory.Get();
	mAddr = null;
	mInterest = 0;
    }

    public void DeliverEvent(Event event, CallbackHandler cb) {
	if (mEventQueue != null)
	    mEventQueue.Add(event);  // ??? , handler);
	else
	    cb.handle(event);
    }

    public void DeliverEvent(Event event) {
	if (mEventQueue != null)
	    mEventQueue.Add(event);  // ??? , handler);
	else if (mCallback != null)
	    mCallback.handle(event);
	else
	    log.info(event.toString());
    }


    public abstract void run(SelectionKey selkey);

    public void Register(Selector selector) {
	try {
	    if (mShuttingDown) {
		SelectionKey selkey = mChannel.register(selector, 0);
		selkey.cancel();
	    }
	    else {
		mChannel.register(selector, mInterest, this);
	    }
	}
	catch (ClosedChannelException e) {
	    e.printStackTrace();
	    if (mAddr != null)
		mConnMap.Remove(mAddr);
	}
    }

    public void Shutdown() {
	mShuttingDown = true;
	mReactor.AddToRegistrationQueue(this);
	mReactor.WakeUp();
    }

    public void SetInterest(int interest) {
	mInterest = interest;
	mReactor.AddToRegistrationQueue(this);
	mReactor.WakeUp();
    }

    public void AddInterest(int interest) {
	mInterest |= interest;
	mReactor.AddToRegistrationQueue(this);
	mReactor.WakeUp();
    }

    public void RemoveInterest(int interest) {
	mInterest &= ~interest;
	mReactor.AddToRegistrationQueue(this);
	mReactor.WakeUp();
    }

    Reactor GetReactor() { return mReactor; }

    void SetCallback(CallbackHandler cb) { mCallback = cb; }

    protected InetSocketAddress mAddr;
    protected CallbackHandler mCallback;
    protected ConnectionMap mConnMap;
    protected Event.Queue mEventQueue;
    protected Reactor mReactor;
    protected AbstractSelectableChannel mChannel;
    protected int mInterest;
    protected boolean mShuttingDown = false;
}
