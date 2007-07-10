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

import java.util.LinkedList;
import java.util.logging.Logger;

public class CallbackHandlerSynchronizer implements CallbackHandler {

    static final Logger log = Logger.getLogger("org.hypertable.AsyncComm");

    public CallbackHandlerSynchronizer(long timeoutSecs) {
	mTimeoutMs = timeoutSecs*1000;
    }
  
    public void handle(Event event) {
	synchronized (this) {
	    mReceiveQueue.add(event);
	    notify();
	}
    }

    public synchronized Event WaitForEvent() throws InterruptedException {
	if (mReceiveQueue.isEmpty())
	    wait(mTimeoutMs);
	if (mReceiveQueue.isEmpty())
	    return null;
	return mReceiveQueue.remove();
    }

    private LinkedList<Event> mReceiveQueue = new LinkedList<Event>();
    private long mTimeoutMs;
}
