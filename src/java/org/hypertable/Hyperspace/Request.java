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

package org.hypertable.Hyperspace;

import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.HeaderBuilder;

public abstract class Request implements Runnable {

    static final Logger log = Logger.getLogger("org.hypertable.Hyperspace");

    public Request(Event event) {
	mEvent = event;
    }

    //public abstract void run();

    protected Event  mEvent;
    protected HeaderBuilder mHeaderBuilder = new HeaderBuilder();
}
