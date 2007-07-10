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


package org.hypertable.HdfsBroker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.hypertable.AsyncComm.Event;


public abstract class Request implements Runnable {

    public Request(OpenFileMap ofmap, Event event) {
	mOpenFileMap = ofmap;
	mEvent = event;
    }

    static final Logger log = Logger.getLogger("org.hypertable.HdfsBroker");

    public abstract void run();

    public int GetFileId() { return mFileId; }

    protected OpenFileMap mOpenFileMap;
    protected OpenFileData  mOpenFileData;
    protected Event  mEvent;
    protected int mFileId;

    protected static AtomicInteger msUniqueId = new AtomicInteger(0);

}
