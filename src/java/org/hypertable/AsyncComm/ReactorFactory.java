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

import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;

public class ReactorFactory {

    public static void Initialize(short count) throws IOException {
	reactors = new Reactor [count];
	for (int i=0; i<count; i++) {
	    reactors[i] = new Reactor();
	    Thread thread = new Thread(reactors[i], "Reactor " + i);
	    thread.setPriority(Thread.MAX_PRIORITY);
	    thread.start();
	}
    }

    public static void Shutdown() {
	for (int i=0; i<reactors.length; i++)
	    reactors[i].Shutdown();
    }

    public static Reactor Get() {
	return reactors[nexti.getAndIncrement() % reactors.length];
    }

    private static AtomicInteger nexti = new AtomicInteger(0);

    private static Reactor [] reactors;
}

