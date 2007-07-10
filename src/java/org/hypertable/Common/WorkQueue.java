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


package org.hypertable.Common;

import java.util.LinkedList;
import java.util.ListIterator;
import java.util.HashMap;

public class WorkQueue {

    public static LinkedList<Runnable> msQueue = new LinkedList<Runnable>();
    public static HashMap<Integer, Integer> msUsageCounters = new HashMap<Integer, Integer>();

    private static class Worker implements Runnable {

	public void run() {
	    Runnable request = null;

	    try {

		while (!Thread.interrupted()) {

		    synchronized (msQueue) {

			while (msQueue.isEmpty())
			    msQueue.wait();

			request = msQueue.remove();

		    }

		    request.run();
		}

	    }
	    catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

    public WorkQueue(int workerCount) {
	assert (workerCount > 0);
	for (int i=0; i<workerCount; i++) {
	    Worker worker = new Worker();
	    (new Thread(worker, "HdfsBrokerWorker " + i)).start();
	}
    }

    public void AddRequest(Runnable request) {
	synchronized (msQueue) {
	    msQueue.addLast(request);
	    msQueue.notify();
	}
    }

}

