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

import java.util.LinkedList;
import java.util.ListIterator;
import java.util.HashMap;
import java.util.Vector;

public class WorkQueue {

    public static LinkedList<Request> msQueue = new LinkedList<Request>();
    public static HashMap<Integer, Integer> msUsageCounters = new HashMap<Integer, Integer>();
    public static boolean msShutdown = false;

    public static void Shutdown() {
	synchronized (msQueue) {
	    msShutdown = true;
	    msQueue.notifyAll();
	}
    }

    private static class Worker implements Runnable {

	public void run() {
	    Request request = null;
	    Integer usageCount;
	    int     fid;

	    try {

		while (true) {

		    synchronized (msQueue) {

			if (request != null) {
			    fid = request.GetFileId();
			    usageCount = msUsageCounters.get(fid);
			    msUsageCounters.put(fid, usageCount - 1);
			}

			ListIterator<Request> iterator = msQueue.listIterator(0);
			request = null;
			while (iterator.hasNext()) {
			    request = iterator.next();
			    fid = request.GetFileId();
			    usageCount = msUsageCounters.get(fid);
			    if (usageCount == null || usageCount.intValue() == 0) {
				msUsageCounters.put(fid, 1);
				iterator.remove();
				break;
			    }
			    else
				request = null;
			}

			if (request == null) {
			    if (msShutdown)
				return;
			    msQueue.wait();
			}
		    }

		    if (request != null)
			request.run();
		}
	    }
	    catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

    private Vector<Thread> mThreadVector = new Vector<Thread>();

    public WorkQueue(int workerCount) {
	assert (workerCount > 0);
	for (int i=0; i<workerCount; i++) {
	    Worker worker = new Worker();
	    Thread thread = new Thread(worker, "HdfsBrokerWorker " + i);
	    thread.start();
	    mThreadVector.add(thread);
	}
    }


    public void AddRequest(Request request) {
	if (msShutdown)
	    return;
	Integer usageCount;
	synchronized (msQueue) {
	    msQueue.addLast(request);
	    usageCount = msUsageCounters.get(request.GetFileId());
	    if (usageCount == null || usageCount.intValue() == 0)
		msQueue.notify();
	}
    }

    public void Join() throws InterruptedException {
	for (Thread t : mThreadVector) {
	    t.join();
	}
    }

}

