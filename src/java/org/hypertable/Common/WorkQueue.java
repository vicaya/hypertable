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

    private static class UsageRec {
	long    threadGroup = 0;
	boolean running = false;
	int     outstanding = 1;
    }

    private static class WorkRec {
	Runnable request = null;
	UsageRec usage = null;
    }

    private static class Worker implements Runnable {

	LinkedList<WorkRec> mQueue;
	HashMap<Long, UsageRec> mUsageMap;

	Worker(LinkedList<WorkRec> workQueue, HashMap<Long, UsageRec> usageMap) {
	    mQueue    = workQueue;
	    mUsageMap = usageMap;
	}

	public void run() {
	    WorkRec rec = null;
	    ListIterator<WorkRec> iter;

	    try {

		while (!Thread.interrupted()) {

		    synchronized (mQueue) {

			while (mQueue.isEmpty()) {
			    mQueue.wait();
			}

			iter = mQueue.listIterator(0);
			synchronized (mUsageMap) {
			    while (iter.hasNext()) {
				rec = iter.next();
				if (rec.usage == null || !rec.usage.running) {
				    if (rec.usage != null)
					rec.usage.running = true;				    
				    iter.remove();
				    break;
				}
				else {
				    java.lang.System.err.println("Skipping request from thread group " + rec.usage.threadGroup + " because it's running");
				}
				rec = null;
			    }
			}
		    }
		    
		    if (rec != null) {
			rec.request.run();
			if (rec.usage != null) {
			    synchronized (mUsageMap) {
				rec.usage.running = false;
				rec.usage.outstanding--;
				if (rec.usage.outstanding == 0) {
				    mUsageMap.remove(rec.usage.threadGroup);
				}
			    }
			}
		    }

		}

	    }
	    catch (InterruptedException e) {
	    }
	    catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

    private LinkedList<WorkRec>     mQueue = new LinkedList<WorkRec>();
    private HashMap<Long, UsageRec> mUsageMap = new HashMap<Long, UsageRec>();
    private Thread [] threads = null;
    private int mRequestCount = 0;

    public WorkQueue(int workerCount) {
	assert (workerCount > 0);
	threads = new Thread[workerCount];
	for (int i=0; i<workerCount; i++) {
	    Worker worker = new Worker(mQueue, mUsageMap);
	    threads[i] = new Thread(worker, "WorkQueueThread " + i);
	    threads[i].start();
	}
    }

    public void Join() {
	try {
	    for (int i=0; i<threads.length; i++) {
		threads[i].join();
	    }
	}
	catch (InterruptedException e) {
	    e.printStackTrace();
	}
    }

    public void Shutdown() {
	for (int i=0; i<threads.length; i++) {
	    threads[i].interrupt();
	}
    }

    public void AddRequest(Runnable request) {
	synchronized (mQueue) {
	    WorkRec rec = new WorkRec();
	    rec.request = request;
	    mQueue.addLast(rec);
	    mQueue.notify();
	}
    }

    public void AddSerialRequest(long threadGroup, Runnable request) {
	WorkRec rec = new WorkRec();
	rec.request = request;
	synchronized (mUsageMap) {
	    rec.usage = mUsageMap.get(threadGroup);
	    if (rec.usage != null)
		rec.usage.outstanding++;
	    else {
		rec.usage = new UsageRec();
		rec.usage.threadGroup = threadGroup;
		mUsageMap.put(threadGroup, rec.usage);
	    }	    
	}
	synchronized (mQueue) {
	    mQueue.addLast(rec);
	    mQueue.notify();
	}
    }

}

