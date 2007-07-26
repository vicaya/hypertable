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

import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

import org.junit.*;
import static org.junit.Assert.*;

public class CommTest {

    static final Logger log = Logger.getLogger("org.hypertable.AsyncComm.CommTest");

    static final int DEFAULT_PORT = 32998;
    static final String DEFAULT_PORT_ARG = "--port=32998";

    private static class ServerLauncher {

	private static class OutputRedirector implements Runnable {
	    public OutputRedirector(Process proc) {
		mProc = proc;
	    }
	    public void run() {
		byte [] buf = new byte [ 128 ];
		int nread, rval = 0;
		InputStream istream = mProc.getInputStream();
		try {
		    while ((nread = istream.read(buf)) > 0)
			System.out.print(new String(buf, 0, nread));
		    rval = mProc.waitFor();
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	    private Process mProc;
	}

	public ServerLauncher() {
	    try {
		ProcessBuilder pbuilder = new ProcessBuilder("java", "-classpath", System.getProperty("java.class.path"),
							     "org.hypertable.AsyncComm.SampleServer", DEFAULT_PORT_ARG);
		pbuilder.redirectErrorStream(true);
		mProc = pbuilder.start();
		Integer intObj = new Integer(0);
		synchronized (intObj) {
		    intObj.wait(2000);
		}

		mOutputRedirectorThread = new Thread(new OutputRedirector(mProc), "CommTest Output Redirector");
		mOutputRedirectorThread.start();
	    }
	    catch (Exception e) {
		e.printStackTrace();
		msShutdown = true;
	    }
	}
	public void Kill() {
	    mProc.destroy();
	}
	private Process mProc;
	private Thread  mOutputRedirectorThread;
    };

    @Before public void setUp() {

	try {

	    ReactorFactory.Initialize((short)1);

	    slauncher = new ServerLauncher(); 

	    mAddr = new InetSocketAddress("localhost", DEFAULT_PORT);

	    mComm = new Comm(0);

	    ConnectionManager.Callback connHandler = new ConnectionManager.Callback(mComm, mAddr, 5);

	    connHandler.SendConnectRequest();

	    if (!connHandler.WaitForConnection(30)) {
		log.fine("Connect error");
		msShutdown = true;
		return;
	    }

	    (new File("/tmp/CommTest.output.1")).delete();
	    (new File("/tmp/CommTest.output.2")).delete();

	    CommTestThreadFunction threadFunc1 = new CommTestThreadFunction(mComm, mAddr, "/usr/share/dict/words");
	    threadFunc1.SetOutputFile("/tmp/CommTest.output.1");
	    mThread1 = new Thread(threadFunc1, "CommTest thread 1");
	    mThread1.start();

	    CommTestThreadFunction threadFunc2 = new CommTestThreadFunction(mComm, mAddr, "/usr/share/dict/words");
	    threadFunc2.SetOutputFile("/tmp/CommTest.output.2");
	    mThread2 = new Thread(threadFunc2, "CommTest thread 2");
	    mThread2.start();
	}
	catch (Exception e) {
	    e.printStackTrace();
	    msShutdown = true;
	}
    }
    
    @Test public void testComm() {

	System.out.println("CommTest ...");

	if (msShutdown)
	    assertTrue(false);

	try {
	    mThread1.join();
	    mThread2.join();
	    
	    ProcessBuilder pbuilder;
	    Process proc;
	    InputStream istream;
	    byte [] buf = new byte [ 128 ];
	    int nread, rval = 0;

	    pbuilder = new ProcessBuilder("diff", "/usr/share/dict/words", "/tmp/CommTest.output.1");
	    pbuilder.redirectErrorStream(true);
	    proc = pbuilder.start();
	    istream = proc.getInputStream();
	    while ((nread = istream.read(buf)) > 0)
		System.out.print(new String(buf, 0, nread));
	    assertTrue(proc.waitFor() == 0);

	    pbuilder = new ProcessBuilder("diff", "/tmp/CommTest.output.1", "/tmp/CommTest.output.2");
	    pbuilder.redirectErrorStream(true);
	    proc = pbuilder.start();
	    istream = proc.getInputStream();
	    while ((nread = istream.read(buf)) > 0)
		System.out.print(new String(buf, 0, nread));
	    assertTrue(proc.waitFor() == 0);

	}
	catch (Exception e) {
	    e.printStackTrace();
	    assertTrue(false);
	}
    }

    @After public void tearDown() {
	slauncher.Kill();
    }

    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(CommTest.class);
    }

    public static void main(String args[]) {
	org.junit.runner.JUnitCore.main("org.hypertable.AsyncComm.CommTest");
    }

    private Comm mComm;
    private ServerLauncher    slauncher;
    private InetSocketAddress mAddr;
    protected static boolean  msShutdown = false;
    Thread mThread1;
    Thread mThread2;
}
