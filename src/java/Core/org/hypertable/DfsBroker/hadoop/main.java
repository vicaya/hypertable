/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

package org.hypertable.DfsBroker.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hypertable.AsyncComm.ApplicationQueue;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.ConnectionHandlerFactory;
import org.hypertable.AsyncComm.DispatchHandler;
import org.hypertable.AsyncComm.ReactorFactory;
import org.hypertable.Common.System;
import org.hypertable.Common.Usage;



public class main {

    static final Logger log = Logger.getLogger(
        "org.hypertable.DfsBroker.hadoop");

    static String usage[] = {
        "",
        "usage: java org.hypertable.DfsBroker.hadoop.main [OPTIONS]",
        "",
        "OPTIONS:",
        "  --help           Display this help text and exit",
        "  --config=<file>  Load configuration properties from <file>",
        "  --verbose        Generate verbose logging output",
        "",
        "This program brokers Hadoop Distributed Filesystem (HDFS) requests",
        "on behalf of remote clients.  It is intended to provide an efficient",
        "way for C++ programs to use HDFS.",
        "",
        null
    };


    static final String DEFAULT_PORT = "38030";

    private static HdfsBroker ms_broker;
    private static ApplicationQueue ms_app_queue;

    // Example shutdown hook class
    private static class ShutdownHook extends Thread {
        public void run() {
            java.lang.System.out.println("ShutdownHook called");
            ms_broker.mOpenFileMap.RemoveAll();
            ms_app_queue.Shutdown();
        }
    }

    private static class HandlerFactory implements ConnectionHandlerFactory {
        public HandlerFactory(Comm comm, ApplicationQueue appQueue,
                              HdfsBroker broker) {
            mComm = comm;
            mAppQueue = appQueue;
            mBroker = broker;
        }
        public DispatchHandler newInstance() {
            return new ConnectionHandler(mComm, mAppQueue, mBroker);
        }

        private Comm  mComm;
        private ApplicationQueue mAppQueue;
        private HdfsBroker mBroker;
    }

    public static void main(String [] args)
                            throws IOException, InterruptedException {
        int port;
        short reactorCount;
        int   workerCount;
        String configFile = null;
        String str;
        Properties props = new Properties();
        HandlerFactory handlerFactory;
        Comm comm;
        boolean verbose = false;

        if (args.length == 1 && args[0].equals("--help"))
            Usage.DumpAndExit(usage);

        for (int i=0; i<args.length; i++) {
            if (args[i].startsWith("--config="))
                configFile = args[i].substring(9);
            else if (args[i].equals("--verbose")) {
                verbose = true;
            }
            else
                Usage.DumpAndExit(usage);
        }

        ShutdownHook sh = new ShutdownHook();
        Runtime.getRuntime().addShutdownHook(sh);

        if (configFile == null)
            configFile = System.installDir + "/conf/hypertable.cfg";

        FileInputStream fis = new FileInputStream(configFile);
        props.load(fis);

        if (verbose)
            props.setProperty("verbose", "true");

        // Determine listen port
        str  = props.getProperty("HdfsBroker.Port", DEFAULT_PORT);
        port = Integer.parseInt(str);

        // Determine reactor count
        str = props.getProperty("HdfsBroker.Reactors");
        reactorCount = (str == null) ? (short)System.processorCount
                                     : Short.parseShort(str);

        // Determine worker count
        str = props.getProperty("HdfsBroker.Workers");
        workerCount = (str == null) ? (short)System.processorCount
                                    : Integer.parseInt(str);

        if (verbose) {
            java.lang.System.out.println("Num CPUs=" + System.processorCount);
            java.lang.System.out.println("HdfsBroker.Port=" + port);
            java.lang.System.out.println("HdfsBroker.Reactors=" + reactorCount);
            java.lang.System.out.println("HdfsBroker.Workers=" + workerCount);
        }

        ReactorFactory.Initialize(reactorCount);

        comm = new Comm(0);
        //Global.protocol = new Protocol();
        ms_app_queue = new ApplicationQueue(workerCount);

        ms_broker = new HdfsBroker(comm, props);
        handlerFactory = new HandlerFactory(comm, ms_app_queue, ms_broker);
        comm.Listen(port, handlerFactory, null);

        ms_app_queue.Join();
    }

}
