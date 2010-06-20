/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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

package org.hypertable.examples.PerformanceTest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Vector;

import org.hypertable.thriftgen.*;
import org.hypertable.thrift.ThriftClient;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;
import org.hypertable.Common.HypertableException;
import org.hypertable.Common.ProgressMeterVertical;
import org.hypertable.Common.Usage;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.CommHeader;
import org.hypertable.AsyncComm.ConnectionHandlerFactory;
import org.hypertable.AsyncComm.DispatchHandler;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ReactorFactory;

public class Dispatcher {

  static final Logger log = Logger.getLogger("org.hypertable.examples");

  private static class RequestHandler implements DispatchHandler {

    public RequestHandler(long timeout) {
      mTimeout = timeout;
    }

    public void handle(Event event) {
      if (event.type == Event.Type.CONNECTION_ESTABLISHED) {
        synchronized (this) {
          mConnections++;
          mStartTime = System.currentTimeMillis();
          notify();
        }
      }
      else if (event.type == Event.Type.DISCONNECT) {
        synchronized (this) {
          mConnections--;
          mStartTime = System.currentTimeMillis();
          notify();
        }
      }
      else if (event.type == Event.Type.ERROR) {
        log.info("Error : " + Error.GetText(event.error));
        System.exit(1);
      }
      else if (event.type == Event.Type.MESSAGE) {
        synchronized (this) {
          Event newEvent = new Event(event);
          mQueue.offer(newEvent);
          notify();
        }
      }
    }

    public synchronized Event GetRequest() throws InterruptedException {
      while (mQueue.isEmpty()) {
        wait();
      }
      return mQueue.remove();
    }

    public synchronized int waitForConnections(int clients) 
      throws HypertableException, InterruptedException {
      long now;
      if (clients > 0 && mConnections == clients)
        return mConnections;
      mStartTime = System.currentTimeMillis();
      now = mStartTime;
      while (now - mStartTime < mTimeout) {
        wait(mTimeout - (now-mStartTime));
        if (clients > 0 && mConnections == clients)
          break;
        now = System.currentTimeMillis();
      }
      if (clients != 0 && clients < mConnections)
        throw new HypertableException(Error.FAILED_EXPECTATION,
                                      "Expected " + clients + " clients, only got " + mConnections);
      return mConnections;
    }

    private LinkedList<Event> mQueue = new LinkedList<Event>();
    private int mConnections = 0;
    private long mStartTime = 0;
    private long mTimeout;
  }

  private static class HandlerFactory implements ConnectionHandlerFactory {
    public HandlerFactory(DispatchHandler cb) {
      mDispatchHandler = cb;
    }
    public DispatchHandler newInstance() {
      return mDispatchHandler;
    }
    private DispatchHandler mDispatchHandler;
  }

  static String usage[] = {
    "",
    "usage: Dispatcher [OPTIONS] read|write|scan <keyCount> <valueSize>",
    "",
    "OPTIONS:",
    "  --help                  Display this help text and exit",
    "  --clients=<n>           Wait for <n> clients to connect",
    "  --driver=<s>            Which DB driver to use.  Valid values include:",
    "                          hypertable, hbase (default = hypertable)",
    "  --key-size=<n>          Key size (default = 10)",
    "  --measure-latency       Measure request latency (default is false)",
    "  --output-dir=<dir>      Directory to write test report into (default is cwd)",
    "  --port=<n>              Specifies the listen port (default = 11256)",
    "  --timeout=<ms>          Wait for connection timeout in milliseconds",
    "                          (default = 5000)",
    "  --random                Random order",
    "  --randomize-tasks       Randomize the task queue before starting test",
    "  --scan-buffer-size=<n>  Size of scan result transfer buffer in number of bytes",
    "                          (default = 65K)",
    "  --submit-at-most=<n>    Submit no more than <n> keys, regardless of <keyCount>",
    "  --submit-exactly=<n>    Submit exactly <n> keys, replicating sequence generated by",
    "                          <keyCount> if necessary",
    "  --test-name=<name>      Test name used as filename prefix for summary output file",
    "  --zipf                  For random order, use Zipfian distribution",
    "                          (default is uniform)",
    "",
    "This program is part of a performance test suite.  It acts as a",
    "dispatcher, handing out parcels of work to test clients.",
    "",
    null
  };

  static final int DEFAULT_PORT = 11256;
  static final int DEFAULT_KEY_SIZE = 10;

  public static String testTypeString(Task.Order order, Task.Type testType) {
    String orderStr = "";
    String typeStr = "";
    if (testType == Task.Type.READ)
      typeStr = "read";
    else if (testType == Task.Type.WRITE)
      typeStr = "write";
    else if (testType == Task.Type.SCAN)
      typeStr = "scan";
    else
      typeStr = "unknown";
    if (testType != Task.Type.SCAN) {
      if (order == Task.Order.SEQUENTIAL)
        orderStr = "sequential-";
      else if (order == Task.Order.RANDOM)
        orderStr = "random-";
      else
        orderStr = "unknown-";
    }
    return orderStr + typeStr;
  }


  public static void main(String [] args)
    throws InterruptedException, IOException {
    int port = DEFAULT_PORT;
    Event event;
    CommHeader header;
    long timeout = 15000;
    int clients = 0;
    Task.Type testType = Task.Type.READ;
    Task.Order order = Task.Order.SEQUENTIAL;
    Task.Distribution distribution = Task.Distribution.UNIFORM;
    String driver = "hypertable";
    String outputDir = null;
    String testName = null;
    boolean testTypeSet = false;
    long keyCount = -1;
    long submitExactly = -1;
    long submitAtMost = -1;
    long keysSubmitted = 0;
    int keySize = DEFAULT_KEY_SIZE;
    int valueSize = -1;
    int scanBufferSize = 65536;
    boolean randomizeTasks = false;
    String argString = "";

    if (args.length == 1 && args[0].equals("--help"))
      Usage.DumpAndExit(usage);

    for (int i=0; i<args.length; i++) {
      if (args[i].startsWith("--port="))
        port = Integer.parseInt(args[i].substring(7));
      else if (args[i].startsWith("--clients="))
        clients = Integer.parseInt(args[i].substring(10));
      else if (args[i].startsWith("--driver="))
        driver = args[i].substring(9);
      else if (args[i].startsWith("--key-size="))
        keySize = Integer.parseInt(args[i].substring(11));
      else if (args[i].startsWith("--submit-at-most="))
        submitAtMost = Long.parseLong(args[i].substring(17));
      else if (args[i].startsWith("--submit-exactly="))
        submitExactly = Long.parseLong(args[i].substring(17));
      else if (args[i].startsWith("--output-dir="))
        outputDir = args[i].substring(13);
      else if (args[i].equals("--random"))
        order = Task.Order.RANDOM;
      else if (args[i].equals("--randomize-tasks"))
        randomizeTasks = true;
      else if (args[i].equals("--timeout="))
        timeout = Long.parseLong(args[i].substring(10));
      else if (args[i].startsWith("--scan-buffer-size="))
        scanBufferSize = Integer.parseInt(args[i].substring(19));
      else if (args[i].startsWith("--test-name="))
        testName = args[i].substring(12);
      else if (args[i].equals("--zipf"))
        distribution = Task.Distribution.ZIPFIAN;
      else if (testTypeSet == false) {
        if (args[i].equals("read"))
          testType = Task.Type.READ;
        else if (args[i].equals("write"))
          testType = Task.Type.WRITE;
        else if (args[i].equals("scan"))
          testType = Task.Type.SCAN;
        else
          Usage.DumpAndExit(usage);
        testTypeSet = true;
      }
      else if (keyCount == -1) {
        keyCount = Long.parseLong(args[i]);
      }
      else if (valueSize == -1)
        valueSize = Integer.parseInt(args[i]);
      else
        Usage.DumpAndExit(usage);
      argString += args[i] + " ";
    }

    if (keyCount == -1 ||
        testType == Task.Type.WRITE && valueSize == -1)
      Usage.DumpAndExit(usage);

    if (submitExactly != -1 && submitAtMost != -1) {
      System.out.println("ERROR:  Only one of --submit-exactly and --submit-at-most may be supplied");
      System.exit(1);
    }

    if (submitAtMost != -1) {
      submitExactly = (submitAtMost > keyCount) ? keyCount : submitAtMost;
    }
    else if (submitExactly == -1)
      submitExactly = keyCount;

    try {
      if (driver.equals("hypertable")) {
        ThriftClient client = ThriftClient.create("localhost", 38080);
        if (!client.exists_table("perftest")) {
          System.out.println("Table 'perftest' does not exist, exiting...");
          System.exit(1);
        }
      }
      else if (driver.equals("hbase")) {
        HBaseAdmin admin = new HBaseAdmin( new HBaseConfiguration() );
        if (!admin.tableExists("perftest")) {
          System.out.println("Table 'perftest' does not exist, exiting...");
          System.exit(1);
        }
      }
    }
    catch (Exception e) {
      System.out.println("Problem creating table - " + e.getMessage());
      System.exit(-1);
    }

    try {

      ReactorFactory.Initialize((short)2);

      Vector<Task> taskVector = new Vector<Task>();
      LinkedList<Message> messageQueue = new LinkedList<Message>();
      RequestHandler requestHandler = new RequestHandler(timeout);
      HandlerFactory handlerFactory = new HandlerFactory(requestHandler);
      Map<String, Result> resultMap = new HashMap<String, Result>();
      MessageSetup messageSetup = new MessageSetup("perftest", driver, testType);
      Message message;
      Comm comm = new Comm(0);
      CommBuf cbuf;
      int connections, pendingResults;
      ProgressMeterVertical progress;
      int progressReadySkip;
      long startTime, stopTime;

      comm.Listen(port, handlerFactory, requestHandler);

      connections = requestHandler.waitForConnections(clients);

      if (clients > 0 && connections != clients) {
        log.info("Expected " + clients + " clients, but only got " + connections);
        System.exit(1);
      }

      if (connections == 0) {
        log.info("Timed out after waiting for connections, exiting...");
        System.exit(1);
      }

      pendingResults = connections;
      progressReadySkip = connections;

      long nranges = 10 * connections;
      long rangesize = keyCount / nranges;
      Task task;
      while (keysSubmitted < submitExactly) {
        for (long start=0,end=0; start<keyCount && keysSubmitted<submitExactly; start=end,end+=rangesize) {
          keysSubmitted += end-start;
          task = new Task(testType, keySize, valueSize, keyCount, order, distribution, start, end, scanBufferSize);
          taskVector.add( task );
        }
      }

      if (randomizeTasks) {
        Task tempTask;
        int randi;
        Random random = new Random(System.currentTimeMillis());
        for (int i=0; i<taskVector.size(); i++) {
          randi = random.nextInt(taskVector.size());
          tempTask = taskVector.get(randi);
          taskVector.set(randi, taskVector.get(0));
          taskVector.set(0, tempTask);
        }
      }

      for (Task t : taskVector) {
        messageQueue.offer( new MessageTask(t) );
      }

      System.out.println();
      progress = new ProgressMeterVertical( (long)messageQueue.size() );
      startTime = System.currentTimeMillis();

      while ((event = requestHandler.GetRequest()) != null) {

        message = MessageFactory.createMessage(event.payload);

        header = new CommHeader();
        header.initialize_from_request_header(event.header);

        if (message.type() == Message.Type.REGISTER) {
          MessageRegister messageRegister = (MessageRegister)message;
          cbuf = messageSetup.createCommBuf(header);
          int error = comm.SendResponse(event.addr, cbuf);
          if (error != Error.OK)
            throw new HypertableException(error);
          System.out.println("Client: " + messageRegister.getClientName());
          if (resultMap.containsKey(messageRegister.getClientName()))
            throw new HypertableException(Error.FAILED_EXPECTATION, "Client " + messageRegister.getHostName() + " already registered");
          resultMap.put(messageRegister.getClientName(), null);
        }
        else if (message.type() == Message.Type.READY) {

          if (progressReadySkip == 0)
            progress.add(1);
          else
            progressReadySkip--;

          if (!messageQueue.isEmpty()) {
            message = messageQueue.remove();
            cbuf = message.createCommBuf(header);
          }
          else
            cbuf = (new Message(Message.Type.FINISHED)).createCommBuf(header);

          int error = comm.SendResponse(event.addr, cbuf);
          if (error != Error.OK)
            throw new HypertableException(error);
        }
        else if (message.type() == Message.Type.SUMMARY) {
          //System.out.println("SUMMARY: " + message);
          Result result = resultMap.remove( ((MessageSummary)message).getClientName() );
          if (result != null)
            throw new HypertableException(Error.FAILED_EXPECTATION, "Received test results from " + ((MessageSummary)message).getClientName() + " twice");
          resultMap.put(((MessageSummary)message).getClientName(), ((MessageSummary)message).getResult());
          pendingResults--;
          if (pendingResults == 0)
            break;
          //System.out.println("Received " + (connections-pendingResults) + " results, " + pendingResults + " more to go");
        }
        else if (message.type() == Message.Type.ERROR) {
          System.out.println(event.addr + ": " + ((MessageError)message).getMessage());
          System.exit(1);
        }
        else
          System.out.println("UNKNOWN");
      }

      progress.finished();

      stopTime = System.currentTimeMillis();

      long itemCount = 0;
      long elapsedMillis = 0;
      long bytesReturned = 0;
      Result result;

      Iterator it = resultMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry pairs = (Map.Entry)it.next();
        //System.out.println(pairs.getKey() + " = " + pairs.getValue());
        result = (Result)pairs.getValue();
        if (testType == Task.Type.READ || testType == Task.Type.SCAN) {
          itemCount += result.itemsReturned;
          bytesReturned += result.valueBytesReturned;
        }
        else
          itemCount += result.itemsSubmitted;
        elapsedMillis += result.elapsedMillis;
      }

      ArrayList<String> summary = new ArrayList<String>();
      String summaryLine = null;

      System.out.println();
      summary.add("Test: " + testTypeString(order, testType));
      if (testType == Task.Type.READ)
        summary.add("Distribution: " + ((distribution==Task.Distribution.ZIPFIAN) ? "zipfian" : "uniform"));
      summary.add("Driver: " + driver);
      summary.add("Args: " + argString);
      summary.add("Key Count: " + keyCount);
      summary.add("Key Size: " + keySize);
      summary.add("Value size: " + valueSize);
      summary.add("Keys Submitted: " + keysSubmitted);
      if (testType == Task.Type.READ || testType == Task.Type.SCAN) {
        summary.add("Items Returned: " + itemCount);
        summary.add("Value Bytes Returned: " + bytesReturned);
      }
      summary.add("Clients: " + connections);
      summary.add("Start time: " + (new Date(startTime)).toString());
      summary.add("Finish time: " + (new Date(stopTime)).toString());
      summary.add("Wall time: " + ((stopTime - startTime) / 1000.0) + " s");
      summary.add("Test time: " + (elapsedMillis / 1000) + " s");

      if (testType == Task.Type.READ || testType == Task.Type.SCAN)
        bytesReturned += itemCount*keySize;  // add in key space
      else
        bytesReturned = itemCount*(keySize+valueSize);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      ps.format("Client Throughput: %.2f bytes/s", ((double)bytesReturned / (double)(elapsedMillis/1000)));
      summary.add(baos.toString());

      baos.reset();
      ps.format("Aggregate Throughput: %.2f bytes/s", ((double)bytesReturned / (double)(elapsedMillis/1000))*(double)connections);
      summary.add(baos.toString());

      if (testType == Task.Type.READ) {
        baos.reset();
        ps.format("Average Latency: %.3f milliseconds", (double)elapsedMillis/(double)itemCount);
        summary.add(baos.toString());
      }
      
      for (Iterator<String> i = summary.iterator( ); i.hasNext( ); ) {
        String s = i.next();
        System.out.println(s);
      }

      String summaryFileName = null;

      if (outputDir != null) {
        if (outputDir.endsWith("/"))
          outputDir = outputDir.substring(0, outputDir.length()-1);
        File out = new File(outputDir);
        if ( !out.isDirectory() ) {
          if (!out.mkdirs()) {
            System.out.println("Error - unable to create output directory '" + outputDir + "'");
            System.exit(-1);
          }
        }
        summaryFileName = outputDir + "/";
      }
      else
        summaryFileName = "";
      if (testName != null)
        summaryFileName += testName + "-";
      summaryFileName += driver + "-" + testTypeString(order, testType) + "-";

      if (testType == Task.Type.READ && order == Task.Order.RANDOM)
        summaryFileName += ((distribution==Task.Distribution.ZIPFIAN) ? "zipfian" : "uniform") + "-";
      summaryFileName += keyCount + "-" +  keySize + "-" + valueSize + "-" + clients + "clients.txt";

      BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFileName));
      for (Iterator<String> i = summary.iterator( ); i.hasNext( ); ) {
        String s = i.next();
        writer.write(s);
        writer.newLine();
      }
      writer.flush();
      writer.close();


      ReactorFactory.Shutdown();

    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
