/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

#include "Common/Compat.h"

#include <queue>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
using namespace std;

extern "C" {
#include <netdb.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
}

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"
#include "Common/Serialization.h"

#include "DispatchHandler.h"
#include "Comm.h"
#include "Event.h"
#include "HeaderBuilder.h"

using namespace Hypertable;
using namespace Serialization;

namespace {
  const char *DEFAULT_HOST = "localhost";
  const int DEFAULT_PORT = 11255;
  const int DEFAULT_TIMEOUT = 10;
  const char *usage[] = {
    "usage: sampleClient [OPTIONS] <input-file>",
    "",
    "OPTIONS:",
    "  --host=<name>   Specifies the host to connect to (default = localhost)",
    "  --port=<n>      Specifies the port to connect to (default = 11255)",
    "  --reactors=<n>  Specifies the number of reactors (default=1)",
    "  --recv-addr=<addr>  Let the server connect to us by listening for",
    "                  connection request on <addr> (host:port).  The address",
    "                  that the server is connecting from should be the same",
    "                  as what's specified with --host and --port or the defaults.",
    "                  (TCP only)",
    "  --timeout=<t>   Specifies the connection timeout in seconds (default=10)",
    "  --verbose       Generate verbose output",
    "  --udp           Operate in UDP mode instead of TCP",
    "",
    "This is a sample program to test the AsyncComm library.  It establishes",
    "a connection with the sampleServer and sends each line of the input file",
    "to the server.  Each reply from the server is echoed to stdout.",
    (const char *)0
  };
  bool g_verbose = false;
}



/**
 * (somewhat) Abstract base class for response handlers;  Defines
 * the message queue and the mutex and condition variable to
 * protect it.
 */
class ResponseHandler : public DispatchHandler {

public:

  ResponseHandler() : m_queue(), m_mutex(), m_cond() { return; }
  virtual ~ResponseHandler() { return; }

  virtual void handle(EventPtr &event_ptr) = 0;

  virtual bool get_response(EventPtr &event_ptr) = 0;

protected:
  queue<EventPtr>   m_queue;
  boost::mutex      m_mutex;
  boost::condition  m_cond;
};





/**
 * This is the dispatch handler that gets installed as the default
 * handler for the TCP connection.  It queues up responses
 * that can be fetched by the application via a call to GetResponse()
 * GetResponse() returns false when the connection is disconnected.
 */
class ResponseHandlerTCP : public ResponseHandler {

public:

  ResponseHandlerTCP() : ResponseHandler(), m_connected(false) { return; }

  virtual void handle(EventPtr &event_ptr) {
    boost::mutex::scoped_lock lock(m_mutex);
    if (event_ptr->type == Event::CONNECTION_ESTABLISHED) {
      if (g_verbose)
        HT_INFOF("Connection Established - %s", event_ptr->to_str().c_str());
      m_connected = true;
      m_cond.notify_one();
    }
    else if (event_ptr->type == Event::DISCONNECT) {
      if (event_ptr->error != 0) {
        HT_INFOF("Disconnect : %s", Error::get_text(event_ptr->error));
      }
      else {
        HT_INFO("Disconnect.");
      }
      m_connected = false;
      m_cond.notify_one();
    }
    else if (event_ptr->type == Event::ERROR) {
      HT_INFOF("Error : %s", Error::get_text(event_ptr->error));
      //exit(1);
    }
    else if (event_ptr->type == Event::MESSAGE) {
      m_queue.push(event_ptr);
      m_cond.notify_one();
    }
  }

  bool wait_for_connection() {
    boost::mutex::scoped_lock lock(m_mutex);
    if (m_connected)
      return true;
    m_cond.wait(lock);
    return m_connected;
  }

  virtual bool get_response(EventPtr &event_ptr) {
    boost::mutex::scoped_lock lock(m_mutex);
    while (m_queue.empty()) {
      if (m_connected == false)
        return false;
      m_cond.wait(lock);
    }
    event_ptr = m_queue.front();
    m_queue.pop();
    return true;
  }

private:
  bool m_connected;
};



/**
 * This handler factory gets passed into Comm::listen.  It
 * gets constructed with a pointer to a DispatchHandler.
 */
class HandlerFactory : public ConnectionHandlerFactory {
public:
  HandlerFactory(DispatchHandlerPtr &dhp) {
    m_dispatch_handler_ptr = dhp;
  }
  virtual void get_instance(DispatchHandlerPtr &dhp) {
    dhp = m_dispatch_handler_ptr;
  }
private:
  DispatchHandlerPtr m_dispatch_handler_ptr;
};




/**
 * This is the dispatch handler that gets installed as the default
 * handler for UDP mode.  It queues up the responses which can be
 * fetched by the application via a call to GetResponse()
 */
class ResponseHandlerUDP : public ResponseHandler {

public:

  ResponseHandlerUDP() : ResponseHandler() { return; }

  virtual void handle(EventPtr &event_ptr) {
    boost::mutex::scoped_lock lock(m_mutex);
    if (event_ptr->type == Event::MESSAGE) {
      m_queue.push(event_ptr);
      m_cond.notify_one();
    }
    else {
      HT_INFOF("%s", event_ptr->to_str().c_str());
      //exit(1);
    }
  }

  virtual bool get_response(EventPtr &event_ptr) {
    boost::mutex::scoped_lock lock(m_mutex);
    while (m_queue.empty()) {
      m_cond.wait(lock);
    }
    event_ptr = m_queue.front();
    m_queue.pop();
    return true;
  }
};





/**
 * main function
 */
int main(int argc, char **argv) {
  Comm *comm;
  int rval;
  const char *host = DEFAULT_HOST;
  struct sockaddr_in addr;
  uint16_t port = DEFAULT_PORT;
  time_t timeout = DEFAULT_TIMEOUT;
  int reactor_count = 1;
  const char *in_file = 0;
  HeaderBuilder hbuilder;
  int error;
  EventPtr event_ptr;
  ConnectionHandlerFactoryPtr chfp;
  DispatchHandlerPtr dhp;
  ResponseHandler *resp_handler;
  bool udp_mode = false;
  string line;
  int outstanding = 0;
  int max_outstanding = 50;
  const char *str;
  struct sockaddr_in local_addr;

  memset(&local_addr, 0, sizeof(local_addr));

  if (argc == 1)
    Usage::dump_and_exit(usage);

  System::initialize(argv[0]);
  ReactorFactory::initialize(1);

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--host=", 7))
      host = &argv[i][7];
    else if (!strncmp(argv[i], "--port=", 7)) {
      rval = atoi(&argv[i][7]);
      if (rval <= 1024 || rval > 65535) {
        cerr << "Invalid port.  Must be in the range of 1024-65535." << endl;
        exit(1);
      }
      port = rval;
    }
    else if (!strncmp(argv[i], "--timeout=", 10))
      timeout = (time_t)atoi(&argv[i][10]);
    else if (!strcmp(argv[i], "--udp"))
      udp_mode = true;
    else if (!strncmp(argv[i], "--reactors=", 11))
      reactor_count = atoi(&argv[i][11]);
    else if (!strncmp(argv[i], "--recv-addr=", 12)) {
      if (!InetAddr::initialize(&local_addr, &argv[i][12]))
        HT_ABORT;
    }
    else if (!strcmp(argv[i], "--verbose")) {
      g_verbose = true;
    }
    else if (in_file == 0)
      in_file = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (in_file == 0)
    Usage::dump_and_exit(usage);

  if (!InetAddr::initialize(&addr, host, port))
    exit(1);

  comm = Comm::instance();

  ifstream myfile(in_file);

  if (!myfile.is_open()) {
    HT_ERRORF("Unable to open file '%s' : %s", in_file, strerror(errno));
    return 0;
  }

  if (udp_mode) {
    assert(local_addr.sin_port == 0);
    resp_handler = new ResponseHandlerUDP();
    dhp = resp_handler;
    port++;
    InetAddr::initialize(&local_addr, INADDR_ANY, port);
    comm->create_datagram_receive_socket(&local_addr, dhp);
  }
  else {
    resp_handler = new ResponseHandlerTCP();
    dhp = resp_handler;

    if (local_addr.sin_port == 0) {
      if ((error = comm->connect(addr, dhp)) != Error::OK) {
        HT_ERRORF("Comm::connect error - %s", Error::get_text(error));
        exit(1);
      }
    }
    else {
      chfp = new HandlerFactory(dhp);
      comm->listen(local_addr, chfp, dhp);
    }
    if (!((ResponseHandlerTCP *)resp_handler)->wait_for_connection())
      exit(1);

  }

  while (!myfile.eof()) {
    getline (myfile,line);
    if (line.length() > 0) {
      CommBufPtr cbp(new CommBuf(hbuilder, encoded_length_str16(line)));
      cbp->append_str16(line);
      int retries = 0;

      if (udp_mode) {
        if ((error = comm->send_datagram(addr, local_addr, cbp)) != Error::OK) {
          HT_ERRORF("Problem sending datagram - %s", Error::get_text(error));
          return 1;
        }
      }
      else {
        while ((error = comm->send_request(addr, timeout, cbp, resp_handler)) != Error::OK) {
          if (error == Error::COMM_NOT_CONNECTED) {
            if (retries == 5) {
              HT_ERROR("Connection timeout.");
              return 1;
            }
            poll(0, 0, 1000);
            retries++;
          }
          else {
            HT_ERRORF("CommEngine::send_message returned '%s'", Error::get_text(error));
            return 1;
          }
        }
      }
      outstanding++;

      if (outstanding  > max_outstanding) {
        if (!resp_handler->get_response(event_ptr))
          break;
        try {
          str = decode_str16(&event_ptr->message, &event_ptr->message_len);
          if (*str != 0)
            cout << "ECHO: " << str << endl;
          else
            cout << "[NULL]" << endl;
        }
        catch (Exception &e) {
          cout <<"Error: "<< e << endl;
        }
        outstanding--;
      }
    }
  }

  while (outstanding > 0 && resp_handler->get_response(event_ptr)) {
    try {
      str = decode_str16(&event_ptr->message, &event_ptr->message_len);
      if (str != 0)
        cout << "ECHO: " << str << endl;
      else
        cout << "[NULL]" << endl;
    }
    catch (Exception &e) {
      cout <<"Error: "<< e << endl;
    }
    //cout << "out = " << outstanding << endl;
    outstanding--;
  }


  myfile.close();

  ReactorFactory::destroy();

  return 0;
}


