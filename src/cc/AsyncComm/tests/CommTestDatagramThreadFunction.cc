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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <iostream>
#include <fstream>
#include <queue>

extern "C" {
#include <poll.h>
}

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/Error.h"
#include "Common/InetAddr.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/Serialization.h"

#include "CommTestDatagramThreadFunction.h"

using namespace Hypertable;
using namespace std;

namespace {


  /**
   * This is the dispatch handler that gets installed as the default
   * dispatch handler for the receive port.  It queues up the responses
   * which can be fetched by the application via a call to GetResponse()
   */
  class ResponseHandler : public DispatchHandler {

  public:

    ResponseHandler() : m_queue(), m_mutex(), m_cond() { return; }
  
    virtual void handle(EventPtr &eventPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      if (eventPtr->type == Event::MESSAGE) {
	m_queue.push(eventPtr);
	m_cond.notify_one();
      }
      else {
	HT_INFOF("%s", eventPtr->toString().c_str());
	//exit(1);
      }
    }

    virtual bool get_response(EventPtr &eventPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      while (m_queue.empty()) {
	m_cond.wait(lock);
      }
      eventPtr = m_queue.front();
      m_queue.pop();
      return true;
    }

  private:
    queue<EventPtr>   m_queue;
    boost::mutex      m_mutex;
    boost::condition  m_cond;
  };

}


/**
 *
 */
void CommTestDatagramThreadFunction::operator()() {
  HeaderBuilder hbuilder(Header::PROTOCOL_NONE, rand());
  int error;
  EventPtr eventPtr;
  int outstanding = 0;
  int maxOutstanding = 50;
  string line;
  ifstream infile(m_input_file);
  ofstream outfile(m_output_file);
  const char *str;
  int nsent = 0;
  struct sockaddr_in localAddr;
  ResponseHandler *respHandler = new ResponseHandler();
  DispatchHandlerPtr dispatchHandlerPtr(respHandler);

  InetAddr::initialize(&localAddr, INADDR_ANY, m_port);

  if ((error = m_comm->create_datagram_receive_socket(&localAddr, dispatchHandlerPtr)) != Error::OK) {
    HT_ERRORF("Problem opening datagram receive port %d - %s", m_port, Error::get_text(error));
    return;
  }

  if (infile.is_open()) {
    while (!infile.eof() && nsent < MAX_MESSAGES) {
      getline (infile,line);
      if (infile.fail())
	break;

      CommBufPtr cbufPtr( new CommBuf(hbuilder, Serialization::encoded_length_string(line)) );
      cbufPtr->append_string(line);
      if ((error = m_comm->send_datagram(m_addr, localAddr, cbufPtr)) != Error::OK) {
	HT_ERRORF("Problem sending datagram - %s", Error::get_text(error));
	return;
      }
      outstanding++;

      if (outstanding  > maxOutstanding) {
	if (!respHandler->get_response(eventPtr))
	  break;
	if (!Serialization::decode_string(&eventPtr->message, &eventPtr->messageLen, &str))
	  outfile << "ERROR: deserialization problem." << endl;
	else {
	  if (*str != 0)
	    outfile << str << endl;
	  else
	    outfile << endl;
	}
	outstanding--;
      }
      nsent++;
    }

    infile.close();
  }
  else {
    HT_ERRORF("Unable to open file '%s' : %s", m_input_file, strerror(errno));
    return;
  }

  while (outstanding > 0 && respHandler->get_response(eventPtr)) {
    if (!Serialization::decode_string(&eventPtr->message, &eventPtr->messageLen, &str))
      outfile << "ERROR: deserialization problem." << endl;
    else {
      if (*str != 0)
	outfile << str << endl;
      else
	outfile << endl;
    }
    //cout << "out = " << outstanding << endl;
    outstanding--;
  }
}
