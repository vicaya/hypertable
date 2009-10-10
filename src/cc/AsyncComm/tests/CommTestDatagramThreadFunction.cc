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
#include "AsyncComm/CommHeader.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

#include "Common/Serialization.h"

#include "CommTestDatagramThreadFunction.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

namespace {


  /**
   * This is the dispatch handler that gets installed as the default
   * dispatch handler for the receive port.  It queues up the responses
   * which can be fetched by the application via a call to GetResponse()
   */
  class ResponseHandler : public DispatchHandler {

  public:

    ResponseHandler() : m_queue(), m_mutex(), m_cond() { return; }

    virtual void handle(EventPtr &event_ptr) {
      ScopedLock lock(m_mutex);
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
      ScopedLock lock(m_mutex);
      while (m_queue.empty()) {
        m_cond.wait(lock);
      }
      event_ptr = m_queue.front();
      m_queue.pop();
      return true;
    }

  private:
    std::queue<EventPtr>   m_queue;
    Mutex             m_mutex;
    boost::condition  m_cond;
  };

}


/**
 *
 */
void CommTestDatagramThreadFunction::operator()() {
  CommHeader header;
  int error;
  EventPtr event_ptr;
  int outstanding = 0;
  int max_outstanding = 50;
  string line;
  ifstream infile(m_input_file);
  ofstream outfile(m_output_file);
  const char *str;
  int nsent = 0;
  struct sockaddr_in local_addr;
  ResponseHandler *resp_handler = new ResponseHandler();
  DispatchHandlerPtr dhp(resp_handler);
  const uint8_t *decode_ptr;
  size_t decode_remain;

  header.gid = rand();

  InetAddr::initialize(&local_addr, INADDR_ANY, m_port);

  try { m_comm->create_datagram_receive_socket(&local_addr, 0, dhp); }
  catch (Exception &e) {
    HT_ERROR_OUT <<"create datagram receiving port "<< e << HT_END;
    return;
  }

  if (infile.is_open()) {
    while (!infile.eof() && nsent < MAX_MESSAGES) {
      getline (infile,line);
      if (infile.fail())
        break;

      CommBufPtr cbp(new CommBuf(header, encoded_length_str16(line)));
      cbp->append_str16(line);
      if ((error = m_comm->send_datagram(m_addr, local_addr, cbp))
          != Error::OK) {
        HT_ERRORF("Problem sending datagram - %s", Error::get_text(error));
        return;
      }
      outstanding++;

      if (outstanding  > max_outstanding) {
        if (!resp_handler->get_response(event_ptr))
          break;
        try {
          decode_ptr = event_ptr->payload;
          decode_remain = event_ptr->payload_len;
          str = decode_str16(&decode_ptr, &decode_remain);

          if (*str != 0)
            outfile << str << endl;
          else
            outfile << endl;
        }
        catch (Exception &e) {
          outfile <<"Error: "<< e << endl;
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

  while (outstanding > 0 && resp_handler->get_response(event_ptr)) {
    try {
      decode_ptr = event_ptr->payload;
      decode_remain = event_ptr->payload_len;
      str = decode_str16(&decode_ptr, &decode_remain);
      if (*str != 0)
        outfile << str << endl;
      else
        outfile << endl;
    }
    catch (Exception &e) {
      outfile <<"Error: "<< e << endl;
    }
    //cout << "out = " << outstanding << endl;
    outstanding--;
  }
}
