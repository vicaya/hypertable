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

#ifndef HYPERTABLE_DFSBROKER_CLIENT_H
#define HYPERTABLE_DFSBROKER_CLIENT_H

#include <ext/hash_map>

#include <boost/thread/mutex.hpp>

#include "Common/Properties.h"
#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/ConnectionManager.h"

#include "Hypertable/Lib/Filesystem.h"

#include "ClientBufferedReaderHandler.h"

/**
 * Forward declarations
 */
namespace Hypertable {
  class DispatchHandler;
  class Comm;
  class CommBuf;
  class MessageBuilderSimple;
}
namespace boost {
  class thread;
}

#include "Protocol.h"

using namespace Hypertable;

namespace Hypertable {

  namespace DfsBroker {

    /** Proxy class for DFS broker.  As specified in the general contract for
     * a Filesystem, commands that operate on the same file descriptor
     * are serialized by the underlying filesystem.  In other words, if you issue
     * three asynchronous commands, they will get carried out and their responses
     * will come back in the same order in which they were issued.
     */
    class Client : public Filesystem {
    public:

      virtual ~Client() { return; }

      /** Constructor with explicit values.  Connects to the DFS broker at the
       * address given by the addr argument and uses the timeout argument for
       * the request timeout values.
       *
       * @param conn_manager_ptr smart pointer to connection manager
       * @param addr address of DFS broker to connect to
       * @param timeout timeout value to use in requests
       */
      Client(ConnectionManagerPtr &conn_manager_ptr, struct sockaddr_in &addr, time_t timeout);

      /** Constructor with Properties object.  The following properties are read
       * to determine the location of the broker and the request timeout value:
       * <pre>
       * DfsBroker.port
       * DfsBroker.host
       * DfsBroker.timeout
       * </pre>
       *
       * @param conn_manager_ptr smart pointer to connection manager
       * @param props_ptr smart pointer to properties object
       */
      Client(ConnectionManagerPtr &conn_manager_ptr, PropertiesPtr &props_ptr);

      /** Constructor without connection manager.
       *
       * @param comm pointer to the Comm object
       * @param addr remote address of already connected DfsBroker
       * @param timeout timeout value to use in requests
       */
      Client(Comm *comm, struct sockaddr_in &addr, time_t timeout);

      /** Waits up to max_wait_secs for a connection to be established with the DFS
       * broker.
       *
       * @param max_wait_secs maximum amount of time to wait
       * @return true if connected, false otherwise
       */
      bool wait_for_connection(long max_wait_secs) {
	if (m_conn_manager_ptr)
	  return m_conn_manager_ptr->wait_for_connection(m_addr, max_wait_secs);
	return true;
      }

      virtual int open(std::string &name, DispatchHandler *handler);
      virtual int open(std::string &name, int32_t *fdp);
      virtual int open_buffered(std::string &name, uint32_t buf_size, uint32_t outstanding, int32_t *fdp, uint64_t start_offset=0, uint64_t end_offset=0);

      virtual int create(std::string &name, bool overwrite, int32_t bufferSize,
			 int32_t replication, int64_t blockSize, DispatchHandler *handler);
      virtual int create(std::string &name, bool overwrite, int32_t bufferSize,
			 int32_t replication, int64_t blockSize, int32_t *fdp);

      virtual int close(int32_t fd, DispatchHandler *handler);
      virtual int close(int32_t fd);

      virtual int read(int32_t fd, uint32_t amount, DispatchHandler *handler);
      virtual int read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

      virtual int append(int32_t fd, const void *buf, uint32_t amount, DispatchHandler *handler);
      virtual int append(int32_t fd, const void *buf, uint32_t amount);

      virtual int seek(int32_t fd, uint64_t offset, DispatchHandler *handler);
      virtual int seek(int32_t fd, uint64_t offset);

      virtual int remove(std::string &name, DispatchHandler *handler);
      virtual int remove(std::string &name);

      virtual int length(std::string &name, DispatchHandler *handler);
      virtual int length(std::string &name, int64_t *lenp);

      virtual int pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler);
      virtual int pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

      virtual int mkdirs(std::string &name, DispatchHandler *handler);
      virtual int mkdirs(std::string &name);

      virtual int flush(int32_t fd, DispatchHandler *handler);
      virtual int flush(int32_t fd);

      virtual int rmdir(std::string &name, DispatchHandler *handler);
      virtual int rmdir(std::string &name);

      virtual int readdir(std::string &name, DispatchHandler *handler);
      virtual int readdir(std::string &name, std::vector<std::string> &listing);

      /** Checks the status of the DFS broker.  Issues a status command and waits
       * for it to return.
       *
       * @return Error::OK if broker is up and OK, otherwise an error code
       */
      int status();

      /** Shuts down the DFS broker.  Issues a shutdown command to the DFS broker.
       * If the flag is set to Protocol::SHUTDOWN_FLAG_IMMEDIATE, then the 
       * broker will call exit(0) directly from the I/O reactor thread.  Otherwise,
       * a shutdown command will get added to the broker's application queue, allowing
       * the shutdown to be handled more gracefully.
       *
       * @param flags controls how broker gets shut down
       * @param handler response handler
       * @return Error::OK if broker is up and OK, otherwise an error code
       */
      int shutdown(uint16_t flags, DispatchHandler *handler);

      Protocol *get_protocol_object() { return &m_protocol; }

      /** Gets the configured request timeout value.
       *
       * @return timeout value in seconds
       */
      time_t get_timeout() { return m_timeout; }
    
    private:

      /** Sends a message to the DFS broker.
       *
       * @param cbufPtr message to send
       * @param handler response handler
       * @return Error::OK on success or error code on failure
       */
      int send_message(CommBufPtr &cbufPtr, DispatchHandler *handler);

      typedef __gnu_cxx::hash_map<uint32_t, ClientBufferedReaderHandler *> BufferedReaderMapT;

      boost::mutex          m_mutex;
      Comm                 *m_comm;
      ConnectionManagerPtr  m_conn_manager_ptr;
      struct sockaddr_in    m_addr;
      time_t                m_timeout;
      MessageBuilderSimple *m_message_builder;
      Protocol              m_protocol;
      BufferedReaderMapT    m_buffered_reader_map;
    };

  }

}


#endif // HYPERTABLE_DFSBROKER_CLIENT_H

