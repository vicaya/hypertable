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

#ifndef HYPERTABLE_MUTATORSCATTERBUFFER_H
#define HYPERTABLE_MUTATORSCATTERBUFFER_H

#include <vector>

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

#include "Common/atomic.h"
#include "Common/ByteString.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "Key.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "Schema.h"

namespace Hypertable {

  class MutatorScatterBuffer : public ReferenceCount {

  public:

    MutatorScatterBuffer(Comm *comm, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr);
    int set(Key &key, uint8_t *value, uint32_t value_len);
    int set(ByteString32T *key, ByteString32T *value);
    void send();
    bool completed();
    int wait_for_completion();
    MutatorScatterBuffer *create_redo_buffer();

  private:

    /**
     * 
     */
    class CompletionCounter {
    public:
      CompletionCounter() : m_outstanding(0), m_last_error(0), m_done(false) { }
      void set(size_t count) { 
	boost::mutex::scoped_lock lock(m_mutex);
	m_outstanding = count;
	m_done = (m_outstanding == 0) ? true : false;
	m_last_error = 0;
      }
      void decrement(int error) {
	boost::mutex::scoped_lock lock(m_mutex);
	assert(m_outstanding);
	m_outstanding--;
	if (error != Error::OK)
	  m_last_error = error;
	if (m_outstanding == 0) {
	  m_done = true;
	  m_cond.notify_all();
	}
      }
      int wait_for_completion() {
	boost::mutex::scoped_lock lock(m_mutex);
	while (m_outstanding)
	  m_cond.wait(lock);
	return m_last_error;
      }

      bool is_complete() { return m_done; }

    private:
      boost::mutex m_mutex;
      boost::condition m_cond;
      size_t m_outstanding;
      int    m_last_error;
      bool m_done;
    };

    friend class MutatorDispatchHandler;

    /**
     * 
     */
    class UpdateBuffer : public ReferenceCount {
    public:
      UpdateBuffer(CompletionCounter *cc) : buf(1024), sorted(false), error(0), counterp(cc) { return; }
      std::vector<uint64_t>  key_offsets;
      DynamicBuffer          buf;
      bool                   sorted;
      struct sockaddr_in     addr;
      int                    error;
      EventPtr               event_ptr;
      CompletionCounter     *counterp;
      DispatchHandlerPtr     dispatch_handler_ptr;
    };
    typedef boost::intrusive_ptr<UpdateBuffer> UpdateBufferPtr;

    typedef __gnu_cxx::hash_map<string, UpdateBufferPtr> UpdateBufferMapT;

    Comm                *m_comm;
    SchemaPtr            m_schema_ptr;
    RangeLocatorPtr      m_range_locator_ptr;
    RangeServerClient    m_range_server;
    std::string          m_table_name;
    TableIdentifierT     m_table_identifier;
    UpdateBufferMapT     m_buffer_map;
    CompletionCounter    m_completion_counter;
  };
  typedef boost::intrusive_ptr<MutatorScatterBuffer> MutatorScatterBufferPtr;


}

#endif // HYPERTABLE_MUTATORSCATTERBUFFER_H
