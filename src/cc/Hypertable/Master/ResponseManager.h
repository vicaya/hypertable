/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_RESPONSEMANAGER_H
#define HYPERTABLE_RESPONSEMANAGER_H

#include <list>
#include <map>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/properties.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/thread/condition.hpp>

#include "AsyncComm/Comm.h"

#include "Common/Mutex.h"
#include "Common/Thread.h"
#include "Common/Time.h"

#include "Hypertable/Lib/MetaLogWriter.h"

#include "Context.h"
#include "Operation.h"

namespace Hypertable {

  using namespace boost::multi_index;

  class ResponseManagerContext {
  public:

    ResponseManagerContext(MetaLog::WriterPtr &mmlw) : mml_writer(mmlw), shutdown(false) {
      comm = Comm::instance();
    }
    
    class OperationRec {
    public:
      OperationRec(OperationPtr &_op) : op(_op) { }
      OperationPtr op;
      HiResTime expiration_time() const { return op->expiration_time(); }
      int64_t id() const { return op->id(); }
      int64_t hash_code() const { return op->hash_code(); }
    };

    typedef boost::multi_index_container<
      OperationRec,
      indexed_by<
      sequenced<>,
      ordered_non_unique<const_mem_fun<OperationRec, HiResTime,
                                       &OperationRec::expiration_time> >,
      hashed_unique<const_mem_fun<OperationRec, int64_t,
                                  &OperationRec::id> >,
      hashed_unique<const_mem_fun<OperationRec, int64_t,
                                  &OperationRec::hash_code> > 
    >
    > OperationList;

    typedef OperationList::nth_index<0>::type OperationSequence;
    typedef OperationList::nth_index<1>::type OperationExpirationTimeIndex;
    typedef OperationList::nth_index<2>::type OperationIdentifierIndex;
    typedef OperationList::nth_index<3>::type OperationHashCodeIndex;

    class DeliveryRec {
    public:
      HiResTime expiration_time;
      int64_t id;
      EventPtr event;
    };

    typedef boost::multi_index_container<
      DeliveryRec,
      indexed_by<
      sequenced<>,
      ordered_non_unique<member<DeliveryRec, HiResTime,
                                &DeliveryRec::expiration_time> >,
      hashed_unique<member<DeliveryRec, int64_t,
                           &DeliveryRec::id> > 
    >
    > DeliveryList;

    typedef DeliveryList::nth_index<0>::type DeliverySequence;
    typedef DeliveryList::nth_index<1>::type DeliveryExpirationTimeIndex;
    typedef DeliveryList::nth_index<2>::type DeliveryIdentifierIndex;

    Mutex mutex;
    boost::condition cond;
    Comm *comm;
    MetaLog::WriterPtr mml_writer;
    bool shutdown;
    OperationList expirable_ops;
    OperationList explicit_removal_ops;
    DeliveryList delivery_list;
    std::list<OperationPtr> removal_queue;
  };

  /**
   */
  class ResponseManager {
  public:
    ResponseManager(ResponseManagerContext *context) : m_context(context) { }
    void operator()();
    void add_operation(OperationPtr &operation);
    void remove_operation(int64_t hash_code);
    bool operation_complete(int64_t hash_code);
    void add_delivery_info(EventPtr &event);
    void shutdown();

  private:
    ResponseManagerContext *m_context;
  };


} // namespace Hypertable

#endif // HYPERTABLE_RESPONSEMANAGER_H
