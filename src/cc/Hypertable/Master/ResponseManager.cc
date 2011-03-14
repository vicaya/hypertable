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

#include "Common/Compat.h"

#include "ResponseManager.h"

using namespace Hypertable;

void ResponseManager::operator()() {
  ResponseManagerContext::OperationExpirationTimeIndex &op_expiration_time_index = m_context->expirable_ops.get<1>();
  ResponseManagerContext::OperationExpirationTimeIndex::iterator op_iter;
  ResponseManagerContext::DeliveryExpirationTimeIndex &delivery_expiration_time_index = m_context->delivery_list.get<1>();
  ResponseManagerContext::DeliveryExpirationTimeIndex::iterator delivery_iter;
  HiResTime now, expire_time;
  bool timed_wait;
  bool shutdown = false;
  std::vector<OperationPtr> operations;
  std::vector<MetaLog::Entity *> entities;

  while (!shutdown) {

    {
      ScopedLock lock(m_context->mutex);

      now.reset();

      op_iter = op_expiration_time_index.begin();
      while (op_iter != op_expiration_time_index.end()) {
        if (op_iter->expiration_time() < now) {
          m_context->mml_writer->record_removal(op_iter->op.get());
          op_iter = op_expiration_time_index.erase(op_iter);
        }
        else
          break;
      }

      delivery_iter = delivery_expiration_time_index.begin();
      while (delivery_iter != delivery_expiration_time_index.end()) {
        if (delivery_iter->expiration_time < now)
          delivery_iter = delivery_expiration_time_index.erase(delivery_iter);
        else
          break;
      }

      timed_wait = false;
      if (op_iter != op_expiration_time_index.end()) {
        expire_time = op_iter->expiration_time();
        if (delivery_iter != delivery_expiration_time_index.end()) {
          if (delivery_iter->expiration_time < expire_time)
            expire_time = delivery_iter->expiration_time;
        }
        timed_wait = true;
      }
      else if (delivery_iter != delivery_expiration_time_index.end()) {
        expire_time = delivery_iter->expiration_time;
        timed_wait = true;
      }

      if (timed_wait) {
        boost::xtime xt = expire_time;
        m_context->cond.timed_wait(lock, xt);
      }
      else
        m_context->cond.wait(lock);

      shutdown = m_context->shutdown;

      operations.clear();
      entities.clear();
      if (!m_context->removal_queue.empty()) {
        for (std::list<OperationPtr>::iterator iter=m_context->removal_queue.begin();
             iter != m_context->removal_queue.end(); ++iter) {
          operations.push_back(*iter);
          entities.push_back(iter->get());
        }
        m_context->removal_queue.clear();
      }
    }

    if (!entities.empty())
      m_context->mml_writer->record_removal(entities);

  }
  
}


void ResponseManager::add_delivery_info(EventPtr &event) {
  ScopedLock lock(m_context->mutex);
  ResponseManagerContext::OperationIdentifierIndex &operation_identifier_index = m_context->expirable_ops.get<2>();
  ResponseManagerContext::OperationIdentifierIndex::iterator iter;
  ResponseManagerContext::DeliveryRec delivery_rec;
  const uint8_t *ptr = event->payload;
  size_t remain = event->payload_len;

  delivery_rec.id = Serialization::decode_i64(&ptr, &remain);

  if ((iter = operation_identifier_index.find(delivery_rec.id)) == operation_identifier_index.end()) {
    delivery_rec.event = event;
    delivery_rec.expiration_time.reset();
    delivery_rec.expiration_time += event->header.timeout_ms;
    m_context->delivery_list.push_back(delivery_rec);
  }
  else {
    int error = Error::OK;
    CommHeader header;
    header.initialize_from_request_header(event->header);
    CommBufPtr cbp(new CommBuf(header, iter->op->encoded_result_length()));
    iter->op->encode_result( cbp->get_data_ptr_address() );
    error = m_context->comm->send_response(event->addr, cbp);
    if (error != Error::OK)
      HT_ERRORF("Problem sending ID response back for %s operation (id=%lld) - %s",
                iter->op->label().c_str(), (Lld)iter->op->id(), Error::get_text(error));
    m_context->removal_queue.push_back(iter->op);
    operation_identifier_index.erase(iter);
    m_context->cond.notify_all();
  }
}


void ResponseManager::add_operation(OperationPtr &operation) {
  ScopedLock lock(m_context->mutex);
  ResponseManagerContext::DeliveryIdentifierIndex &delivery_identifier_index = m_context->delivery_list.get<2>();
  ResponseManagerContext::DeliveryIdentifierIndex::iterator iter;

  if (operation->remove_explicitly())
    m_context->explicit_removal_ops.push_back(ResponseManagerContext::OperationRec(operation));
  else if ((iter = delivery_identifier_index.find(operation->id())) == delivery_identifier_index.end())
    m_context->expirable_ops.push_back(ResponseManagerContext::OperationRec(operation));
  else {
    int error = Error::OK;
    CommHeader header;
    header.initialize_from_request_header(iter->event->header);
    CommBufPtr cbp(new CommBuf(header, operation->encoded_result_length()));
    operation->encode_result( cbp->get_data_ptr_address() );
    error = m_context->comm->send_response(iter->event->addr, cbp);
    if (error != Error::OK)
      HT_ERRORF("Problem sending ID response back for %s operation (id=%lld) - %s",
                operation->label().c_str(), (Lld)operation->id(), Error::get_text(error));
    m_context->removal_queue.push_back(operation);
    delivery_identifier_index.erase(iter);
    m_context->cond.notify_all();
  }
}


void ResponseManager::remove_operation(int64_t hash_code) {
  ScopedLock lock(m_context->mutex);
  ResponseManagerContext::OperationHashCodeIndex &hash_index = m_context->explicit_removal_ops.get<3>();
  ResponseManagerContext::OperationHashCodeIndex::iterator iter;
  HT_ASSERT((iter = hash_index.find(hash_code)) != hash_index.end());
  m_context->removal_queue.push_back(iter->op);
  hash_index.erase(iter);
  m_context->cond.notify_all();
}

bool ResponseManager::operation_complete(int64_t hash_code) {
  ScopedLock lock(m_context->mutex);
  ResponseManagerContext::OperationHashCodeIndex &hash_index = m_context->expirable_ops.get<3>();
  return hash_index.find(hash_code) != hash_index.end();
}

void ResponseManager::shutdown() {
  ScopedLock lock(m_context->mutex);
  m_context->shutdown = true;
  m_context->cond.notify_all();
}
