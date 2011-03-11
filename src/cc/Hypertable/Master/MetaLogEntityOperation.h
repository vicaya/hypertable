/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_METALOGENTITYOPERATION_H
#define HYPERTABLE_METALOGENTITYOPERATION_H

#include "Common/Mutex.h"
#include "Hypertable/Lib/MetaLogEntity.h"

namespace Hypertable {
  namespace MetaLog {

    class EntityOperation : public Entity {
    public:
      EntityOperation(int32_t type) : Entity(type) { }
      EntityOperation(const EntityHeader &header_) : Entity(header_) { }
      virtual ~EntityOperation() { }
      virtual size_t encoded_length() const = 0;
      virtual void encode(uint8_t **bufp) const = 0;
      virtual void decode(const uint8_t **bufp, size_t *remainp) = 0;
      virtual const String name() = 0;
      virtual void display(std::ostream &os) = 0;

      virtual void server_dependencies(std::vector<String> &servers) {
        ScopedLock lock(m_mutex);
        servers = m_dependent_servers;
      }
      virtual void table_dependencies(std::vector<String> &tables) {
        ScopedLock lock(m_mutex);
        tables = m_dependent_tables;
      }

    protected:
      virtual size_t encoded_dependencies_length() const;
      virtual void encode_dependencies(uint8_t **bufp) const;
      virtual void decode_dependencies(const uint8_t **bufp, size_t *remainp);
      virtual void display_dependencies(std::ostream &os);

      Mutex m_mutex;
      std::vector<String> m_dependent_servers;
      std::vector<String> m_dependent_tables;
    };
    typedef intrusive_ptr<EntityOperation> EntityOperationPtr;

    namespace EntityType {
      enum {
        MASTER_OPERATION_CREATE_TABLE = 0x00020001
      };
    }
    

  } // namespace MetaLog
} // namespace Hypertable

#endif // HYPERTABLE_METALOGENTITYOPERATION_H
