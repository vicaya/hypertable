/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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

#ifndef HYPERTABLE_METALOGDEFINITION_H
#define HYPERTABLE_METALOGDEFINITION_H

#include "Common/ReferenceCount.h"
#include "Common/String.h"

#include "MetaLogEntity.h"

namespace Hypertable {
  namespace MetaLog {
    class Definition : public ReferenceCount {
    public:
      Definition(const char* backup_label) : m_backup_label(backup_label) { }
      virtual uint16_t version() = 0;
      virtual const char *name() = 0;
      virtual const char *backup_label() { return m_backup_label.c_str(); }
      virtual Entity *create(const EntityHeader &header) = 0;

    private:
      String m_backup_label;
    };
    typedef intrusive_ptr<Definition> DefinitionPtr;
  }

}

#endif // HYPERTABLE_METALOGDEFINITION_H
