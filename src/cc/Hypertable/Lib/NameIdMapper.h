/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

#ifndef HYPERTABLE_NAMEIDMAPPER_H
#define HYPERTABLE_NAMEIDMAPPER_H

#include "Common/Compat.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"

#include "Hyperspace/Session.h"

namespace Hyperspace {
  class Session;
}

namespace Hypertable {

  /** Easy mapping between a Table/Namespace name string to ids and vice versa.
   */
  class NameIdMapper: public ReferenceCount {

  public:
    NameIdMapper(Hyperspace::SessionPtr &hyperspace);
    /**
     * @param name name of the table/namespace
     * @param id the returned id of the table/namespace specified by name
     * @return true if mapping exists
     */
    bool name_to_id(const String &name, String &id);

    /**
     * @param id the id of the table/namespace
     * @param name returned name of the table/namespace specified by id
     * @return true if mapping exists
     */
    bool id_to_name(const String &id, String &name);

  protected:
    bool do_mapping(const String &input, bool id_in, String &output);

    Hyperspace::SessionPtr m_hyperspace;
    static const String HS_DIR;
    static const String HS_NAMEMAP_DIR;
    static const String HS_NAMEMAP_NAMES_DIR;
    static const String HS_NAMEMAP_IDS_DIR;
    static const int HS_NAMEMAP_COMPONENTS;
  };

  typedef intrusive_ptr<NameIdMapper> NameIdMapperPtr;

} // namesapce Hypertable

#endif // HYPERTABLE_NAMEIDMAPPER_H
