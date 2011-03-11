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
#include <vector>
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"

#include "Hyperspace/Session.h"

#include "NamespaceListing.h"

namespace Hyperspace {
  class Session;
}

namespace Hypertable {

  /** Easy mapping between a Table/Namespace name string to ids and vice versa.
   */
  class NameIdMapper: public ReferenceCount {

  public:

    enum Flag { IS_NAMESPACE=0x0001, CREATE_INTERMEDIATE=0x0002 };

    NameIdMapper(Hyperspace::SessionPtr &hyperspace, const String &toplevel_dir);
    /**
     * @param name name of the table/namespace
     * @param id the returned id of the table/namespace specified by name
     * @param is_namespace returns true if name corresponds to a namespace
     * @return true if mapping exists
     */
    bool name_to_id(const String &name, String &id, bool *is_namespacep=0);

    /**
     * @param id the id of the table/namespace
     * @param name returned name of the table/namespace specified by id
     * @param is_namespace returns true if name corresponds to a namespace
     * @return true if mapping exists
     */
    bool id_to_name(const String &id, String &name, bool *is_namespacep=0);

    /**
     * @param id the id of the namespace
     * @param include_sub_entries include or not include all sub entries
     * @param listing returned names of the table/namespaces contained within the namespace
     *        specified by id
     */
    void id_to_sublisting(const String &id, bool include_sub_entries, std::vector<NamespaceListing> &listing);

    /**
     * @param name name to map
     * @param id output parameter to hold newly mapped ID
     * @param flags control falgs (IS_NAMESPACE and/or CREATE_INTERMEDIATE)
     */
    void add_mapping(const String &name, String &id, int flags=0, bool ignore_exists=false);

    /**
     * @param name name to map
     */
    void drop_mapping(const String &name);

    /**
     * @param name name to check for mapping
     * @param is_namespace if mapping exists set to true if is namespace
     * @return true if mapping exists, false otherwise
     */
    bool exists_mapping(const String &name, bool *is_namespace);

    /**
     * Rename one entity, it doesn't recursively rename all entities under the path
     * specified by old_name
     *
     * @param old_name old name
     * @param new_name new name
     */
    void rename(const String &old_name, const String &new_name);

    void add_entry(const String &names_parent, const String &names_entry,
                   std::vector<uint64_t> &ids, bool is_namespace);

  protected:
    bool do_mapping(const String &input, bool id_in, String &output, bool *is_namespacep);
    static void get_namespace_listing(const std::vector<Hyperspace::DirEntryAttr> &dir_listing, std::vector<NamespaceListing> &listing);

    Mutex m_mutex;
    Hyperspace::SessionPtr m_hyperspace;
    String m_toplevel_dir;
    String m_names_dir;
    String m_ids_dir;
    size_t m_prefix_components;
  };

  typedef intrusive_ptr<NameIdMapper> NameIdMapperPtr;

} // namesapce Hypertable

#endif // HYPERTABLE_NAMEIDMAPPER_H
