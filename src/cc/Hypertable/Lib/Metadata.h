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

#ifndef HYPERTABLE_METADATA_H
#define HYPERTABLE_METADATA_H

#include <map>
#include <string>

#include <expat.h>

#include "Common/ByteString.h"
#include "Common/StringExt.h"

#include "Hypertable/RangeServer/RangeInfo.h"

using namespace hypertable;

namespace hypertable {

  typedef std::map<std::string, RangeInfoPtr> RangeInfoMapT;

  typedef std::map<std::string, RangeInfoMapT> TableMapT;

  class Metadata {

  public:

    Metadata(const char *fname=0);

    int get_range_info(std::string &tableName, std::string &endRow, RangeInfoPtr &rangeInfoPtr);

    int get_range_info(const char *tableName, const char *endRow, RangeInfoPtr &rangeInfoPtr) {
      std::string tableNameStr = tableName;
      std::string endRowStr    = endRow;
      return get_range_info(tableNameStr, endRowStr, rangeInfoPtr);
    }

    void add_range_info(RangeInfoPtr &rangePtr);

    void sync(const char *fname=0);
    void display();

    static Metadata *new_instance(const char *fname);
    friend Metadata *new_instance(const char *fname);

    TableMapT     m_table_map;

  private:

    std::string   m_filename;

    static Metadata     *ms_metadata;
    static RangeInfo    *ms_range;
    static std::string   ms_collected_text;

    static void start_element_handler(void *userData, const XML_Char *name, const XML_Char **atts);
    static void end_element_handler(void *userData, const XML_Char *name);
    static void character_data_handler(void *userData, const XML_Char *s, int len);

  };

}

#endif // HYPERTABLE_METADATA_H
