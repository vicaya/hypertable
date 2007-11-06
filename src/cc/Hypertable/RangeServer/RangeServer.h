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

#ifndef HYPERTABLE_RANGESERVER_H
#define HYPERTABLE_RANGESERVER_H

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/Types.h"

#include "ResponseCallbackCreateScanner.h"
#include "ResponseCallbackFetchScanblock.h"
#include "ResponseCallbackUpdate.h"
#include "TableInfo.h"

using namespace hypertable;

namespace hypertable {

  class ConnectionHandler;

  class RangeServer : public ReferenceCount {
  public:
    RangeServer(Comm *comm, PropertiesPtr &propsPtr);
    virtual ~RangeServer();

    void compact(ResponseCallback *cb, RangeSpecificationT *rangeSpec, uint8_t compactionType);
    void create_scanner(ResponseCallbackCreateScanner *cb, RangeSpecificationT *rangeSpec, ScanSpecificationT *spec);
    void fetch_scanblock(ResponseCallbackFetchScanblock *cb, uint32_t scannerId);
    void load_range(ResponseCallback *cb, RangeSpecificationT *rangeSpec);
    void update(ResponseCallbackUpdate *cb, const char *tableName, uint32_t generation, BufferT &buffer);

    std::string &server_id_str() { return m_server_id_str; }

    void master_change();

  private:
    int directory_initialize(Properties *props);

    bool get_table_info(std::string &name, TableInfoPtr &info);
    bool get_table_info(const char *name, TableInfoPtr &info) {
      std::string nameStr = name;
      return get_table_info(nameStr, info);
    }

    void set_table_info(std::string &name, TableInfoPtr &info);
    void set_table_info(const char *name, TableInfoPtr &info) {
      std::string nameStr = name;
      set_table_info(nameStr, info);
    }

    int verify_schema(TableInfoPtr &tableInfoPtr, int generation, std::string &errMsg);

    typedef __gnu_cxx::hash_map<string, TableInfoPtr> TableInfoMapT;

    boost::mutex           m_mutex;
    bool                   m_verbose;
    Comm                  *m_comm;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    TableInfoMapT          m_table_info_map;
    ApplicationQueuePtr    m_app_queue_ptr;
    ConnectionManagerPtr   m_conn_manager_ptr;
    uint64_t               m_existence_file_handle;
    struct LockSequencerT  m_existence_file_sequencer;
    std::string            m_server_id_str;
    ConnectionHandler     *m_master_connection_handler;
    MasterClientPtr        m_master_client_ptr;
  };
  typedef boost::intrusive_ptr<RangeServer> RangeServerPtr;
  

}

#endif // HYPERTABLE_RANGESERVER_H
