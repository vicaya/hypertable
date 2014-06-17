#include "Common/Compat.h"
#include "Common/Init.h"
#include "Common/Properties.h"
#include "Common/Logger.h"
#include "Common/Random.h"

#include <iostream>

#include "Hypertable/Lib/StatsRangeServer.h"

using namespace Hypertable;
using namespace std;


int main(int argc, char *argv[]) {
  Config::init(argc, argv);
  PropertiesPtr props = new Properties();
  String dirs = "/,/tmp";
  StatsRangeServerPtr stats1, stats2;
  char idbuf[32];

  Random::seed(1);

  props->set("Hypertable.RangeServer.Monitoring.DataDirectories", dirs);

  stats1 = new StatsRangeServer(props);
  stats1->set_location("rs1");

  stats1->timestamp = Random::number64();
  stats1->scan_count = Random::number64();
  stats1->scanned_cells = Random::number64();
  stats1->scanned_bytes = Random::number64();
  stats1->update_count = Random::number64();
  stats1->updated_cells = Random::number64();
  stats1->updated_bytes = Random::number64();
  stats1->sync_count = Random::number64();
  stats1->query_cache_max_memory = Random::number64();
  stats1->query_cache_available_memory = Random::number64();
  stats1->query_cache_accesses = Random::number64();
  stats1->query_cache_hits = Random::number64();
  stats1->block_cache_max_memory = Random::number64();
  stats1->block_cache_available_memory = Random::number64();
  stats1->block_cache_accesses = Random::number64();
  stats1->block_cache_hits = Random::number64();

  stats1->system.refresh();

  StatsTable table_stat;

  for (size_t i=0; i<50; i++) {
    table_stat.clear();
    sprintf(idbuf, "1/%d", (int)i+1);
    table_stat.table_id = idbuf;

    table_stat.scans = Random::number64();
    table_stat.cells_scanned = Random::number64();
    table_stat.cells_returned = Random::number64();
    table_stat.bytes_scanned = Random::number64();
    table_stat.bytes_returned = Random::number64();
    table_stat.cells_written = Random::number64();
    table_stat.bytes_written = Random::number64();
    table_stat.disk_used = Random::number64();
    table_stat.compression_ratio = Random::uniform01();
    table_stat.memory_used = Random::number64();
    table_stat.memory_allocated = Random::number64();
    table_stat.shadow_cache_memory = Random::number64();
    table_stat.block_index_memory = Random::number64();
    table_stat.bloom_filter_memory = Random::number64();
    table_stat.bloom_filter_accesses = Random::number64();
    table_stat.bloom_filter_maybes = Random::number64();

    table_stat.range_count = Random::number32() % 2000;

    stats1->tables.push_back(table_stat);
  }
  
  
  size_t len = stats1->encoded_length();
  
  uint8_t *buf = new uint8_t[ len ];
  uint8_t *ptr = buf;

  stats1->encode(&ptr);

  HT_ASSERT((size_t)(ptr-buf) == len);

  stats2 = new StatsRangeServer();

  const uint8_t *ptr2 = buf;
  stats2->decode(&ptr2, &len);

  HT_ASSERT(len == 0);

  HT_ASSERT(*stats1 == *stats2);

}
