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
  Random rng;
  char idbuf[32];

  rng.seed(1);

  props->set("Hypertable.RangeServer.Monitoring.DataDirectories", dirs);

  stats1 = new StatsRangeServer(props);
  stats1->set_location("rs1");

  stats1->timestamp = rng.number64();
  stats1->scan_count = rng.number64();
  stats1->scanned_cells = rng.number64();
  stats1->scanned_bytes = rng.number64();
  stats1->update_count = rng.number64();
  stats1->updated_cells = rng.number64();
  stats1->updated_bytes = rng.number64();
  stats1->sync_count = rng.number64();
  stats1->query_cache_max_memory = rng.number64();
  stats1->query_cache_available_memory = rng.number64();
  stats1->query_cache_accesses = rng.number64();
  stats1->query_cache_hits = rng.number64();
  stats1->block_cache_max_memory = rng.number64();
  stats1->block_cache_available_memory = rng.number64();
  stats1->block_cache_accesses = rng.number64();
  stats1->block_cache_hits = rng.number64();

  stats1->system.refresh();

  StatsTable table_stat;

  for (size_t i=0; i<50; i++) {
    table_stat.clear();
    sprintf(idbuf, "1/%d", (int)i+1);
    table_stat.table_id = idbuf;

    table_stat.scans = rng.number64();
    table_stat.cells_read = rng.number64();
    table_stat.bytes_read = rng.number64();
    table_stat.cells_written = rng.number64();
    table_stat.bytes_written = rng.number64();
    table_stat.disk_used = rng.number64();
    table_stat.memory_used = rng.number64();
    table_stat.memory_allocated = rng.number64();
    table_stat.shadow_cache_memory = rng.number64();
    table_stat.block_index_memory = rng.number64();
    table_stat.bloom_filter_memory = rng.number64();
    table_stat.bloom_filter_accesses = rng.number64();
    table_stat.bloom_filter_maybes = rng.number64();

    table_stat.range_count = rng.number32() % 2000;

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
