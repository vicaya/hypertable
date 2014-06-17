/**
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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

#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <cmath>

extern "C" {
#include <poll.h>
#include <stdio.h>
#include <time.h>
}

#include <boost/algorithm/string.hpp>
#include <boost/progress.hpp>
#include <boost/shared_array.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/xtime.hpp>
#include <boost/timer.hpp>

#include "Common/Mutex.h"
#include "Common/Stopwatch.h"
#include "Common/String.h"
#include "Common/Sweetener.h"
#include "Common/Init.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/DataGenerator.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Cells.h"

#include "LoadClient.h"
#include "LoadThread.h"
#include "ParallelLoad.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;
using namespace boost;

namespace {

  const char *usage =
    "\n"
    "Usage: ht_generate_load [options] <type>\n\n"
    "Description:\n"
    "  This program is used to generate load on a Hypertable\n"
    "  cluster.  The <type> argument indicates the type of load\n"
    "  to generate ('query' or 'update').\n\n"
    "Options";

  struct AppPolicy : Config::Policy {
    static void init_options() {
      allow_unregistered_options(true);
      cmdline_desc(usage).add_options()
        ("help,h", "Show this help message and exit")
        ("help-config", "Show help message for config properties")
        ("table", str()->default_value("LoadTest"), "Name of table to query/update")
        ("delete-percentage", i32(),
         "When generating update workload make this percentage deletes")
        ("max-bytes", i64(), "Amount of data to generate, measured by number "
         "of key and value bytes produced")
        ("max-keys", i64(), "Maximum number of keys to generate for query load")
        ("parallel", i32()->default_value(0),
         "Spawn threads to execute requests in parallel")
        ("query-delay", i32(), "Delay milliseconds between each query")
        ("sample-file", str(),
         "Output file to hold request latencies, one per line")
        ("seed", i32()->default_value(1), "Pseudo-random number generator seed")
        ("row-seed", i32()->default_value(1), "Pseudo-random number generator seed")
        ("spec-file", str(),
         "File containing the DataGenerator specificaiton")
        ("stdout", boo()->zero_tokens()->default_value(false),
         "Display generated data to stdout instead of sending load to cluster")
        ("verbose,v", boo()->zero_tokens()->default_value(false),
         "Show more verbose output")
        ("flush", boo()->zero_tokens()->default_value(false), "Flush after each update")
        ("no-log-sync", boo()->zero_tokens()->default_value(false), "Don't sync rangeserver commit logs on autoflush")
        ("flush-interval", i64()->default_value(0),
         "Amount of data after which to mutator buffers are flushed "
         "and commit log is synced. Only used if no-log-sync flag is on")
        ("thrift", boo()->zero_tokens()->default_value(false),
         "Generate load via Thrift interface instead of C++ client library")
        ("version", "Show version information and exit")
        ;
      alias("delete-percentage", "DataGenerator.DeletePercentage");
      alias("max-bytes", "DataGenerator.MaxBytes");
      alias("max-keys", "DataGenerator.MaxKeys");
      alias("seed", "DataGenerator.Seed");
      alias("row-seed", "rowkey.seed");
      cmdline_hidden_desc().add_options()
        ("type", str(), "Type (update or query).");
      cmdline_positional_desc().add("type", 1);
    }
  };
}


typedef Meta::list<AppPolicy, DataGeneratorPolicy, DefaultCommPolicy> Policies;

void generate_update_load(PropertiesPtr &props, String &tablename, bool flush, bool no_log_sync,
                          ::uint64_t flush_interval, bool to_stdout, String &sample_fname,
                          ::int32_t delete_pct, bool thrift);
void generate_update_load_parallel(PropertiesPtr &props, String &tablename, ::int32_t parallel,
                                   bool flush, bool no_log_sync, ::uint64_t flush_interval,
                                   ::int32_t delete_pct, bool thrift);
void generate_query_load(PropertiesPtr &props, String &tablename, bool to_stdout,
                         ::int32_t delay, String &sample_fname, bool thrift);
double std_dev(::uint64_t nn, double sum, double sq_sum);
void parse_command_line(int argc, char **argv, PropertiesPtr &props);

int main(int argc, char **argv) {
  String table, load_type, spec_file, sample_fname;
  PropertiesPtr generator_props = new Properties();
  bool flush, to_stdout, no_log_sync, thrift;
  ::uint64_t flush_interval=0;
  ::int32_t query_delay = 0;
  ::int32_t delete_pct = 0;
  ::int32_t parallel = 0;

  try {
    init_with_policies<Policies>(argc, argv);

    if (!has("type")) {
      std::cout << cmdline_desc() << std::flush;
      _exit(0);
    }

    load_type = get_str("type");

    table = get_str("table");

    parallel = get_i32("parallel");

    sample_fname = has("sample-file") ? get_str("sample-file") : "";

    if (has("query-delay"))
      query_delay = get_i32("query-delay");

    flush = get_bool("flush");
    no_log_sync = get_bool("no-log-sync");
    to_stdout = get_bool("stdout");
    if (no_log_sync)
      flush_interval = get_i64("flush-interval");
    thrift = get_bool("thrift");

    if (has("spec-file")) {
      spec_file = get_str("spec-file");
      if (FileUtils::exists(spec_file))
        generator_props->load(spec_file, cmdline_hidden_desc(), true);
      else
        HT_THROW(Error::FILE_NOT_FOUND, spec_file);
    }

    parse_command_line(argc, argv, generator_props);

    if (generator_props->has("DataGenerator.MaxBytes") &&
	generator_props->has("DataGenerator.MaxKeys")) {
      HT_ERROR("Only one of 'DataGenerator.MaxBytes' or 'DataGenerator.MaxKeys' may be specified");
      _exit(1);
    }

    if (generator_props->has("DataGenerator.DeletePercentage")) {
      if (to_stdout)
        HT_FATAL("DataGenerator.DeletePercentage not supported with stdout option");
      delete_pct = generator_props->get_i32("DataGenerator.DeletePercentage");
    }
    
    if (parallel > 0 && load_type == "query")
      HT_FATAL("parallel support for query load not yet implemented");

    if (load_type == "update" && parallel > 0)
      generate_update_load_parallel(generator_props, table, parallel, flush,
                                    no_log_sync, flush_interval, delete_pct, thrift);
    else if (load_type == "update")
      generate_update_load(generator_props, table, flush, no_log_sync, flush_interval,
                           to_stdout, sample_fname, delete_pct, thrift);
    else if (load_type == "query") {
      if (!generator_props->has("DataGenerator.MaxKeys") && !generator_props->has("max-keys")) {
	HT_ERROR("'DataGenerator.MaxKeys' or --max-keys must be specified for load type 'query'");
	_exit(1);
      }
      generate_query_load(generator_props, table, to_stdout, query_delay, sample_fname, thrift);
    }
    else {
      std::cout << cmdline_desc() << std::flush;
      _exit(1);
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    exit(1);
  }

  fflush(stdout);
  _exit(0); // don't bother with static objects
}


void parse_command_line(int argc, char **argv, PropertiesPtr &props) {
  const char *ptr;
  String key, value;
  props->parse_args(argc, argv, cmdline_desc(), 0, 0, true);
  for (int i=1; i<argc; i++) {
    if (argv[i][0] == '-') {
      ptr = strchr(argv[i], '=');
      if (ptr) {
        key = String(argv[i], ptr-argv[i]);
        trim_if(key, is_any_of("-"));
        value = String(ptr+1);
        trim_if(value, is_any_of("'\""));
        if (key == "delete-percentage") {
          props->set(key, boost::any( atoi(value.c_str()) ));
          props->set("DataGenerator.DeletePercentage", boost::any( atoi(value.c_str()) ));
        }
        else if (key == ("max-bytes")) {
          props->set(key, boost::any( strtoll(value.c_str(), 0, 0) ));
          props->set("DataGenerator.MaxBytes", boost::any( strtoll(value.c_str(), 0, 0) ));
        }
        else if (key == ("max-keys")) {
          props->set(key, boost::any( strtoll(value.c_str(), 0, 0) ));
          props->set("DataGenerator.MaxKeys", boost::any( strtoll(value.c_str(), 0, 0) ));
        }
        else if (key == "seed") {
          props->set(key, boost::any( atoi(value.c_str()) ));
          props->set("DataGenerator.Seed", boost::any( atoi(value.c_str()) ));
        }
        else if (key == "row-seed") {
          props->set(key, boost::any( atoi(value.c_str()) ));
          props->set("rowkey.seed", boost::any( atoi(value.c_str()) ));
        }
        else
          props->set(key, boost::any(value));
      }
      else {
        key = String(argv[i]);
        trim_if(key, is_any_of("-"));
        if (!props->has(key))
          props->set(key, boost::any( true ));
      }
    }
  }
}


void generate_update_load(PropertiesPtr &props, String &tablename, bool flush,
                          bool no_log_sync, ::uint64_t flush_interval,
                          bool to_stdout, String &sample_fname,
                          ::int32_t delete_pct, bool thrift)
{
  double cum_latency=0, cum_sq_latency=0, latency=0;
  double min_latency=10000000, max_latency=0;
  ::uint64_t total_cells=0;
  ::uint64_t total_bytes=0;
  Cells cells;
  clock_t start_clocks=0, stop_clocks=0;
  double clocks_per_usec = (double)CLOCKS_PER_SEC / 1000000.0;
  bool output_samples = false;
  ofstream sample_file;
  DataGenerator dg(props);
  ::uint32_t mutator_flags=0;
  ::uint64_t unflushed_data=0;

  if (no_log_sync)
    mutator_flags |= TableMutator::FLAG_NO_LOG_SYNC;

  if (to_stdout) {
    cout << "rowkey\tcolumnkey\tvalue\n";
    for (DataGenerator::iterator iter = dg.begin(); iter != dg.end(); iter++) {
      if ((*iter).column_qualifier == 0 || *(*iter).column_qualifier == 0)
        cout << (*iter).row_key << "\t" << (*iter).column_family
             << "\t" << (const char *)(*iter).value << "\n";
      else
        cout << (*iter).row_key << "\t" << (*iter).column_family << ":"
             << (*iter).column_qualifier << "\t" << (const char *)(*iter).value << "\n";
    }
    cout << flush;
    return;
  }

  if (sample_fname != "") {
    sample_file.open(sample_fname.c_str());
    output_samples = true;
  }

  Stopwatch stopwatch;

  try {
    LoadClientPtr load_client_ptr;
    String config_file = get_str("config");
    bool key_limit = props->has("DataGenerator.MaxKeys");
    bool largefile_mode = false;
    uint32_t adjusted_bytes = 0;
    int64_t last_total = 0, new_total;

    if (dg.get_max_bytes() > std::numeric_limits<long>::max()) {
      largefile_mode = true;
      adjusted_bytes = (uint32_t)(dg.get_max_bytes() / 1048576LL);
    }
    else
      adjusted_bytes = dg.get_max_bytes();

    boost::progress_display progress_meter(key_limit ? dg.get_max_keys() : adjusted_bytes);

    if (config_file != "")
      load_client_ptr = new LoadClient(config_file, thrift);
    else
      load_client_ptr = new LoadClient(thrift);

    load_client_ptr->create_mutator(tablename, mutator_flags);

    for (DataGenerator::iterator iter = dg.begin(); iter != dg.end(); total_bytes+=iter.last_data_size(),++iter) {

      if (delete_pct != 0 && (::random() % 100) < delete_pct) {
        KeySpec key;
        key.row = (*iter).row_key;
        key.row_len = strlen((const char *)key.row);
        key.column_family = (*iter).column_family;
        key.column_qualifier = (*iter).column_qualifier;
        if (key.column_qualifier != 0)
          key.column_qualifier_len = strlen(key.column_qualifier);
        key.timestamp = (*iter).timestamp;
        key.revision = (*iter).revision;
        if (flush)
          start_clocks = clock();
        load_client_ptr->set_delete(key);
      }
      else {
        // do update
        cells.clear();
        cells.push_back(*iter);
        if (flush)
          start_clocks = clock();
        load_client_ptr->set_cells(cells);
      }

      if (flush) {
        load_client_ptr->flush();
        stop_clocks = clock();
        if (stop_clocks < start_clocks)
          latency = ((std::numeric_limits<clock_t>::max() - start_clocks) + stop_clocks) / clocks_per_usec;
        else
          latency = (stop_clocks-start_clocks) / clocks_per_usec;
        if (output_samples)
          sample_file << (unsigned long)latency << "\n";
        else {
          cum_latency += latency;
          cum_sq_latency += pow(latency,2);
          if (latency < min_latency)
            min_latency = latency;
          if (latency > max_latency)
            max_latency = latency;
        }
      }
      else if (flush_interval>0) {
        // if flush interval was specified then keep track of how much data is currently
        // not flushed and call flush once the flush interval limit is reached
        unflushed_data += iter.last_data_size();
        if (unflushed_data > flush_interval) {
          load_client_ptr->flush();
          unflushed_data = 0;
        }

      }

      ++total_cells;
      if (key_limit)
	       progress_meter += 1;
      else {
	       if (largefile_mode == true) {
	         new_total = last_total + iter.last_data_size();
	         uint32_t consumed = (uint32_t)((new_total / 1048576LL) - (last_total / 1048576LL));
	         last_total = new_total;
	         progress_meter += consumed;
	       }
	       else
	         progress_meter += iter.last_data_size();
      }
    }

    load_client_ptr->flush();

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    exit(1);
  }


  stopwatch.stop();

  printf("\n");
  printf("\n");
  printf("        Elapsed time: %.2f s\n", stopwatch.elapsed());
  printf("Total cells inserted: %llu\n", (Llu) total_cells);
  printf("Throughput (cells/s): %.2f\n", (double)total_cells/stopwatch.elapsed());
  printf("Total bytes inserted: %llu\n", (Llu)total_bytes);
  printf("Throughput (bytes/s): %.2f\n", (double)total_bytes/stopwatch.elapsed());

  if (flush && !output_samples) {
    printf("  Latency min (usec): %llu\n", (Llu)min_latency);
    printf("  Latency max (usec): %llu\n", (Llu)max_latency);
    printf("  Latency avg (usec): %llu\n", (Llu)((double)cum_latency/total_cells));
    printf("Latency stddev (usec): %llu\n", (Llu)std_dev(total_cells, cum_latency, cum_sq_latency));
  }
  printf("\n");

  if (output_samples)
    sample_file.close();

}

void generate_update_load_parallel(PropertiesPtr &props, String &tablename, ::int32_t parallel,
                                   bool flush, bool no_log_sync, ::uint64_t flush_interval,
                                   ::int32_t delete_pct, bool thrift)
{
  double cum_latency=0, cum_sq_latency=0;
  double min_latency=0, max_latency=0;
  ::uint64_t total_cells=0;
  ::uint64_t total_bytes=0;
  Cells cells;
  ofstream sample_file;
  DataGenerator dg(props);
  ::uint32_t mutator_flags=0;
  std::vector<ParallelStateRec> load_vector(parallel);
  ::uint32_t next = 0;
  boost::thread_group threads;

  if (no_log_sync)
    mutator_flags |= TableMutator::FLAG_NO_LOG_SYNC;

  Stopwatch stopwatch;

  try {
    ClientPtr client;
    NamespacePtr ht_namespace;
    TablePtr table;
    LoadClientPtr load_client_ptr;
    String config_file = get_str("config");
    bool key_limit = props->has("DataGenerator.MaxKeys");
    bool largefile_mode = false;
    uint32_t adjusted_bytes = 0;
    int64_t last_total = 0, new_total;
    LoadRec *lrec;

    client = new Hypertable::Client(config_file);
    ht_namespace = client->open_namespace("/");
    table = ht_namespace->open_table(tablename);

    for (::int32_t i=0; i<parallel; i++)
      threads.create_thread(LoadThread(table, mutator_flags, load_vector[i]));

    if (dg.get_max_bytes() > std::numeric_limits<long>::max()) {
      largefile_mode = true;
      adjusted_bytes = (uint32_t)(dg.get_max_bytes() / 1048576LL);
    }
    else
      adjusted_bytes = dg.get_max_bytes();

    boost::progress_display progress_meter(key_limit ? dg.get_max_keys() : adjusted_bytes);

    for (DataGenerator::iterator iter = dg.begin(); iter != dg.end(); total_bytes+=iter.last_data_size(),++iter) {

      if (delete_pct != 0 && (::random() % 100) < delete_pct)
        lrec = new LoadRec(*iter, true);
      else
        lrec = new LoadRec(*iter);
      lrec->amount = iter.last_data_size();

      {
        ScopedLock lock(load_vector[next].mutex);
        load_vector[next].requests.push_back(lrec);
        // Delete garbage, update progress meter
        while (!load_vector[next].garbage.empty()) {
          LoadRec *garbage = load_vector[next].garbage.front();
          if (key_limit)
            progress_meter += 1;
          else {
            if (largefile_mode == true) {
              new_total = last_total + garbage->amount;
              uint32_t consumed = (uint32_t)((new_total / 1048576LL) - (last_total / 1048576LL));
              last_total = new_total;
              progress_meter += consumed;
            }
            else
              progress_meter += garbage->amount;
          }
          delete garbage;
          load_vector[next].garbage.pop_front();
        }
        load_vector[next].cond.notify_all();
      }
      next = (next+1) % parallel;

      ++total_cells;

    }

    for (::int32_t i=0; i<parallel; i++) {
      ScopedLock lock(load_vector[next].mutex);
      load_vector[i].finished = true;
      load_vector[i].cond.notify_all();
    }

    threads.join_all();

    min_latency = load_vector[0].min_latency;
    for (::int32_t i=0; i<parallel; i++) {
      cum_latency += load_vector[i].cum_latency;
      cum_sq_latency += load_vector[i].cum_sq_latency;
      if (load_vector[i].min_latency < min_latency)
        min_latency = load_vector[i].min_latency;
      if (load_vector[i].max_latency > max_latency)
        max_latency = load_vector[i].max_latency;
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    exit(1);
  }

  stopwatch.stop();

  printf("\n");
  printf("\n");
  printf("        Elapsed time: %.2f s\n", stopwatch.elapsed());
  printf("Total cells inserted: %llu\n", (Llu) total_cells);
  printf("Throughput (cells/s): %.2f\n", (double)total_cells/stopwatch.elapsed());
  printf("Total bytes inserted: %llu\n", (Llu)total_bytes);
  printf("Throughput (bytes/s): %.2f\n", (double)total_bytes/stopwatch.elapsed());
  if (true) {
  //if (flush) {
    printf("  Latency min (usec): %llu\n", (Llu)min_latency);
    printf("  Latency max (usec): %llu\n", (Llu)max_latency);
    printf("  Latency avg (usec): %llu\n", (Llu)((double)cum_latency/total_cells));
    printf("Latency stddev (usec): %llu\n", (Llu)std_dev(total_cells, cum_latency, cum_sq_latency));
  }

  printf("\n");

}


void generate_query_load(PropertiesPtr &props, String &tablename, bool to_stdout, ::int32_t delay, String &sample_fname, bool thrift)
{
  double cum_latency=0, cum_sq_latency=0, latency=0;
  double min_latency=10000000, max_latency=0;
  ::int64_t total_cells=0;
  ::int64_t total_bytes=0;
  Cells cells;
  clock_t start_clocks, stop_clocks;
  double clocks_per_usec = (double)CLOCKS_PER_SEC / 1000000.0;
  bool output_samples = false;
  ofstream sample_file;
  DataGenerator dg(props, true);

  if (to_stdout) {
    for (DataGenerator::iterator iter = dg.begin(); iter != dg.end(); iter++) {
      if (*(*iter).column_qualifier == 0)
        cout << (*iter).row_key << "\t" << (*iter).column_family << "\n";
      else
        cout << (*iter).row_key << "\t" << (*iter).column_family << ":"
             << (*iter).column_qualifier << "\n";
    }
    cout << flush;
    return;
  }

  if (sample_fname != "") {
    sample_file.open(sample_fname.c_str());
    output_samples = true;
  }

  Stopwatch stopwatch;

  try {
    LoadClientPtr load_client_ptr;
    ScanSpecBuilder scan_spec;
    Cell cell;
    String config_file = get_str("config");
    boost::progress_display progress_meter(dg.get_max_keys());
    uint64_t last_bytes = 0;

    if (config_file != "")
      load_client_ptr = new LoadClient(config_file, thrift);
    else
      load_client_ptr = new LoadClient(thrift);


    for (DataGenerator::iterator iter = dg.begin(); iter != dg.end(); iter++) {

      if (delay)
        poll(0, 0, delay);

      scan_spec.clear();
      scan_spec.add_column((*iter).column_family);
      scan_spec.add_row((*iter).row_key);

      start_clocks = clock();

      load_client_ptr->create_scanner(tablename, scan_spec.get());
      last_bytes = load_client_ptr->get_all_cells();
      total_bytes += last_bytes;
      load_client_ptr->close_scanner();

      stop_clocks = clock();
      if (stop_clocks < start_clocks)
        latency = ((std::numeric_limits<clock_t>::max() - start_clocks) + stop_clocks) / clocks_per_usec;
      else
        latency = (stop_clocks-start_clocks) / clocks_per_usec;
      if (output_samples)
        sample_file << (unsigned long)latency << "\n";
      else {
        cum_latency += latency;
        cum_sq_latency += pow(latency,2);
        if (latency < min_latency)
          min_latency = latency;
        if (latency > max_latency)
          max_latency = latency;
      }

      if (last_bytes)
        ++total_cells;
      progress_meter += 1;
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    exit(1);
  }

  stopwatch.stop();

  printf("\n");
  printf("\n");
  printf("        Elapsed time: %.2f s\n", stopwatch.elapsed());
  printf("Total cells returned: %llu\n", (Llu) total_cells);
  printf("Throughput (cells/s): %.2f\n", (double)total_cells/stopwatch.elapsed());
  printf("Total bytes returned: %llu\n", (Llu)total_bytes);
  printf("Throughput (bytes/s): %.2f\n", (double)total_bytes/stopwatch.elapsed());

  if (!output_samples) {
    printf("  Latency min (usec): %llu\n", (Llu)min_latency);
    printf("  Latency max (usec): %llu\n", (Llu)max_latency);
    printf("  Latency avg (usec): %llu\n", (Llu)((double)cum_latency/total_cells));
    printf("Latency stddev (usec): %llu\n", (Llu)std_dev(total_cells, cum_latency, cum_sq_latency));
  }
  printf("\n");

  if (output_samples)
    sample_file.close();
}



/**
 * @param nn Size of set of numbers
 * @param sum Sum of numbers in set
 * @param sq_sum Sum of squares of numbers in set
 * @return std deviation of set
 */
double std_dev(::uint64_t nn, double sum, double sq_sum)
{
  double mean = sum/nn;
  double sq_std = sqrt((sq_sum/(double)nn) - pow(mean,2));
  return sq_std;
}
