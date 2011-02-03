/** -*- c++ -*-
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
#include "Common/Init.h"
#include "Common/DynamicBuffer.h"
#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include <iostream>
#include <fstream>

#include "AsyncComm/ConnectionManager.h"

#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/SerializedKey.h"

#include "../CellStoreV4.h"
#include "../FileBlockCache.h"
#include "../Global.h"

#include <cstdlib>

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: CellStoreScanner_test",
    "",
    "  This program tests for the proper functioning of the CellStore",
    "  scanner.  It creates a dummy cell store and then repeatedly scans",
    "  it with different ranges",
    (const char *)0
  };
  const char *schema_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily id=\"1\">\n"
  "      <Name>tag</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
  "</Schema>";

  const char *words[] = {
    "prolif",
    "yonsid",
    "testam",
    "hypost",
    "scagli",
    "bandym",
    "protei",
    "paleot",
    "hetero",
    "undeli",
    "Megach",
    "hepari",
    "salama",
    "stereo",
    "favora",
    "aeroli",
    "ovopyr",
    "persec",
    "fondle",
    "Diplod",
    "federa",
    "folkfr",
    "doorli",
    "healin",
    "rugosi",
    "underh",
    "consum",
    "overle",
    "cumins",
    "Hannib",
    "etiolo",
    "unprop",
    "Julian",
    "unsnar",
    "tarril",
    "bratti",
    "tettig",
    "gariba",
    "spermo",
    "palmic",
    "Lauren",
    "basipo",
    "peculi",
    "hetero",
    "unders",
    "untoot",
    "propro",
    "ostrac",
    "Felapt",
    "alloty",
    "Macrur",
    "verdel",
    "semici",
    "humbug",
    "Easter",
    "epacri",
    "holopt",
    "unguid",
    "Nummul",
    "fondak",
    "protog",
    "microm",
    "blight",
    "Guauae",
    "bradya",
    "typhom",
    "ducato",
    "trapez",
    "vipres",
    "coilin",
    "depanc",
    "gnatho",
    "micasi",
    "garnet",
    "thrack",
    "humora",
    "Philis",
    "tropic",
    "cloyso",
    "oxypro",
    "uphols",
    "therol",
    "seawor",
    "nigrif",
    "Calyst",
    "bulimi",
    "leptot",
    "protiu",
    "schrei",
    "laemod",
    "Melast",
    "nonalu",
    "nonsen",
    "chatoy",
    "Pomace",
    "implor",
    "syngen",
    "infide",
    "opiany",
    "archit",
    "unwife",
    "nonthi",
    "cataco",
    "dispul",
    "formal",
    "afters",
    "indica",
    "turnca",
    "feldsp",
    "uralit",
    "suidia",
    "hydrol",
    "parapo",
    "cavern",
    "ammoni",
    "propit",
    "overan",
    "fringe",
    "planet",
    "gangli",
    "rehand",
    "lucidn",
    "anthra",
    "Palaeo",
    "murgav",
    "Billji",
    "thingl",
    "hundre",
    "spheru",
    "worser",
    "phalli",
    "blaene",
    "Achern",
    "Acroce",
    "forese",
    "phytoo",
    "palaeo",
    "enfudd",
    "Laemod",
    "urolit",
    "presyn",
    "harmon",
    "preade",
    "bucket",
    "redoub",
    "submax",
    "sociog",
    "epidid",
    "capsul",
    "interc",
    "unders",
    "chryso",
    "couser",
    "untran",
    "argill",
    "Alkora",
    "antisy",
    "undisp",
    "indire",
    "clashy",
    "uncomm",
    "glaieu",
    "electr",
    "undere",
    "preten",
    "tuberc",
    "terato",
    "docume",
    "Manche",
    "facien",
    "lethol",
    "usitat",
    "othelc",
    "primog",
    "gypsog",
    "earthb",
    "backst",
    "cloudi",
    "uncouc",
    "Panaya",
    "utricu",
    "person",
    "mediov",
    "transc",
    "acetab",
    "polycy",
    "skunkd",
    "prepre",
    "delftw",
    "Acanth",
    "frostp",
    "blackg",
    "tetraz",
    "Olympi",
    "lubrif",
    "grazab",
    "Schist",
    "divers",
    "unconc",
    "nonven",
    "overbl",
    "vivise",
    "visual",
    "postin",
    "catach",
    "tribal",
    "digyni",
    "outhor",
    "Tachin",
    "Syriol",
    "histor",
    "Amoreu",
    "coachm",
    "absent",
    "stibic",
    "subseq",
    "nonoec",
    "caraco",
    "Caripu",
    "Uranic",
    "fletch",
    "acedia",
    "temera",
    "beadle",
    "bancal",
    "mordic",
    "superd",
    "Polyne",
    "Interl",
    "diapho",
    "contin",
    "wastem",
    "cubele",
    "Chorda",
    "unsati",
    "prefin",
    "Amanda",
    "Micros",
    "nonrep",
    "corpus",
    "precas",
    "uncrib",
    "delayf",
    "carlin",
    "dilogy",
    "gravim",
    "unstop",
    "shorem",
    "speakl",
    "inenar",
    "antiho",
    "benzol",
    "inoppo",
    "decrus",
    "compos",
    "Stenop",
    "Rhapis",
    "youngl",
    "laeotr",
    "cannon",
    "nonute",
    "phyllo",
    "ascidi",
    "berate",
    "holoqu",
    "analep",
    "kynuri",
    "conver",
    "overfa",
    "pigflo",
    "suprap",
    "Mattap",
    "citabl",
    "urocer",
    "altern",
    "Subosc",
    "dietet",
    "spiffi",
    "perica",
    "placen",
    "circum",
    "aeroph",
    "harmon",
    "hodder",
    "morphe",
    "marmot",
    "bechir",
    "superc",
    "undome",
    "noncen",
    "Teuton",
    "Uragog",
    "scribi",
    "endodo",
    "praeta",
    "smirkl",
    "Redemp",
    "superr",
    "haplop",
    "poster",
    "chills",
    "Cacaja",
    "unpain",
    "concep",
    "unprac",
    "expunc",
    "ticket",
    "Boulan",
    "lamini",
    "treaty",
    "smokis",
    "straig",
    "hypocr",
    "overle",
    "defini",
    "Dalmat",
    "straig",
    "scoldi",
    "ulster",
    "prevol",
    "redesp",
    "polyhy",
    "unhang",
    "habita",
    "unscra",
    "millif",
    "befume",
    "Panhel",
    "malaci",
    "omnipa",
    "relent",
    "rockal",
    "Royena",
    "Varang",
    "cytoge",
    "superc",
    "pluris",
    "skedad",
    "recons",
    "interp",
    "unclas",
    "infruc",
    "folded",
    "bronch",
    "unlawf",
    "bridge",
    "thinka",
    "Sudani",
    "singab",
    "triflo",
    "slumwi",
    "Aepyce",
    "muskro",
    "eucras",
    "Heracl",
    "ungirt",
    "tinker",
    "supple",
    "martel",
    "tympan",
    "octona",
    "neebor",
    "semime",
    "theodo",
    "remedi",
    "unsucc",
    "agangl",
    "labial",
    "Termit",
    "irrevo",
    "Schope",
    "expans",
    "propro",
    "theelo",
    "unslep",
    "greyne",
    "palust",
    "eventl",
    "danali",
    "bisymm",
    "Opisth",
    "outben",
    "agrono",
    "schizo",
    "retake",
    "subdeb",
    "plotte",
    "palsgr",
    "Gonyst",
    "stickf",
    "pretra",
    "muffed",
    "statut",
    "hinoid",
    "logist",
    "centra",
    "stepch",
    "forema",
    "oometr",
    "nubige",
    "undimi",
    "deutop",
    (const char *)0
  };

  size_t display_scan(CellListScannerPtr &scanner, ostream &out) {
    Key key_comps;
    ByteString bsvalue;
    size_t count = 0;
    while (scanner->get(key_comps, bsvalue)) {
      out << key_comps << "\n";
      count++;
      scanner->forward();
    }
    out << flush;
    return count;
  }
}


int main(int argc, char **argv) {
  try {
    struct sockaddr_in addr;
    ConnectionManagerPtr conn_mgr;
    DfsBroker::ClientPtr client;
    CellStorePtr cs;
    std::ofstream out("CellStoreScanner_delete_test.output");
    String delete_row = "delete_row";
    String delete_cf  = "delete_cf";
    String delete_row_cf = "delete_row_cf";
    String delete_none = "delete_none";
    String delete_large = "delete_large";
    String insert = "insert";

    Config::init(argc, argv);

    if (Config::has("help"))
      Usage::dump_and_exit(usage);

    ReactorFactory::initialize(2);

    uint16_t port = Config::properties->get_i16("DfsBroker.Port");

    InetAddr::initialize(&addr, "localhost", port);

    conn_mgr = new ConnectionManager();
    Global::dfs = new DfsBroker::Client(conn_mgr, addr, 15000);

    // force broker client to be destroyed before connection manager
    client = (DfsBroker::Client *)Global::dfs.get();

    if (!client->wait_for_connection(15000)) {
      HT_ERROR("Unable to connect to DFS");
      return 1;
    }

    Global::block_cache = new FileBlockCache(100000LL, 100000LL);
    Global::memory_tracker = new MemoryTracker(Global::block_cache);

    String testdir = "/CellStoreScanner_delete_test";
    client->mkdirs(testdir);

    SchemaPtr schema = Schema::new_instance(schema_str, strlen(schema_str),
                                            true);
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }

    String csname = testdir + "/cs0";
    PropertiesPtr cs_props = new Properties();
    // make sure blocks are small so only one key value pair fits in a block
    cs_props->set("blocksize", uint32_t(32));
    cs = new CellStoreV4(Global::dfs.get(), schema.get());
    HT_TRY("creating cellstore", cs->create(csname.c_str(), 24000, cs_props));

    DynamicBuffer dbuf(512000);
    String row;
    String qualifier;
    SerializedKey serkey;
    int64_t timestamp = 1;
    std::vector<SerializedKey> serkeyv;
    std::vector<Key> keyv;
    Key key;
    ScanContextPtr scan_ctx;
    String value="0";
    uint8_t valuebuf[128];
    uint8_t *uptr;
    ByteString bsvalue;

    uptr = valuebuf;
    Serialization::encode_vi32(&uptr,value.length());
    strcpy((char *)uptr, value.c_str());
    bsvalue.ptr = valuebuf;


    // test delete logic
    {
      // delete row
      serkey.ptr = dbuf.ptr;
      row = delete_row;
      qualifier = insert;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1,
                            qualifier.c_str(), timestamp, timestamp);
      timestamp++;
      serkeyv.push_back(serkey);
      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_ROW, row.c_str(), 0, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      // delete column family
      serkey.ptr = dbuf.ptr;
      row = delete_cf;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);
      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_COLUMN_FAMILY, row.c_str(), 1, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      // delete row & column family
      serkey.ptr = dbuf.ptr;
      row = delete_row_cf;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_ROW, row.c_str(), 0, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_COLUMN_FAMILY, row.c_str(), 1, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_ROW, row.c_str(), 0, "", timestamp,
                            timestamp);
      timestamp++;
       serkeyv.push_back(serkey);

      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_COLUMN_FAMILY, row.c_str(), 1, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      // delete none
      serkey.ptr = dbuf.ptr;
      row = delete_none;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);


      // delete large
      serkey.ptr = dbuf.ptr;
      row = delete_large;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      size_t wordi = 0;
      String word;
      while (dbuf.fill() < 140000) {
        serkey.ptr = dbuf.ptr;
        if (words[wordi] == 0)
          wordi = 0;
        word = words[wordi++];
        create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, word.c_str(), timestamp,
                              timestamp);
        timestamp++;
        serkeyv.push_back(serkey);
      }

      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_COLUMN_FAMILY, row.c_str(), 1, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      while (dbuf.fill() < 280000) {
        serkey.ptr = dbuf.ptr;
        if (words[wordi] == 0)
          wordi = 0;
        word = words[wordi++];
        create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, word.c_str(), timestamp,
                              timestamp);
        timestamp++;
        serkeyv.push_back(serkey);
      }
      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_ROW, row.c_str(), 0, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);
    }

    sort(serkeyv.begin(), serkeyv.end());

    keyv.reserve( serkeyv.size() );

    out << "[baseline]\n";
    for (size_t i=0; i<serkeyv.size(); i++) {
      key.load( serkeyv[i] );
      cs->add(key, bsvalue);
      if (delete_large.compare(key.row) || key.flag == FLAG_DELETE_ROW ||
          key.flag == FLAG_DELETE_COLUMN_FAMILY || !insert.compare(key.column_qualifier))
        out << key << "\n";
      keyv.push_back(key);
    }

    TableIdentifier table_id("0");
    cs->finalize(&table_id);

    RangeSpec range;
    range.start_row = "";
    range.end_row = Key::END_ROW_MARKER;

    ScanSpecBuilder ssbuilder;
    String column;

    CellListScannerPtr scanner;

    /**
     * Test deletes
     */

    out << "[delete-row]\n";
    ssbuilder.clear();
    row = delete_row;
    column = (String)"tag:" + qualifier;
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    ssbuilder.clear();
    ssbuilder.add_cell_interval(row.c_str(),"tag:a", true,
        row.c_str(), "tag:z", true);
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    ssbuilder.clear();
    ssbuilder.add_row(row.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    out << "[delete-cf]\n";
    ssbuilder.clear();
    row = delete_cf;
    column = (String)"tag:" + qualifier;
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    ssbuilder.clear();
    ssbuilder.add_cell_interval(row.c_str(),"tag:a", true,
        row.c_str(), "tag:z", true);
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    ssbuilder.clear();
    ssbuilder.add_cell_interval(row.c_str(),"tag", true,
        row.c_str(), "tag:z", true);
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    ssbuilder.clear();
    ssbuilder.add_row(row.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);


    out << "[delete-row-cf]\n";
    ssbuilder.clear();
    row = delete_row_cf;
    column = (String)"tag:" + qualifier;
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    ssbuilder.clear();
    ssbuilder.add_cell_interval(row.c_str(),"tag", true,
        row.c_str(), "tag", true);
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    out << "[delete-none]\n";
    ssbuilder.clear();
    row = delete_none;
    column = (String) "tag:"+qualifier;
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    out << "[delete-large]\n";
    ssbuilder.clear();
    row = delete_large;
    column = (String)"tag:" + qualifier;
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                               schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    out << flush;
    String cmd_str = "diff CellStoreScanner_delete_test.output "
                     "CellStoreScanner_delete_test.golden";
    if (system(cmd_str.c_str()) != 0)
      return 1;

    client->rmdir(testdir);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  catch (...) {
    HT_ERROR_OUT << "unexpected exception caught" << HT_END;
    return 1;
  }
  return 0;
}

