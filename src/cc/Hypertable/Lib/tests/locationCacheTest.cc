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

#include <fstream>
#include <utility>

#include "Common/NumberStream.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/LocationCache.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: locationCacheTest",
    "",
    "Validates LocationCache class.  Generates output file './locationCacheTest.output' and",
    "diffs it against ./locationCacheTest.golden'.",
    0
  };
  typedef pair<const char *, const char *> RowRangeT;

  const int MAX_RANGES = 33;
  const int MAX_WORDS = 90;
  const int MAX_SERVERIDS = 11;

  RowRangeT ranges[MAX_RANGES] = {
    RowRangeT("","allogene"),
    RowRangeT("allogene", "archtreasurer"),
    RowRangeT("archtreasurer", "beerocracy"),
    RowRangeT("beerocracy", "bulblet"),
    RowRangeT("bulblet", "chieftainship"),
    RowRangeT("chieftainship", "consolatory"),
    RowRangeT("consolatory", "deaconal"),
    RowRangeT("deaconal", "diumvirate"),
    RowRangeT("diumvirate", "Epicureanism"),
    RowRangeT("Epicureanism", "flaminica"),
    RowRangeT("flaminica", "globulet"),
    RowRangeT("globulet", "heterochromatin"),
    RowRangeT("heterochromatin", "impressionistically"),
    RowRangeT("impressionistically", "janker"),
    RowRangeT("janker", "linder"),
    RowRangeT("linder", "merohedrism"),
    RowRangeT("merohedrism", "mycodomatium"),
    RowRangeT("mycodomatium", "nunatak"),
    RowRangeT("nunatak", "oversound"),
    RowRangeT("oversound", "perkingly"),
    RowRangeT("perkingly", "polymely"),
    RowRangeT("polymely", "prosopyl"),
    RowRangeT("prosopyl", "reconsultation"),
    RowRangeT("reconsultation", "Saan"),
    RowRangeT("Saan", "setterwort"),
    RowRangeT("setterwort", "spherics"),
    RowRangeT("spherics", "sulphoarsenious"),
    RowRangeT("sulphoarsenious", "tetrazolyl"),
    RowRangeT("tetrazolyl", "trophic"),
    RowRangeT("trophic", "undoubtingness"),
    RowRangeT("undoubtingness", "unserrated"),
    RowRangeT("unserrated", "vowellessness"),
    RowRangeT("vowellessness", "")
  };

  const char *words[MAX_WORDS] = {
    "acrogynae",
    "airgraphics",
    "Ampelosicyos",
    "anthracitization",
    "arachidonic",
    "astragalonavicular",
    "backspread",
    "beefer",
    "biophysics",
    "bountyless",
    "buscarle",
    "Carcharodon",
    "cerulein",
    "christcross",
    "coastal",
    "concordist",
    "correlativity",
    "crownbeard",
    "dapperly",
    "deozonization",
    "dime",
    "Docetize",
    "earnestness",
    "enchytraeid",
    "eradicable",
    "expansional",
    "feuille",
    "forbearingly",
    "gabioned",
    "Gigartina",
    "greaseproofness",
    "hardback",
    "hesperidin",
    "horsewhipper",
    "hyposynaphe",
    "incident",
    "insomnolency",
    "iridoconstrictor",
    "jumboesque",
    "labyrinthodontid",
    "Lethocerus",
    "loving",
    "mannan",
    "meningoencephalocele",
    "millstream",
    "monosilane",
    "myodynamics",
    "newspaperish",
    "nonpacifist",
    "occipitomastoid",
    "organizatory",
    "overdaringly",
    "palaeographer",
    "Parsism",
    "perhazard",
    "phonodynamograph",
    "placentate",
    "polyglotter",
    "precant",
    "prevailingly",
    "protopatrician",
    "pycniospore",
    "ranklingly",
    "regenerateness",
    "retile",
    "rosolite",
    "sarcoma",
    "scurrilize",
    "seriopantomimic",
    "silicotitanate",
    "snoove",
    "spiflicated",
    "stenostomia",
    "subcylindrical",
    "superexpansion",
    "Syriarch",
    "Teloogoo",
    "thirstful",
    "torturing",
    "trinitroresorcin",
    "tyrology",
    "uncloak",
    "undistended",
    "unidentifiably",
    "unperplexing",
    "unsocially",
    "upwaft",
    "vervelle",
    "waterworm",
    "worldful"
  };

  const char *serverIds[MAX_SERVERIDS] = {
    "192.168.1.100:1234_282298",
    "192.168.1.101:1234_267346",
    "192.168.1.102:1234_982733",
    "192.168.1.103:1234_823482",
    "192.168.1.104:1234_712562",
    "192.168.1.105:1234_127834",
    "192.168.1.106:1234_928734",
    "192.168.1.107:1234_379872",
    "192.168.1.108:1234_123223",
    "192.168.1.109:1234_629873",
    "192.168.1.110:1234_832333"
  };
    

  ofstream outfile;

  void TestLookup(LocationCache &cache, uint32_t tableId, const char *rowKey) {
    RangeLocationInfo  range_loc_info;
    outfile << "LOOKUP(" << tableId << ", " << rowKey << ") -> ";

    if (cache.lookup(tableId, rowKey, &range_loc_info))
      outfile << range_loc_info.location << endl;
    else
      outfile << "[NULL]" << endl;
  }
  
}


int main(int argc, char **argv) {
  LocationCache cache(68);
  NumberStream randstr("./random.dat");
  uint32_t rangei;
  uint32_t tableId;
  uint32_t serveri;
  const char *start, *end;
  RangeLocationInfo range_loc_info;

  if (argc > 1 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-?")))
    Usage::dump_and_exit(usage);

  outfile.open("./locationCacheTest.output");

  range_loc_info.start_row = "bar";
  range_loc_info.end_row = "kite";
  range_loc_info.location = "234345";
  cache.insert(0, range_loc_info);

  range_loc_info.start_row = "foo";
  range_loc_info.end_row = "kite";
  range_loc_info.location = "234345";
  cache.insert(0, range_loc_info);

  TestLookup(cache, 0, "foo");
  TestLookup(cache, 0, "food");
  TestLookup(cache, 0, "kite");
  TestLookup(cache, 0, "kited");

  for (size_t i=0; i<2000; i++) {
    if ((randstr.getInt() % 3) == 0)
      TestLookup(cache, randstr.getInt() % 4, words[randstr.getInt() % MAX_WORDS]);
    else {
      rangei = randstr.getInt() % MAX_RANGES;
      tableId = randstr.getInt() % 4;
      serveri = randstr.getInt() % MAX_SERVERIDS;
      start = (*ranges[rangei].first == 0) ? "[NULL]" : ranges[rangei].first;
      end = (*ranges[rangei].second == 0) ? "[NULL]" : ranges[rangei].second;
      outfile << "INSERT(" << tableId << ", " << start << ", " << end << ", " << serverIds[serveri] << endl << flush;
      range_loc_info.start_row = ranges[rangei].first;
      range_loc_info.end_row   = ranges[rangei].second;
      range_loc_info.location  = serverIds[serveri];
      cache.insert(tableId, range_loc_info);
    }
  }

  cache.display(outfile);

  outfile.close();

  if (system("diff ./locationCacheTest.output ./locationCacheTest.golden"))
    return 1;

  return 0;
}
