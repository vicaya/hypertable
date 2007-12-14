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

#include "Common/StringExt.h"

#include "HqlHelpText.h"

using namespace Hypertable;

namespace {

  const char *help_text_contents =
  "\n" \
  "CREATE TABLE      - Creates a table\n" \
  "DESCRIBE TABLE    - Displays a table's schema\n" \
  "INSERT            - Inserts data into a table\n" \
  "LOAD DATA INFILE  - Loads data from a tab delimited input file into a table\n" \
  "SELECT            - Select (and display) cells from a table\n" \
  "SHOW CREATE TABLE - Displays CREATE TABLE command used to create table\n" \
  "\n" \
  "For more information, type 'help <item>', where <item> is one of the preceeding\n" \
  "commands.\n" \
  "\n";

  const char *help_text_create_table =
  "\n" \
  "CREATE TABLE name '(' [create_definition, ...] ')'\n" \
  "\n" \
  "create_definition:\n" \
  "    column_family_name [MAX_VERSIONS '=' value] [TTL '=' duration]\n" \
  "    | ACCESS GROUP name [IN_MEMORY] [BLOCKSIZE '=' value] ['(' [column_family_name, ...] ')']\n" \
  "\n" \
  "duration:\n" \
  "    num MONTHS\n" \
  "    | num WEEKS\n" \
  "    | num DAYS\n" \
  "    | num HOURS\n" \
  "    | num MINUTES\n" \
  "    | num SECONDS\n" \
  "\n";

  const char *help_text_select =
  "\n" \
  "SELECT ( '*' | column_family_name [ ',' column_family_name]* )\n" \
  "    FROM table_name\n" \
  "    [where_clause]\n" \
  "    [options_spec]\n" \
  "\n" \
  "where_clause:\n" \
  "    WHERE where_predicate [ && where_predicate ... ] \n" \
  "\n" \
  "where_predicate: \n" \
  "    ROW = row_key\n" \
  "    | ROW > row_key\n"
  "    | ROW >= row_key\n" \
  "    | ROW < row_key\n" \
  "    | ROW <= row_key\n" \
  "    | TIMESTAMP >  timestamp\n" \
  "    | TIMESTAMP >= timestamp\n" \
  "    | TIMESTAMP <  timestamp\n" \
  "    | TIMESTAMP <= timestamp\n" \
  "\n" \
  "options_spec:\n" \
  "    ( MAX_VERSIONS = version_count\n" \
  "    | START_TIME = timestamp\n" \
  "    | END_TIME = timestamp\n" \
  "    | LIMIT = row_count\n" \
  "    | INTO FILE 'file_name' )*\n" \
  "\n" \
  "timestamp:\n" \
  "    'YYYY-MM-DD HH:MM:SS[.nanoseconds]'\n" \
  "\n";

  const char *help_text_describe_table =
  "\n" \
  "DESCRIBE TABLE name\n" \
  "\n" \
  "Example:\n" \
  "\n" \
  "hypertable> describe table Test1;\n" \
  "\n" \
  "<Schema generation=\"1\">\n" \
  "  <AccessGroup name=\"jan\">\n" \
  "    <ColumnFamily id=\"4\">\n" \
  "      <Name>cherry</Name>\n" \
  "    </ColumnFamily>\n" \
  "  </AccessGroup>\n" \
  "  <AccessGroup name=\"default\">\n" \
  "    <ColumnFamily id=\"5\">\n" \
  "      <Name>banana</Name>\n" \
  "    </ColumnFamily>\n" \
  "    <ColumnFamily id=\"6\">\n" \
  "      <Name>apple</Name>\n" \
  "    </ColumnFamily>\n" \
  "  </AccessGroup>\n" \
  "  <AccessGroup name=\"marsha\">\n" \
  "    <ColumnFamily id=\"7\">\n" \
  "      <Name>onion</Name>\n" \
  "    </ColumnFamily>\n" \
  "    <ColumnFamily id=\"8\">\n" \
  "      <Name>cassis</Name>\n" \
  "    </ColumnFamily>\n" \
  "  </AccessGroup>\n" \
  "</Schema>\n" \
  "\n";

  const char *help_text_show_create_table = 
  "\n" \
  "SHOW CREATE TABLE name\n" \
  "\n" \
  "Example:\n" \
  "\n" \
  "hypertable> show create table Test1;\n" \
  "\n" \
  "CREATE TABLE Test1 (\n" \
  "  banana,\n" \
  "  apple,\n" \
  "  cherry,\n" \
  "  onion,\n" \
  "  cassis,\n" \
  "  ACCESS GROUP jan ( cherry ),\n" \
  "  ACCESS GROUP default ( banana apple ),\n" \
  "  ACCESS GROUP marsha ( onion cassis )\n" \
  ")\n" \
  "\n";

  const char *help_text_load_data_infile = 
  "\n" \
  "LOAD DATA INFILE fname INTO TABLE name\n" \
  "\n" \
  "Example:\n" \
  "\n" \
  "hypertable> load data infile \"data.txt\" into table Test1;\n" \
  "\n" \
  "Loading 238,920,253 bytes of input data...\n" \
  "\n" \
  "0%   10   20   30   40   50   60   70   80   90   100%\n" \
  "|----|----|----|----|----|----|----|----|----|----|\n" \
  "***************************************************\n" \
  "Load complete (59.70s elapsed_time, 4001802.20 bytes/s, 33499.06 inserts/s)\n" \
  "\n";  

  const char *help_text_insert =
  "\n" \
  "INSERT INTO table_name VALUES value_list\n" \
  "\n" \
  "value_list:\n" \
  "    value_spec [ ',' value_spec ... ]\n" \
  "\n" \
  "value_spec:\n" \
  "    '(' row_key ',' column_key ',' value ')'\n" \
  "    '(' timestamp ',' row_key ',' column_key ',' value ')'\n" \
  "\n" \
  "timestamp:\n" \
  "    'YYYY-MM-DD HH:MM:SS[.nanoseconds]'\n" \
  "\n";


  typedef __gnu_cxx::hash_map<std::string, const char *>  HelpTextMapT;

  HelpTextMapT &buildHelpTextMap() {
    HelpTextMapT *map = new HelpTextMapT();
    (*map)["contents"] = help_text_contents;
    (*map)["create table"] = help_text_create_table;
    (*map)["insert"] = help_text_insert;
    (*map)["select"] = help_text_select;
    (*map)["describe table"] = help_text_describe_table;
    (*map)["show create table"] = help_text_show_create_table;
    (*map)["load data infile"] = help_text_load_data_infile;
    return *map;
  }

  HelpTextMapT &textMap = buildHelpTextMap();
}


const char *HqlHelpText::Get(const char *subject) {
  HelpTextMapT::const_iterator iter = textMap.find(subject);
  if (iter == textMap.end())
    return 0;
  return (*iter).second;
}


const char *HqlHelpText::Get(std::string &subject) {
  HelpTextMapT::const_iterator iter = textMap.find(subject);
  if (iter == textMap.end())
    return 0;
  return (*iter).second;
}


