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

  const char *help_text_contents[] = {
    "",
    "CREATE TABLE ....... Creates a table",
    "DELETE ............. Deletes all or part of a row from a table",
    "DESCRIBE TABLE ..... Displays a table's schema",
    "DROP TABLE ......... Removes a table",
    "INSERT ............. Inserts data into a table",
    "LOAD DATA INFILE ... Loads data from a tab delimited input file into a table",
    "SELECT ............. Selects (and display) cells from a table",
    "SHOW CREATE TABLE .. Displays CREATE TABLE command used to create table",
    "SHOW TABLES ........ Displays the list of tables",
    "",
    "Statements must be terminated with ';' to execute.  For more information on",
    "a specific statement, type 'help <statement>', where <statement> is one from",
    "the preceeding list.",
    "",
    (const char *)0
  };

  const char *help_text_rsclient_contents[] = {
    "",
    "CREATE SCANNER .... Creates a scanner and displays first block of results",
    "DESTROY SCANNER ... Destroys a scanner",
    "DROP RANGE ........ Drop a range",
    "FETCH SCANBLOCK ... Fetch the next block results of a scan",
    "LOAD RANGE ........ Load a range",
    "REPLAY START ...... Start replay",
    "REPLAY LOG ........ Replay a commit log",
    "REPLAY COMMIT ..... Commit replay",
    "SHUTDOWN   ........ Shutdown the RangeServer",
    "UPDATE ............ Selects (and display) cells from a table",
    "",
    "Statements must be terminated with ';' to execute.  For more information on",
    "a specific statement, type 'help <statement>', where <statement> is one from",
    "the preceeding list.",
    "",
    (const char *)0
  };

  const char *help_text_create_scanner[] = {
    "",
    "CREATE SCANNER ON range_spec",
    "    [where_clause]",
    "    [options_spec]",
    "",
    "range_spec:",
    "    table_name '[' [start_row] \"..\" ( end_row | ?? ) ']'",
    "",
    "where_clause:",
    "    WHERE where_predicate [ && where_predicate ... ] ",
    "",
    "where_predicate: ",
    "    ROW = row_key",
    "    | ROW > row_key",
    "    | ROW >= row_key",
    "    | ROW < row_key",
    "    | ROW <= row_key",
    "    | ROW STARTS WITH substr",
    "    | TIMESTAMP >  timestamp",
    "    | TIMESTAMP >= timestamp",
    "    | TIMESTAMP <  timestamp",
    "    | TIMESTAMP <= timestamp",
    "",
    "options_spec:",
    "    ( MAX_VERSIONS = version_count",
    "    | LIMIT = row_count",
    "    | INTO FILE 'file_name'",
    "    | DISPLAY_TIMESTAMPS",
    "    | RETURN_DELETES",
    "    | KEYS_ONLY )*",
    "",
    "timestamp:",
    "    'YYYY-MM-DD HH:MM:SS[.nanoseconds]'",
    "",
    "NOTES:  If the start_row is absent from a range_spec, it means NULL or",
    "the beginning of the range.  If the end_row is specified as ??, then it",
    "will get converted to 0xff 0xff which indicates the end of the range.",
    "",
    "Example:",
    "",
    "  CREATE SCANNER ON Test[..??]",
    "",
    (const char *)0
  };

  const char *help_text_destroy_scanner[] = {
    "",
    "DESTROY SCANNER [scanner_id]",
    "",
    "This command will destroy a scanner previously created with",
    "a CREATE SCANNER command.  If a scanner_id is supplied, then",
    "the scanner corresponding to that ID will be destroyed, otherwise",
    "the \"current\" or most recently created scanner will get destroyed.",
    "",
    (const char *)0
  };

  const char *help_text_fetch_scanblock[] = {
    "",
    "FETCH SCANBLOCK [scanner_id]",
    "",
    "This command will fetch and display the next block of results",
    "of a scanner.  If a scanner_id is supplied, then the scanner",
    "corresponding to that ID will be destroyed, otherwise the",
    "\"current\" or most recently created scanner will get destroyed.",
    "",
    (const char *)0
  };

  const char *help_text_load_range[] = {
    "",
    "LOAD RANGE range_spec",
    "",
    "range_spec:",
    "    table_name '[' [start_row] \"..\" ( end_row | ?? ) ']'",
    "",
    "This command will issue a 'load range' command to the RangeServer",
    "for the range specified with range_spec.",
    "",
    (const char *)0
  };

  const char *help_text_update[] = {
    "",
    "UPDATE table_name input_file",
    "",
    "This command will read blocks of key/value pairs from input_file",
    "and send them to the range server.  Here are some example input file",
    "lines that illustrate the format of this file:",
    "",
    "1189631331826108        acaleph DELETE",
    "1189631331826202        acrostolion     apple:http://www.baseball.com/       Vilia miretur vulgus",
    "1189631331826211        acerin  banana:http://sports.espn.go.com/       DELETE",
    "",
    "The fields are separated by the tab character.  The lines have the following format:",
    ""
    "<timestamp> '\t' <row-key> '\t' <column-family>[:<column-qualfier>] '\t' <value>",
    "",
    "The string \"DELETE\" has special meaning.  In the first example line",
    "above, it generates a 'delete row' for the row key 'acaleph'.  In the",
    "third example line above, it generates a 'delete cell' for the column",
    "'banana:http://sports.espn.go.com/' of row 'acerin'.",
    "",
    (const char *)0
  };    

  const char *help_text_shutdown[] = {
    "",
    "SHUTDOWN",
    "",
    "This command causes the RangeServer to shutdown.  It will",
    "return immediately, but the RangeServer will wait for all",
    "running requests to complete before shutting down.",
    "",
    (const char *)0
  };

  const char *help_text_drop_range[] = {
    "",
    "DROP RANGE range_spec",
    "",
    "range_spec:",
    "    table_name '[' [start_row] \"..\" ( end_row | ?? ) ']'",
    "",
    "This command will issue a 'drop range' command to the RangeServer",
    "for the range specified with range_spec.",
    "",
    (const char *)0
  };

  const char *help_text_replay_start[] = {
    "",
    "REPLAY START",
    "",
    "This command will issue a 'replay start' command to the RangeServer.",
    "This has the effect of clearing the replay table/range map, droppin any",
    "existing replay commit log and creating a new one."
    "",
    (const char *)0
  };

  const char *help_text_replay_log[] = {
    "",
    "REPLAY LOG",
    "",
    "This command will issue a 'replay log' command to the RangeServer.",
    "[...]",
    "",
    (const char *)0
  };

  const char *help_text_replay_commit[] = {
    "",
    "REPLAY COMMIT",
    "",
    "This command will issue a 'replay commit' command to the RangeServer.",
    "This has the effect of atomically merging the 'replay' range map into the",
    "'live' range map and linking the replay log into the main log.",
    "",
    (const char *)0
  };


  const char *help_text_create_table[] = {
    "",
    "CREATE TABLE [table_option ...] name '(' [create_definition, ...] ')'",
    "",
    "table_option:",
    "    COMPRESSOR '=' string_literal",
    "",
    "create_definition:",
    "    column_family_name [MAX_VERSIONS '=' value] [TTL '=' duration]",
    "    | ACCESS GROUP name [access_group_option ...] ['(' [column_family_name, ...] ')']",
    "",
    "duration:",
    "    num MONTHS",
    "    | num WEEKS",
    "    | num DAYS",
    "    | num HOURS",
    "    | num MINUTES",
    "    | num SECONDS",
    "",
    "access_group_option:",
    "    IN_MEMORY",
    "    | BLOCKSIZE '=' value",
    "    | COMPRESSOR '=' string_literal",
    "",
    (const char *)0
  };

  const char *help_text_select[] = {
    "",
    "SELECT ( '*' | column_family_name [ ',' column_family_name]* )",
    "    FROM table_name",
    "    [where_clause]",
    "    [options_spec]",
    "",
    "where_clause:",
    "    WHERE where_predicate [ && where_predicate ... ] ",
    "",
    "where_predicate: ",
    "    ROW = row_key",
    "    | ROW > row_key",
    "    | ROW >= row_key",
    "    | ROW < row_key",
    "    | ROW <= row_key",
    "    | ROW STARTS WITH substr",
    "    | TIMESTAMP >  timestamp",
    "    | TIMESTAMP >= timestamp",
    "    | TIMESTAMP <  timestamp",
    "    | TIMESTAMP <= timestamp",
    "",
    "options_spec:",
    "    ( MAX_VERSIONS = version_count",
    "    | LIMIT = row_count",
    "    | INTO FILE 'file_name'",
    "    | DISPLAY_TIMESTAMPS",
    "    | RETURN_DELETES",
    "    | KEYS_ONLY )*",
    "",
    "timestamp:",
    "    'YYYY-MM-DD HH:MM:SS[.nanoseconds]'",
    "",
    (const char *)0
  };
    
  const char *help_text_describe_table[] = {
    "",
    "DESCRIBE TABLE name",
    "",
    "Example:",
    "",
    "hypertable> describe table Test1;",
    "",
    "<Schema generation=\"1\">",
    "  <AccessGroup name=\"jan\">",
    "    <ColumnFamily id=\"4\">",
    "      <Name>cherry</Name>",
    "    </ColumnFamily>",
    "  </AccessGroup>",
    "  <AccessGroup name=\"default\">",
    "    <ColumnFamily id=\"5\">",
    "      <Name>banana</Name>",
    "    </ColumnFamily>",
    "    <ColumnFamily id=\"6\">",
    "      <Name>apple</Name>",
    "    </ColumnFamily>",
    "  </AccessGroup>",
    "  <AccessGroup name=\"marsha\">",
    "    <ColumnFamily id=\"7\">",
    "      <Name>onion</Name>",
    "    </ColumnFamily>",
    "    <ColumnFamily id=\"8\">",
    "      <Name>cassis</Name>",
    "    </ColumnFamily>",
    "  </AccessGroup>",
    "</Schema>",
    "",
    (const char *)0
  };

  const char *help_text_show_create_table[] = {
  "",
    "SHOW CREATE TABLE name",
    "",
    "Example:",
    "",
    "hypertable> show create table Test1;",
    "",
    "CREATE TABLE Test1 (",
    "  banana,",
    "  apple,",
    "  cherry,",
    "  onion,",
    "  cassis,",
    "  ACCESS GROUP jan ( cherry ),",
    "  ACCESS GROUP default ( banana apple ),",
    "  ACCESS GROUP marsha ( onion cassis )",
    ")",
    "",
    (const char *)0
  };

  const char *help_text_load_data_infile[] = {
    "",
    "LOAD DATA INFILE [options] fname INTO TABLE name",
    "",
    "LOAD DATA INFILE [options] fname INTO FILE fname",
    "",
    "options:",
    "",
    "  ( ROW_KEY_COLUMN=name | TIMESTAMP_COLUMN=name )*",
    "",
    "The following is an example of the timestamp format that is",
    "expected in the timestamp column:",
    "",
    "  2008-01-09 09:46:51",
    "",
    "Example:",
    "",
    "hypertable> load data infile \"data.txt\" into table Test1;",
    "",
    "Loading 238,920,253 bytes of input data...",
    "",
    "0%   10   20   30   40   50   60   70   80   90   100%",
    "|----|----|----|----|----|----|----|----|----|----|",
    "***************************************************",
    "Load complete (59.70s elapsed_time, 4001802.20 bytes/s, 33499.06 inserts/s)",
    "",
    "The second form (INTO FILE) is really for testing purposes.  It allows",
    "you to convert your .tsv file into a load file that contains one",
    "insert per line which can be helpful for debugging." \
    "",
    (const char *)0
  };
    
  const char *help_text_insert[] = {
    "",
    "INSERT INTO table_name VALUES value_list",
    "",
    "value_list:",
    "    value_spec [ ',' value_spec ... ]",
    "",
    "value_spec:",
    "    '(' row_key ',' column_key ',' value ')'",
    "    '(' timestamp ',' row_key ',' column_key ',' value ')'",
    "",
    "timestamp:",
    "    'YYYY-MM-DD HH:MM:SS[.nanoseconds]'",
    "",
    (const char *)0
  };
    
  const char *help_text_delete[] = {
    "",
    "DELETE Syntax",
    "",
    "  DELETE ( '*' | column_key [ ',' column_key ...] )",
    "    FROM table_name",
    "    WHERE ROW '=' row_key",
    "    [ TIMESTAMP timestamp ]",
    "",
    "  column_key:",
    "    column_family [ ':' column_qualifier ]",
    "",
    "  timestamp:",
    "    'YYYY-MM-DD HH:MM:SS[.nanoseconds]'",
    "",
    "The DELETE command provides a way to delete cells from a row in a table.",
    "The command applies to a single row only and can be used to delete, for",
    "a given row, all of the cells in a qualified column, all the cells in a",
    "column family, or all of the cells in the row.  An example of each",
    "type of delete is shown below.  Assume that we're starting with a table",
    "that contains the following:",
    "",
    "hypertable> SELECT * FROM CrawlDb;",
    "2008-01-28 11:42:27.550602004   org.hypertable.www      status-code     200",
    "2008-01-28 11:42:27.534581004   org.hypertable.www      status-code     200",
    "2008-01-28 11:42:27.550602002   org.hypertable.www      anchor:http://www.news.com/     Hypertable",
    "2008-01-28 11:42:27.534581002   org.hypertable.www      anchor:http://www.news.com/     Hypertable",
    "2008-01-28 11:42:27.550602001   org.hypertable.www      anchor:http://www.opensource.org/       Hypertable.org",
    "2008-01-28 11:42:27.534581001   org.hypertable.www      anchor:http://www.opensource.org/       Hypertable.org",
    "2008-01-28 11:42:27.550602003   org.hypertable.www      checksum        822828699",
    "2008-01-28 11:42:27.534581003   org.hypertable.www      checksum        2921728",
    "",
    "The first example shows how to delete the cells in the column",
    "'anchor:http://www.opensource.org/' of the row 'org.hypertable.www'.",
    "",
    "hypertable> DELETE \"anchor:http://www.opensource.org/\" FROM CrawlDb WHERE ROW = 'org.hypertable.www';",
    "",
    "hypertable> select * from CrawlDb;",
    "2008-01-28 11:42:27.550602004   org.hypertable.www      status-code     200",
    "2008-01-28 11:42:27.534581004   org.hypertable.www      status-code     200",
    "2008-01-28 11:42:27.550602002   org.hypertable.www      anchor:http://www.news.com/     Hypertable",
    "2008-01-28 11:42:27.534581002   org.hypertable.www      anchor:http://www.news.com/     Hypertable",
    "2008-01-28 11:42:27.550602003   org.hypertable.www      checksum        822828699",
    "2008-01-28 11:42:27.534581003   org.hypertable.www      checksum        2921728",
    "",
    "The next example shows how to delete all of the cells in the column",
    "family 'checksum' of the row 'org.hypertable.www'.",
    "",
    "hypertable> DELETE checksum FROM CrawlDb WHERE ROW = \"org.hypertable.www\";",
    "",
    "hypertable> select * from CrawlDb;",
    "2008-01-28 11:42:27.550602004   org.hypertable.www      status-code     200",
    "2008-01-28 11:42:27.534581004   org.hypertable.www      status-code     200",
    "2008-01-28 11:42:27.550602002   org.hypertable.www      anchor:http://www.news.com/     Hypertable",
    "2008-01-28 11:42:27.534581002   org.hypertable.www      anchor:http://www.news.com/     Hypertable",
    "",
    "And finaly, here's how to delete all of the cells in the row 'org.hypertable.www'.",
    "",
    "hypertable> DELETE * FROM CrawlDb WHERE ROW = \"org.hypertable.www\";",
    "",
    "hypertable> SELECT * FROM CrawlDb;",
    "",
    (const char *)0
  };

  const char *help_text_show_tables[] = {
    "",
    "SHOW TABLES",
    "",
    "Example:",
    "",
    "hypertable> show tables;",
    "",
    "CrawlDb",
    "Query-Log",
    "",
    (const char *)0
  };

  const char *help_text_drop_table[] = {
    "",
    "DROP TABLE [IF EXISTS] name",
    "",
    "Removes the table 'name' from the system.  If the IF EXIST clause is given,",
    "then the command will succeed (i.e. won't generate an error) if a table",
    "by the name of 'name' does not exist.",
    "",
    (const char *)0
  };


  typedef __gnu_cxx::hash_map<std::string, const char **>  HelpTextMapT;

  HelpTextMapT &buildHelpTextMap() {
    HelpTextMapT *map = new HelpTextMapT();
    (*map)[""] = help_text_contents;
    (*map)["contents"] = help_text_contents;
    (*map)["create table"] = help_text_create_table;
    (*map)["delete"] = help_text_delete;
    (*map)["insert"] = help_text_insert;
    (*map)["select"] = help_text_select;
    (*map)["describe table"] = help_text_describe_table;
    (*map)["describe"] = help_text_describe_table;
    (*map)["show create table"] = help_text_show_create_table;
    (*map)["show create"] = help_text_show_create_table;
    (*map)["load data infile"] = help_text_load_data_infile;
    (*map)["load data"] = help_text_load_data_infile;
    (*map)["load"] = help_text_load_data_infile;
    (*map)["show tables"] = help_text_show_tables;
    (*map)["drop"] = help_text_drop_table;
    (*map)["drop table"] = help_text_drop_table;
    return *map;
  }

  HelpTextMapT &textMap = buildHelpTextMap();
}


const char **HqlHelpText::Get(const char *subject) {
  HelpTextMapT::const_iterator iter = textMap.find(subject);
  if (iter == textMap.end())
    return 0;
  return (*iter).second;
}


const char **HqlHelpText::Get(std::string &subject) {
  HelpTextMapT::const_iterator iter = textMap.find(subject);
  if (iter == textMap.end())
    return 0;
  return (*iter).second;
}


void HqlHelpText::install_range_server_client_text() {
  textMap[""] = help_text_rsclient_contents;
  textMap["contents"] = help_text_rsclient_contents;
  textMap["select"] = help_text_select;
  textMap["create"] = help_text_create_scanner;
  textMap["create scanner"] = help_text_create_scanner;
  textMap["destroy"] = help_text_destroy_scanner;
  textMap["destroy scanner"] = help_text_destroy_scanner;
  textMap["drop range"] = help_text_drop_range;
  textMap["fetch"] = help_text_fetch_scanblock;
  textMap["fetch scanblock"] = help_text_fetch_scanblock;
  textMap["load"] = help_text_load_range;
  textMap["load range"] = help_text_load_range;
  textMap["update"] = help_text_update;
  textMap["replay start"] = help_text_replay_start;
  textMap["replay log"] = help_text_replay_log;
  textMap["replay commit"] = help_text_replay_commit;
  textMap["shutdown"] = help_text_shutdown;
}
