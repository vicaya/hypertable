/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
    "SHUTDOWN ........... Shuts servers down gracefully",
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
    "    table_name '[' [start_row] \"..\" (end_row | ?? ) ']'",
    "",
    "where_clause:",
    "    WHERE where_predicate [AND where_predicate ...] ",
    "",
    "where_predicate: ",
    "    cell_predicate",
    "    | row_predicate",
    "    | timestamp_predicate",
    "",
    "relop: '=' | '<' | '<=' | '>' | '>=' | '=^'",
    "",
    "cell_spec: row_key ',' qualified_column",
    "",
    "cell_predicate: ",
    "    [cell_spec relop] CELL relop cell_spec",
    "    | '(' [cell_spec relop] CELL relop cell_spec (OR [cell_spec relop] CELL relop cell_spec)* ')'",
    "",
    "row_predicate: ",
    "    [row_key relop] ROW relop row_key",
    "    | '(' [row_key relop] ROW relop row_key (OR [row_key relop] ROW relop row_key)* ')'",
    "",
    "timestamp_predicate: ",
    "    [timestamp relop] TIMESTAMP relop timestamp",
    "",
    "options_spec:",
    "    (REVS = revision_count",
    "    | LIMIT = row_count",
    "    | INTO FILE 'file_name'",
    "    | DISPLAY_TIMESTAMPS",
    "    | RETURN_DELETES",
    "    | KEYS_ONLY)*",
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
    "    table_name '[' [start_row] \"..\" (end_row | ?? ) ']'",
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

  const char *help_text_shutdown_rangeserver[] = {
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
    "    table_name '[' [start_row] \"..\" (end_row | ?? ) ']'",
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
    "SELECT ('*' | column_family_name [',' column_family_name]*)",
    "    FROM table_name",
    "    [where_clause]",
    "    [options_spec]",
    "",
    "where_clause:",
    "    WHERE where_predicate [AND where_predicate ...] ",
    "",
    "where_predicate: ",
    "    cell_predicate",
    "    | row_predicate",
    "    | timestamp_predicate",
    "",
    "relop: '=' | '<' | '<=' | '>' | '>=' | '=^'",
    "",
    "cell_spec: row_key ',' qualified_column",
    "",
    "cell_predicate: ",
    "    [cell_spec relop] CELL relop cell_spec",
    "    | '(' [cell_spec relop] CELL relop cell_spec (OR [cell_spec relop] CELL relop cell_spec)* ')'",
    "",
    "row_predicate: ",
    "    [row_key relop] ROW relop row_key",
    "    | '(' [row_key relop] ROW relop row_key (OR [row_key relop] ROW relop row_key)* ')'",
    "",
    "timestamp_predicate: ",
    "    [timestamp relop] TIMESTAMP relop timestamp",
    "",
    "options_spec:",
    "    (REVS = revision_count",
    "    | LIMIT = row_count",
    "    | INTO FILE 'file_name'",
    "    | DISPLAY_TIMESTAMPS",
    "    | RETURN_DELETES",
    "    | KEYS_ONLY)*",
    "",
    "timestamp:",
    "    'YYYY-MM-DD HH:MM:SS[.nanoseconds]'",
    "",
    "NOTES:",
    "The parser only accepts a single timestamp predicate.  The '=^' operator is the",
    "\"starts with\" operator.  It will return all rows that have the same prefix as the",
    "operand.",
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
    "  ACCESS GROUP jan (cherry),",
    "  ACCESS GROUP default (banana apple),",
    "  ACCESS GROUP marsha (onion cassis)",
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
    "  (ROW_KEY_COLUMN '=' name ['+' name ...] |",
    "    TIMESTAMP_COLUMN '=' name |",
    "    HEADER_FILE '=' '\"' filename '\"' |",
    "    ROW_UNIQIFY_CHARS '=' n)* |",
    "",
    "The following is an example of the timestamp format that is",
    "expected in the timestamp column:",
    "",
    "  2008-01-09 09:46:51",
    "",
    "Example:",
    "",
    "hypertable> load data infile row_key_column=AnonID \"all-no-timestamps.tsv\" into table 'query-log';",
    "",
    "Loading   1,551,351,425 bytes of input data...",
    "",
    "0%   10   20   30   40   50   60   70   80   90   100%",
    "|----|----|----|----|----|----|----|----|----|----|",
    "***************************************************",
    "Load complete.",
    "",
    "  Elapsed time:  247.19 s",
    "Avg value size:  15.23 bytes",
    "  Avg key size:  7.09 bytes",
    "    Throughput:  6276028.06 bytes/s",
    " Total inserts:  75274825",
    "    Throughput:  304526.05 inserts/s",
    "       Resends:  269130",
    "",
    "The second form (INTO FILE) is really for testing purposes.  It allows",
    "you to convert your .tsv file into a load file that contains one",
    "insert per line which can be helpful for debugging.",
    "",
    "The first line in the file 'fname' should contain a tab separated list",
    "of column names which indicates what is contained in the fields of each",
    "line of data.  This header line can optionally start with a comment '#'",
    "character and can also be supplied in a separate file with the HEADER_FILE",
    "option.",
    "",
    "The ROW_UNIQIFY_CHARS option provides a way to append a random string of",
    "characters to the end of the row keys to ensure that they are unique.",
    "The maximum number of characters you can specify is 21 and each character",
    "represents 6 random bits.",
    "",
    (const char *)0
  };

  const char *help_text_insert[] = {
    "",
    "INSERT INTO table_name VALUES value_list",
    "",
    "value_list:",
    "    value_spec [',' value_spec ...]",
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
    "  DELETE ('*' | column_key [',' column_key ...])",
    "    FROM table_name",
    "    WHERE ROW '=' row_key",
    "    [TIMESTAMP timestamp]",
    "",
    "  column_key:",
    "    column_family [':' column_qualifier]",
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

  const char *help_text_shutdown[] = {
    "",
    "SHUTDOWN",
    "",
    "Issues a shutdown request to the Master which, in turn, issues a shutdown",
    "request to all RangeServers.  Upon receiving a shutdown request, each",
    "RangeServer will quiesce, close its commit logs, destroy its Hyperspace",
    "session and then exit.",
    "",
    (const char *)0
  };


  typedef hash_map<std::string, const char **>  HelpTextMap;

  HelpTextMap &build_help_text_map() {
    HelpTextMap *map = new HelpTextMap();
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
    (*map)["shutdown"] = help_text_shutdown;
    return *map;
  }

  HelpTextMap &text_map = build_help_text_map();
}


const char **HqlHelpText::Get(const char *subject) {
  HelpTextMap::const_iterator iter = text_map.find(subject);
  if (iter == text_map.end())
    return 0;
  return (*iter).second;
}


const char **HqlHelpText::Get(std::string &subject) {
  HelpTextMap::const_iterator iter = text_map.find(subject);
  if (iter == text_map.end())
    return 0;
  return (*iter).second;
}


void HqlHelpText::install_range_server_client_text() {
  text_map[""] = help_text_rsclient_contents;
  text_map["contents"] = help_text_rsclient_contents;
  text_map["select"] = help_text_select;
  text_map["create"] = help_text_create_scanner;
  text_map["create scanner"] = help_text_create_scanner;
  text_map["destroy"] = help_text_destroy_scanner;
  text_map["destroy scanner"] = help_text_destroy_scanner;
  text_map["drop range"] = help_text_drop_range;
  text_map["fetch"] = help_text_fetch_scanblock;
  text_map["fetch scanblock"] = help_text_fetch_scanblock;
  text_map["load"] = help_text_load_range;
  text_map["load range"] = help_text_load_range;
  text_map["update"] = help_text_update;
  text_map["replay start"] = help_text_replay_start;
  text_map["replay log"] = help_text_replay_log;
  text_map["replay commit"] = help_text_replay_commit;
  text_map["shutdown"] = help_text_shutdown_rangeserver;
}
