SELECT CELLS
------
#### EBNF

    SELECT CELLS ('*' | (column_predicate [',' column_predicate]*))
      FROM table_name
      [where_clause]
      [options_spec]

    where_clause:
        WHERE where_predicate [AND where_predicate ...]

    where_predicate:
      cell_predicate
      | row_predicate
      | timestamp_predicate
      | value_predicate

    relop: '=' | '<' | '<=' | '>' | '>=' | '=^'
    
    column_predicate
      = column_family
      | column_family ':' column_qualifer
      | column_family ':' '/' column_qualifier_regexp '/'

    cell_spec: row ',' column

    cell_predicate:
      [cell_spec relop] CELL relop cell_spec
      | '(' [cell_spec relop] CELL relop cell_spec
            (OR [cell_spec relop] CELL relop cell_spec)* ')'

    row_predicate:
      [row_key relop] ROW relop row_key
      | '(' [row_key relop] ROW relop row_key
              (OR [row_key relop] ROW relop row_key)* ')'
      | ROW REGEXP '"'row_regexp'"'

    timestamp_predicate:
      [timestamp relop] TIMESTAMP relop timestamp
    
    value_predicate:
      VALUE REGEXP '"'value_regexp'"' 
    

    options_spec:
      (REVS revision_count
      | LIMIT row_count
      | CELL_LIMIT max_cells_per_cf
      | INTO FILE [file_location]filename[.gz]
      | DISPLAY_TIMESTAMPS
      | KEYS_ONLY
      | NO_ESCAPE
      | RETURN_DELETES)*

    timestamp:
      'YYYY-MM-DD HH:MM:SS[.nanoseconds]'
    
    file_location:
      "dfs://" | "file://"
 
#### Description
<p>
The parser only accepts a single timestamp predicate.  The '=^' operator is the
"starts with" operator.  It will return all rows that have the same prefix as
the operand.

#### Options
<p>
#### `REVS revision_count`
<p>
Each cell in a Hypertable table can have multiple timestamped revisions.  By
default all revisions of a cell are returned by the `SELECT CELLS` statement.  The
`REVS` option allows control over the number of cell revisions returned.  The
cell revisions are stored in reverse-chronological order, so `REVS=1` will
return the most recent version of the cell.

#### `LIMIT row_count`
<p>
Limits the number of rows returned by the `SELECT CELLS` statement to `row_count`.

#### `CELL_LIMIT max_cells_per_cf`
<p>
Limits the number of cells returned per row per column family by the `SELECT CELLS` 
statement to `max_cells_per_cf`.

#### `INTO FILE [file://|dfs://]filename[.gz]`
<p>
The result of a `SELECT CELLS` command is displayed to standard output by default.
The `INTO FILE` option allows the output to get redirected to a file.  
If the file name starts with the location specifier `dfs://` then the output file is 
assumed to reside in the DFS. If it starts with `file://` then output is 
sent to a local file. This is also the default location in the absence of any 
location specifier.
If the file name specified ends in a `.gz` extension, then the output is compressed
with gzip before it is written to the file.  The first line of the output,
when using the `INTO FILE` option, is a header line, which will take one of
the two following formats.  The second format will be output if the
`DISPLAY_TIMESTAMPS` option is supplied.

     #row '\t' column '\t' value

     #timestamp '\t' row '\t' column '\t' value

<p>
#### `DISPLAY_TIMESTAMPS`
<p>
The `SELECT CELLS` command displays one cell per line of output.  Each line contains
three tab delimited fields, row, column, and value.  The `DISPLAY_TIMESTAMPS`
option causes the cell timestamp to be included in the output as well.  When
this option is used, each output line will contain four tab delimited fields
in the following order:

     timestamp, row, column, value
<p>
#### `KEYS_ONLY`
<p>
The `KEYS_ONLY` option suppresses the output of the value.  It is somewhat
efficient because the option is processed by the RangeServers and not by
the client.  The value data is not transferred back to the client, only
the key data.

#### `NO_ESCAPE`
<p>
The output format of a `SELECT CELLS` command comprises tab delimited lines, one
cell per line, which is suitable for input to the `LOAD DATA INFILE`
command.  However, if the value portion of the cell contains either newline
or tab characters, then it will confuse the `LOAD DATA INFILE` input parser.
To prevent this from happening, newline, tab, and backslash characters are
converted into two character escape sequences, described in the following table.

<table border="1">
<tr>
<td>&nbsp;<b>Character</b>&nbsp;</td>
<td>&nbsp;<b>Escape Sequence</b>&nbsp;</td>
</tr>
<tr>
<td>&nbsp;backslash \</td>
<td><pre> '\' '\' </pre></td>
</tr>
<tr>
<td>&nbsp;newline \n&nbsp;</td>
<td><pre> '\' 'n' </pre></td>
</tr>
<tr>
<td>&nbsp;tab \t</td>
<td><pre> '\' 't' </pre></td>
</tr>
</table>
<p>
The `NO_ESCAPE` option turns off this escaping mechanism.
<p>
#### `RETURN_DELETES`
<p>
The `RETURN_DELETES` option is used internally for debugging.  When data is
deleted from a table, the data is not actually deleted right away.  A delete
key will get inserted into the database and the delete will get processed
and applied during subsequent scans.  The `RETURN_DELETES` option will return
the delete keys in addition to the normal cell keys and values.  This option
can be useful when used in conjuction with the `DISPLAY_TIMESTAMPS` option to
understand how the delete mechanism works.

<p>
#### Examples
    SELECT CELLS * FROM test WHERE ('a' <= ROW <= 'e') and
           CELLS                   '2008-07-28 00:00:02' < TIMESTAMP < '2008-07-28 00:00:07';
    SELECT CELLS * FROM test WHERE ROW =^ 'b';
    SELECT CELLS * FROM test WHERE (ROW = 'a' or ROW = 'c' or ROW = 'g');
    SELECT CELLS * FROM test WHERE ('a' < ROW <= 'c' or ROW = 'g' or ROW = 'c');

    SELECT CELLS col1:/^w[^a-zA-Z]*$/ from RegexpTest WHERE ROW REGEXP "^\D+" AND VALUE REGEXP "l.*e";
    SELECT CELLS col1:/^w/, col2:/^[em].*/ from RegexpTest;
