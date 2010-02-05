LOAD DATA INFILE
----------------

#### EBNF

    LOAD DATA INFILE [options] fname INTO TABLE name

    LOAD DATA INFILE [options] fname INTO FILE fname

    options:

      (ROW_KEY_COLUMN '=' column_specifier ['+' column_specifier ...]
       | TIMESTAMP_COLUMN '=' name |
       | HEADER_FILE '=' '"' filename '"'
       | ROW_UNIQUIFY_CHARS '=' n
       | NO_ESCAPE
       | IGNORE_UNKNOWN_CFS
       | SINGLE_CELL_FORMAT
       | RETURN_DELETES)*

    column_specifier =
      [ column_format ] column_name

    column_format
      "%0" int
      | "%-"
      | "%"

#### Description
<p>
The `LOAD DATA INFILE` command provides a way to bulk load data from an
optionally compressed file or stdin (fname of "-"), into a table.  The input
is assumed to start with a header line that indicates the format of the lines
in the file.  The header can optionlly be stored in a separate file and
referenced with the `HEADER_FILE` option.  The header is expected to have the
following format:

    header =
      [ '#' ] single_cell_format
      | [ '#' ] multi_cell_format

    single_cell_format =
      "row" '\t' "column" '\t' "value" '\n'
      | "timestamp" '\t' "row" '\t' "column" '\t' "value" '\n'

    multi_cell_format =
      column | string ( '\t' ( column | string ) )*

    column = column_family [ ':' column_qualifier ]

Two basic tab-delimited formats are supported, a single cell format in which
each line contains a single cell, and a multi-cell format in which each line
can contain a list of cells.  The following example shows the single-cell
format:

##### Example 1

    row     column  value
    1127071 query   guardianship
    1127071 item:rank       8
    1127071 click_url       http://adopting.adoption.com
    1246036 query   polish american priests association
    1246036 item:rank       6
    1246036 click_url       http://www.palichicago.org
    12653   query   lowes
    12653   item:rank       1
    12653   click_url       http://www.lowes.com
    1270972 query   head hunters
    1270972 item:rank       2
    1270972 click_url       http://www.headhunters.com
    2648672 query   jamie farr
    2648672 item:rank       1
    2648672 click_url       http://www.imdb.com
    ...

An optional initial timestamp column can be included which represents the cell
timestamp, for example:

##### Example 2

    timestamp       row     column  value
    2009-08-12 00:01:08     1127071 query   guardianship
    2009-08-12 00:01:08     1127071 item:rank       8
    2009-08-12 00:01:08     1127071 click_url       http://adopting.adoption.com
    2009-08-12 00:01:18     1246036 query   polish american priests association
    2009-08-12 00:01:18     1246036 item:rank       6
    2009-08-12 00:01:18     1246036 click_url       http://www.palichicago.org
    2009-08-12 00:01:14     12653   query   lowes
    2009-08-12 00:01:14     12653   item:rank       1
    2009-08-12 00:01:14     12653   click_url       http://www.lowes.com
    2009-08-12 00:01:10     1270972 query   head hunters
    2009-08-12 00:01:10     1270972 item:rank       2
    2009-08-12 00:01:10     1270972 click_url       http://www.headhunters.com
    2009-08-12 00:01:17     2648672 query   jamie farr
    2009-08-12 00:01:17     2648672 item:rank       1
    2009-08-12 00:01:17     2648672 click_url       http://www.imdb.com
    ...

The multi-line format assumes that each tab delimited field represents a cell
value and the column header specifies the name of the column.  Unless otherwise
specified, the first column is assumed to be the rowkey.  For example:

##### Example 3

    anon_id	    query	    item:rank	click_url
    3613173	    batman signal images	18	http://www.icomania.com
    1127071	    guardianship  8		http://adopting.adoption.com
    1270972	    head hunters  2		http://www.headhunters.com
    465778	    google	  1		http://www.google.com
    12653	    lowes	  1		http://www.lowes.com
    48785	    address locator		2	http://www.usps.com/ncsc/
    48785	    address locator		3	http://factfinder.census.gov
    2648672	    jamie farr			1	http://www.imdb.com
    1246036	    polish american	6	http://www.palichicago.org
    605089	    dachshunds for sale	2	http://www.houstonzone.org
    760038	    stds       1   http://www.ashastd.org

When loaded into a table with a straight `LOAD DATA INFILE` command, the above
file will produce a set of cells equivalent to Example 1 above.

#### Options
<p>
#### `ROW_KEY_COLUMN = column_specifier [ + column_specifier ... ]`
<p>
The `LOAD DATA INFILE` command accepts a number of options.  The first is the
`ROW_KEY_COLUMN` option.  This is used in conjunction with the multi-cell
input file format.  It provides a way to select which column in the input
file should be used as the row key.  By separating two or more column names
with the '+' character, multiple column values will be concatenated together,
separated by a single space character to form the row key.  Also, each
column specifier can have one of the following prefixes to control field width
and justification:

##### Table 1

<table border="1">
<tr>
<th>Prefix</th>
<th>Description</th>
</tr>
<tr>
<td><pre> %0&lt;n&gt; </pre></td>
<td>For numeric columns, specifies a field width of &lt;n&gt;
and right-justify with '0' padding</td>
</tr>
<tr>
<td><pre> %-&lt;n&gt; </pre></td>
<td>Specifies a field width of &lt;n&gt; and right-justification
with ' ' (space) padding</td>
</tr>
<td><pre> %&lt;n&gt; </pre></td>
<td>Specifies a field width of &lt;n&gt; and left-justification
with ' ' (space) padding</td>
</tr>
</table>
<p>
For example, assuming the data in Example 3 above is contained in a filed
named "query-log.tsv", then the following `LOAD DATA INFILE` command:

    LOAD DATA INFILE ROW_KEY_COLUMN="%09anon_id"+query "query-log.tsv" INTO TABLE 'anon-id-query';

will populated the 'anon-id-query' table with the following content:

    000012653 lowes	     item:rank		1
    000012653 lowes	     click_url		http://www.lowes.com
    000048785 address locator		item:rank	3
    000048785 address locator		item:rank	2
    000048785 address locator		click_url	http://factfinder.census.gov
    000048785 address locator		click_url	http://www.usps.com/ncsc/
    000465778 google  item:rank		1
    000465778 google  click_url		http://www.google.com
    000605089 dachshunds for sale		item:rank	2
    000605089 dachshunds for sale		click_url	http://www.houstonzone.org
    000760038 stds	     item:rank		1
    000760038 stds	     click_url		http://www.ashastd.org
    001127071 guardianship			item:rank	8
    001127071 guardianship			click_url	http://adopting.adoption.com
    001246036 polish american		item:rank	6
    001246036 polish american		click_url	http://www.palichicago.org
    001270972 head hunters			item:rank	2
    001270972 head hunters			click_url	http://www.headhunters.com
    002648672 jamie farr			item:rank	1
    002648672 jamie farr			click_url	http://www.imdb.com
    003613173 batman signal images		item:rank	18
    003613173 batman signal images		click_url	http://www.icomania.com

#### `TIMESTAMP_COLUMN = column_name`
<p>
The `TIMESTAMP_COLUMN` option is used in conjunction with the multi-cell input
file format to specify which field of the input file should be used as the
timestamp.  The timestamp extracted from this field will be used for each cell
in the row.  The timestamp field is assumed to have the format `YYYY-MM-DD HH:MM:SS`

#### `HEADER_FILE = "filename"`
<p>
The `HEADER_FILE` option is used to specify an alternate file that contains
the header line for the data file.  This is useful in situations where you have
log files that roll periodically and/or you want to be able to concatenate
them.  This option allows the input files to just contain data and the header
to be specified in a separate file.

#### `ROW_UNIQUIFY_CHARS = n`
<p>
The `ROW_UNIQUIFY_CHARS` option provides a way to append a random string of
characters to the end of the row keys to ensure that they are unique.  The
maximum number of characters you can specify is 21 and each character
represents 6 random bits.  It is useful in situations where the row key isn't
discriminating enough to cause each input line to wind up in its own row.
For example, let's say you want to dump a server log into a table, using the
timestamp as the row key.  However, as in the case of an Apache log, the
timestamp usually only has resolution down to the second and there may be
many entries that fall within the same second.

#### `NO_ESCAPE`
<p>
The `NO_ESCAPE` option provides a way to disable the escaping mechanism.
The newline and tab characters are escaped and unescaped when transferred in
and out of the system.  The `LOAD DATA INFILE` command will scan the input
for the two character sequence '\' 'n' and will convert it into a newline
character.  It will also scan the input for the two character sequence
'\' 't' and will convert it into a tab character.  The `NO_ESCAPE` option
disables this conversion.

#### `IGNORE_UNKNOWN_CFS`
<p>
Ignore unknown (non-existent) column families in the LOAD DATA INFILE input
and insert other cells 

#### `SINGLE_CELL_FORMAT`
<p>
The `LOAD DATA INFILE` command will attempt to detect the format of the input
file by parsing the first line if it begins with a '#' character.  It assumes
that the remainder of the line is a tab-delimited list of column names.
However, if the file format is the 3-field single-cell format
(i.e. row,column,value) and lacks a header, and the first character of the
row key in first line happens to be the '#' character, the parser will get
confused and interpret the first cell as the format line.  The
`SINGLE_CELL_FORMAT` option provides a way to tell the interpreter that there
is no header format line and the format of the load file is one of the single
cell formats (e.g. row,column,value or timestamp,row,column,value) and have
it determine the format by counting the tabs on the first line.

#### `RETURN_DELETES`
<p>
The RETURN_DELETES option is used internally for debugging.  When data is
deleted from a table, the data is not actually deleted right away.  A delete
key will get inserted into the database and the delete will get processed
and applied during subsequent scans.  The RETURN_DELETES option will return
the delete keys in addition to the normal cell keys and values.  This option
can be useful when used in conjuction with the DISPLAY_TIMESTAMPS option to
understand how the delete mechanism works.

#### Compression
<p>
If the name of the input file ends with a ".gz", the file is assumed to be
compressed and will be streamed in through a decompressor (gzip).
