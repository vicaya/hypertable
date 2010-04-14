DUMP TABLE
----------
#### EBNF

    DUMP TABLE table_name
      [where_clause]
      [options_spec]

    where_clause:
        WHERE timestamp_predicate

    relop: '=' | '<' | '<=' | '>' | '>='

    timestamp_predicate:
      [timestamp relop] TIMESTAMP relop timestamp

    options_spec:
      (REVS revision_count
      | INTO FILE [file_location]filename[.gz]
      | BUCKETS n
      | NO_ESCAPE)*
    
    file_location:
      "dfs://" | "file://"

    timestamp:
      'YYYY-MM-DD HH:MM:SS[.nanoseconds]'

#### Description
<p>
The `DUMP TABLE` command provides a way to create efficient table backups
which can be loaded with `LOAD DATA INFILE`.  The problem with using `SELECT`
to create table backups is that it outputs table data in order of row key.
`LOAD DATA INFILE` yields worst-case performance when loading data that is
sorted by the row key because only one RangeServer at a time will be actively
receiving updates.  Backup file generated with `DUMP TABLE` are much more
efficient because the data distribution in the backup file causes many (or all)
of the RangeServers to actively receive updates during the loading process.
The `DUMP TABLE` command will randomly select n ranges and output cells from
those ranges in round-robin fashion.  n is the number of buckets (default is 50)
and can be specified with the `BUCKETS` option.

#### Options
<p>
#### `REVS revision_count`
<p>
Each cell in a Hypertable table can have multiple timestamped revisions.  By
default all revisions of a cell are returned by the `DUMP TABLE` statement.  The
`REVS` option allows control over the number of cell revisions returned.  The
cell revisions are stored in reverse-chronological order, so `REVS 1` will
return the most recent version of the cell.

#### `INTO FILE [file://|dfs://]filename[.gz]`
<p>
The result of a `DUMP TABLE` command is displayed to standard output by default.
The `INTO FILE` option allows the output to get redirected to a file.  
If the file name starts with the location specifier `dfs://` then the output file is 
assumed to reside in the DFS. If it starts with `file://` then output is 
sent to a local file. This is also the default location in the absence of any 
location specifier.
If the file name specified ends in a `.gz` extension, then the output is compressed
with gzip before it is written to the file.

#### `BUCKETS n`
<p>
This option causes the `DUMP TABLE` command to use `n` buckets.  The default is
50.  It is recommended that `n` is at least as large as the number of nodes
in the cluster that the backup with be restored to.

#### `NO_ESCAPE`
<p>
The output format of a `DUMP TABLE` command comprises tab delimited lines, one
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
#### Examples

    DUMP TABLE foo;
    DUMP TABLE foo WHERE '2008-07-28 00:00:02' < TIMESTAMP < '2008-07-28 00:00:07';
    DUMP TABLE foo INTO FILE 'foo.tsv.gz'
    DUMP TABLE foo REVS 1 BUCKETS 1000;
