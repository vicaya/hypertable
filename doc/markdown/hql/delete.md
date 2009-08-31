DELETE
------
#### EBNF
    DELETE ('*' | column [',' column ...])
      FROM table_name
      WHERE ROW '=' row_key
      [TIMESTAMP timestamp]

    column:
      column_family [':' column_qualifier]

    timestamp:
      YYYY-MM-DD HH:MM:SS[.nanoseconds]
#### Description
<p>
The `DELETE` command provides a way to delete cells from a row in a table.  The
command applies to a single row only and can be used to delete, for a given
row, all of the cells in a qualified column, all the cells in a column family,
or all of the cells in the row.  If the `TIMESTAMP` clause is given, then the
delete will only apply to those cells whose internal timestamp field is equal
to or less than the given timestamp.  An example of each type of delete is
shown below.  Assume that we're starting with a table that contains the
following:
<pre>
hypertable> SELECT * FROM crawldb;
org.hypertable.www      status-code     200
org.hypertable.www      status-code     200
org.hypertable.www      anchor:http://www.news.com/     Hypertable
org.hypertable.www      anchor:http://www.news.com/     Hypertable
org.hypertable.www      anchor:http://www.opensource.org/       Hypertable.org
org.hypertable.www      anchor:http://www.opensource.org/       Hypertable.org
org.hypertable.www      checksum        822828699
org.hypertable.www      checksum        2921728
</pre>
<p>
The first example shows how to delete the cells in the column
`anchor:http://www.opensource.org/` of the row `org.hypertable.www`.
<pre>
hypertable> DELETE "anchor:http://www.opensource.org/" FROM crawldb WHERE
ROW='org.hypertable.www';

hypertable> select * from crawldb;
org.hypertable.www      status-code     200
org.hypertable.www      status-code     200
org.hypertable.www      anchor:http://www.news.com/     Hypertable
org.hypertable.www      anchor:http://www.news.com/     Hypertable
org.hypertable.www      checksum        822828699
org.hypertable.www      checksum        2921728
</pre>
<p>
The next example shows how to delete all of the cells in the column family
`checksum` of the row `org.hypertable.www`.
<pre>
hypertable> DELETE checksum FROM crawldb WHERE ROW="org.hypertable.www";

hypertable> select * from crawldb;
org.hypertable.www      status-code     200
org.hypertable.www      status-code     200
org.hypertable.www      anchor:http://www.news.com/     Hypertable
org.hypertable.www      anchor:http://www.news.com/     Hypertable
</pre>
<p>
And finally, here's how to delete all of the cells in the row
`org.hypertable.www`.
<pre>
hypertable> DELETE * FROM crawldb WHERE ROW="org.hypertable.www";

hypertable> SELECT * FROM crawldb;
</pre>
