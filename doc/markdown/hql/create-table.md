CREATE TABLE
------------
#### EBNF

    CREATE TABLE name '(' [create_definition, ...] ')' [table_option ...]
    CREATE TABLE name LIKE example_name

    create_definition:
      column_family_name [column_family_option ...]
      | ACCESS GROUP name [access_group_option ...]
        ['(' [column_family_name, ...] ')']

    column_family_option:
      MAX_VERSIONS '=' int
      | TTL '=' duration
      | COUNTER

    duration:
      int MONTHS
      | int WEEKS
      | int DAYS
      | int HOURS
      | int MINUTES
      | int [ SECONDS ]

    access_group_option:
      COUNTER
      | IN_MEMORY
      | BLOCKSIZE '=' int
      | REPLICATION '=' int
      | COMPRESSOR '=' compressor_spec
      | BLOOMFILTER '=' bloom_filter_spec

    compressor_spec:
      bmz [ bmz_options ]
      | lzo
      | quicklz
      | zlib [ zlib_options ]
      | none

    bmz_options:
      --fp-len int
      | --offset int

    zlib_options:
      -9
      | --best
      | --normal

    bloom_filter_spec:
      rows [ bloom_filter_options ]
      | rows+cols [ bloom_filter_options ]
      | none

    bloom_filter_options:
      --false-positive float
      --bits-per-item float
      --num-hashes int
      --max-approx-items int

    table_option:
      MAX_VERSIONS '=' int
      | TTL '=' duration
      | IN_MEMORY
      | BLOCKSIZE '=' int
      | REPLICATION '=' int
      | COMPRESSOR '=' compressor_spec
      | GROUP_COMMIT_INTERVAL '=' int

#### Description
<p>
`CREATE TABLE` creates a table with the given name.  A table consists of a set
of column family and access group specifications.
### Column Families
<p>
Column families are somewhat analogous to a traditional database column.  The
main difference is that a theoretically infinite number of qualified columns
can be created within a column family.  The qualifier is an optional NUL-
terminated string that can be supplied, along with the data, in the insert
statement.  This is what gives tables in Hypertable their sparse nature.  For
example, given a column family "tag", the following set of qualified columns
may be inserted for a single row.

 * `tag:good`
 * `tag:science`
 * `tag:authoritative`
 * `tag:green`

The column family is represented internally as a single byte, so there is a
limit of 255 column families (the 0 value is reserved) which may be supplied in
the `CREATE TABLE` statement.
### Access Groups
<p>
Tables consist of one or more access groups, each containing some number of
column families.  There is a default access group named "default" which
contains all column families that are not explicitly referenced in an ACCESS
GROUP clause.  For example, the following two statements are equivalent.
<table border="1">
<tr>
<td valign="top">
<pre><code>
 CREATE TABLE foo (
   a,
   b,
   c,
   ACCESS GROUP bar ( a, b )
 )
</code></pre>
</td valign="top">
<td>
<pre><code>
 CREATE TABLE foo (
   a,
   b,
   c,
   ACCESS GROUP bar (a, b),
   ACCESS GROUP default (c)
 )
</code></pre>
</td>
</tr>
</table>
<p>
Access groups provide control over the physical layout of the table data on
disk.  The data for all column families in the same access group are stored
physically together on disk.  By carefully defining a set of access groups and
choosing which column families go into those access groups, performance can be
significantly improved for expected workloads.  For example, say you have a
table with 100 column families, but two of the column families get access
together with much higher frequency than the rest of the 98 column families.
By putting the two frequently accessed column families in their own access
group, the system does much less disk i/o because only the data for the two
column families gets transfered whenever those column families are accessed.  A
row-oriented database can be emulated by having a single access group.  A
column-oriented database can be emulated by having each column family within
their own access group.
### Table Options
<p>
The following table options are supported:

  * `MAX_VERSIONS '=' int`
  * `TTL '=' duration`
  * `IN_MEMORY`
  * `BLOCKSIZE '=' int`
  * `REPLICATION '=' int`
  * `COMPRESSOR '=' compressor_spec`
  * `GROUP_COMMIT_INTERVAL '=' int`

Most of these are the same options as the ones in the column family and access
group specification except that they act as defaults in the case where no
corresponding option is specified in the column family or access group
specifier.  See the description under Access Group Options for option details.

"group commit" is a feature whereby the system will accumulate update requests
for a table and commit them together as a group on a regular interval.  This
improves the performance of systems that receive a large number of concurrent
updates by reducing the number of times sync() gets called on the commit log.

The `GROUP_COMMIT_INTERVAL` option tells the system that updates to this table
should be carried out with group commit and also specifies the commit interval
in milliseconds.  The interval is constrained by the value of the config property
`Hypertable.RangeServer.CommitInterval`, which acts as a lower bound and defaults
to 50ms.  The value specified for `GROUP_COMMIT_INTERVAL` will get rounded up to
the nearest multiple of this property value.

### Column Family Options
<p>
The following column family options are supported:

  * `MAX_VERSIONS '=' int`
  * `TTL '=' duration`
  * `COUNTER`

Cells in a table are specified by not only a row key and a qualified column,
but also a timestamp.  This allows for essentially multiple timestamped version
s of each cell.  Cells are kept stored in reverse chronological order of
timestamp and the `MAX_VERSIONS` allows you to specify that you only want to
keep the n most recent versions of each cell.  Older versions are lazily
garbage collected through the normal compaction process.

The `TTL` option allows you to specify that you only want to keep cell versions
that fall within some time window in the immediate past.  For example, you can
specify that you only want to keep cells that were created within the past two
weeks.  Like the `MAX_VERSIONS` option, older versions are lazily garbage
collected through the normal compaction process.

The `COUNTER` option makes each instance of this column act as an atomic
counter.  Counter columns are accessed using the same methods as other
columns.  However, to modify the counter, the value must be formatted
specially, as described in the following table.
<p>
<table border="1">
<tr>
<th>Value Format</th>
<th>Description</th>
</tr>
<tr>
<td><pre> ['+'] n </pre></td>
<td> Increment the counter by n </td>
</tr>
<tr>
<td><pre> '-' n </pre></td>
<td> Decrement the counter by n </td>
</tr>
<tr>
<td><pre> '=' n </pre></td>
<td> Reset the counter to n </td>
</tr>
</table>
<p>
For example, consider the following sequence of values written to a counter
column:
<pre>
  +9
  =0
  +3
  +4
  +5
  -2
</pre>

After these six values get written to a counter column, a subsequent read of that
column would return the ASCII string "10".

### Access Group Options
<p>
The following access group options are supported:

  * `COUNTER`
  * `IN_MEMORY`
  * `BLOCKSIZE '=' int`
  * `REPLICATION '=' int`
  * `COMPRESSOR '=' compressor_spec`
  * `BLOOMFILTER '=' bloom_filter_spec`

The `COUNTER` option makes all column families in the access group
counter columns (see `COUNTER` description under Column Family Options
section).

The `IN_MEMORY` option indicates that all cell data for the access group should
remain memory resident.  Queries against column families in IN_MEMORY access
groups are efficient because no disk access is required.

The cell data inserted into an access group resides in one of two places.  The
recently inserted cells are stored in an in-memory data structure called the
cell cache and older cells get compacted into on-disk data structures called
cell stores.  The cell stores are organized as a series of compressed blocks of
sorted key/value pairs (cells).  At the end of the compressed blocks is a block
index which contains, for each block, the key (row,column,timestamp) of the
last cell in the block, followed by the block offset.  It also contains a Bloom
Filter.

The `BLOCKSIZE` option controls the size of the compressed blocks in the cell
stores.  A smaller block size minimizes the amount of data that must be read
from disk and decompressed for a key lookup at the expense of a larger block
index which consumes memory.  The default value for the block size is 65K.

The `REPLICATION` option controls the replication level in the underlying
distributed file system (DFS) for cell store files created for this access
group.  The default is unspecified, which translates to whatever the default
replication level is for the underlying file system.

The `COMPRESSOR` option specifies the compression codec that should be used for
cell store blocks within an access group.  See the Compressors section below
for a description of each compression codec.

<em>NOTE: if the block, after compression, is not significantly reduced in
size, then no compression will be performed on the block</em>

An access group can consist of many on-disk cell stores.  A query for a single
row key can result probing each cell store to see if data is present for that
row even when most of the cell stores do not contain any data for that row.
To eliminate this inefficiency, each cell store contains an optional Bloom
Filter.  The Bloom Filter is a probabilistic data structure that can
indicate, with high probability, if a key is present and also indicate
definitively if a key is not present.  By mapping the bloom filters, for each
cell store in memory, queries can be made much more efficient because only the
cell stores that contain the row are searched.

The bloom filter specification can take one of the following forms.  The `rows`
form, which is the default, causes only row keys to be inserted into the bloom
filter.  The `rows+cols` form causes the row key concatenated with the column
family to be inserted into the bloom filter.  `none` disables the bloom
filter.

  * `rows [ bloom_filter_options ]`
  * `rows+cols [ bloom_filter_options ]`
  * `none`

The following table describes the bloom filter options:

<table border="1">
<tr>
<th>Option</th>
<th>Default</th>
<th>Description</th>
</tr>
<tr>
<td><pre> --false-positive arg </pre></td>
<td><pre> 0.01 </pre></td>
<td>Expected false positive probability.  This option is (currently) mutually
exclusive with the --bits-per-item and --num-hashes options.  If specified
it will choose the minimum number of bits per item that can achieve the given
false positive probability and will choose the appropriate number of hash
functions.</td>
</tr>
<tr>
<td><pre> --bits-per-item arg </pre></td>
<td><pre> [NULL] </pre></td>
<td>Number of bits to use per item (to compute size of bloom filter).  Must
be used in conjunction with --num-hashes.</td>
</tr>
<tr>
<td><pre> --num-hashes arg </pre></td>
<td><pre> [NULL] </pre></td>
<td>Number of hash functions to use.  Must be used in conjunction with
--bits-per-item.</td>
</tr>
<tr>
<td><pre> --max-approx-items arg </pre></td>
<td><pre> 1000 </pre></td>
<td>Number of cell store items used to guess the number of actual Bloom filter
entries</td>
</tr>
</table>
<p>

### Compressors
<p>
The cell store blocks within an access group are compressed using the
compression codec that is specified for the access group.  The following
compression codecs are available:

  * `bmz`
  * `lzo`
  * `quicklz`
  * `zlib`
  * `none`

The default code is `lzo` for cell store blocks.  The following tables describe
the available options.

<table border="1">
<caption><code>bmz</code> codec options</caption>
<tr>
<th>Option</th>
<th>Default</th>
<th>Description</th>
</tr>
<tr>
<td><pre> --fp-len arg </pre></td>
<td><pre> 19 </pre></td>
<td>Minimum fingerprint length</td>
</tr>
<tr>
<td><pre> --offset arg </pre></td>
<td><pre> 0 </pre></td>
<td>Starting fingerprint offset</td>
</tr>
</table>
<p>

<table border="1">
<caption><code>zlib</code> codec options</caption>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
<tr>
<td><pre> -9 [ --best ] </pre></td>
<td>Highest compression ratio (at the cost of speed)</td>
</tr>
<tr>
<td><pre> --normal </pre></td>
<td>Normal compression ratio</td>
</tr>
</table>
<p>
