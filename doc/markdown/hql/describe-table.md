DESCRIBE TABLE
--------------
#### EBNF

    DESCRIBE TABLE [ WITH IDS ] table_name

#### Description
<p>
The `DESCRIBE TABLE` command displays the XML-style schema for a table.  The
output of the straight `DESCRIBE TABLE` command can be passed into the
Hypertable C++ `Client::create_table()` API as the schema parameter.  If the
optional `WITH IDS` clause is supplied, then the schema "generation" attribute
and the column family "id" attributes are included in the XML output.  For
example, the following table creation statement:

    CREATE TABLE foo (
      a MAX_VERSIONS=1,
      b TTL=1 DAY,
      c,
      ACCESS GROUP primary BLOCKSIZE=1024 ( a, b ),
      ACCESS GROUP secondary compressor="zlib --best" ( c )
    )

will create a table with the following schema as reported by the `CREATE TABLE`
command:

    <Schema>
      <AccessGroup name="primary" blksz="1024">
        <ColumnFamily>
          <Name>a</Name>
          <MaxVersions>1</MaxVersions>
          <deleted>false</deleted>
        </ColumnFamily>
        <ColumnFamily>
          <Name>b</Name>
          <ttl>86400</ttl>
          <deleted>false</deleted>
        </ColumnFamily>
      </AccessGroup>
      <AccessGroup name="secondary" compressor="zlib --best">
        <ColumnFamily>
          <Name>c</Name>
          <deleted>false</deleted>
        </ColumnFamily>
      </AccessGroup>
    </Schema>

and the following output will be generated when the `WITH IDS` clause is
supplied in the `CREATE TABLE` statement:

    <Schema generation="1">
      <AccessGroup name="primary" blksz="1024">
        <ColumnFamily id="1">
          <Generation>1</Generation>
          <Name>a</Name>
          <MaxVersions>1</MaxVersions>
          <deleted>false</deleted>
        </ColumnFamily>
        <ColumnFamily id="2">
          <Generation>1</Generation>
          <Name>b</Name>
          <ttl>86400</ttl>
          <deleted>false</deleted>
        </ColumnFamily>
      </AccessGroup>
      <AccessGroup name="secondary" compressor="zlib --best">
        <ColumnFamily id="3">
          <Generation>1</Generation>
          <Name>c</Name>
          <deleted>false</deleted>
        </ColumnFamily>
      </AccessGroup>
    </Schema>
