ALTER TABLE
-----------
#### EBNF

    ALTER TABLE name alter_mode '(' [alter_definition] ')'
        [alter_mode '(' alter_definition ')' ... ]

    alter_mode:
      ADD
      | DROP
      | RENAME COLUMN FAMILY 

    alter_definition:
      add_cf_definition
      | drop_cf_definition
      | rename_cf_definition

    add_cf_definition:
      column_family_name [MAX_VERSIONS '=' int] [TTL '=' duration]
      | ACCESS GROUP name [access_group_option ...]
        ['(' [column_family_name, ...] ')']
    
    drop_cf_definition:    column_family_name
    
    rename_cf_definition:    (old)column_family_name, (new)column_family_name

    duration:
      num MONTHS
      | num WEEKS
      | num DAYS
      | num HOURS
      | num MINUTES
      | num [ SECONDS ]

    access_group_option:
      IN_MEMORY
      | BLOCKSIZE '=' int
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

#### Description
<p>
The `ALTER TABLE` command provides a way to alter a table by adding access
groups and column families or removing column families or renaming column families.  See
[`CREATE TABLE`](create-table.html) for a description of the column family
and access group options.  Column families that are not explicitly
included in an access group specification will go into the "default"
access group.

#### Example
<p>
The following statements:

    CREATE TABLE foo (
      a MAX_VERSIONS=1,
      b TTL=1 DAY,
      c,
      ACCESS GROUP primary BLOCKSIZE=1024 ( a ),
      ACCESS GROUP secondary COMPRESSOR="zlib --best" ( b, c )
    );

    ALTER TABLE foo
      ADD ( d MAX_VERSIONS=2 )
      ADD ( ACCESS GROUP tertiary BLOOMFILTER="rows --false-positive 0.1" (d))
      DROP ( c ) 
      RENAME COLUMN FAMILY (a, e); 

will produce the following output with `SHOW CREATE TABLE foo;` ...

    CREATE TABLE foo (
      e MAX_VERSIONS=1,
      b TTL=86400,
      d MAX_VERSIONS=2,
      ACCESS GROUP primary BLOCKSIZE=1024 (e),
      ACCESS GROUP secondary COMPRESSOR="zlib --best" (b),
      ACCESS GROUP tertiary BLOOMFILTER="rows --false-positive 0.1" (d),
    )

























