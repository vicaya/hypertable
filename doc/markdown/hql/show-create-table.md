SHOW CREATE TABLE
-----------------
#### EBNF

    SHOW CREATE TABLE table_name

#### Description
<p>
The `SHOW CREATE TABLE` command shows the CREATE TABLE statement that creates
the given table.

#### Example

    hypertable> SHOW CREATE TABLE foo;
    CREATE TABLE foo (
      a,
      b,
      c,
      ACCESS GROUP bar (a, b),
      ACCESS GROUP default (c)
    )
