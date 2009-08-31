DROP TABLE
----------
#### EBNF

    DROP TABLE [IF EXISTS] table_name

#### Description
<p>
The `DROP TABLE` command removes the table `table_name` from the system.  If
the `IF EXIST` clause is supplied, the command won't generate an error if a
table by the name of `table_name` does not exist.
