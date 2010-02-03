HQL Statement Syntax
--------------------

The Hypertable Query Language (HQL) allows you to create, modify, and query
tables and invoke adminstrative commands.  HQL is interpreted by the following
interfaces:

  * The `hypertable` command line interface (CLI)
  * The `HqlService.hql_exec()` Thrift API method
  * The `Hypertable::HqlInterpreter` C++ class

[`ALTER TABLE`](alter-table.html) - Add and remove columns from a table

[`CREATE TABLE`](create-table.html) - Create a table

[`DELETE`](delete.html) - Delete a row or columns from a specific row of a
table

[`DESCRIBE TABLE`](describe-table.html) - Show the table schema

[`DROP TABLE`](drop-table.html) - Remove a table

[`DUMP TABLE`](dump-table.html) - Create efficient backup

[`INSERT`](insert.html) - Insert data into a table

[`LOAD DATA INFILE`](load-data-infile.html) - Bulk insert data into a table

[`SELECT`](select.html) - Query a table

[`SHOW CREATE TABLE`](show-create-table.html) - Show the `CREATE TABLE` command
used to create a table

[`SHOW TABLES`](show-tables.html) - Display all existing table names

[`SHUTDOWN`](shutdown.html) - Gracefully shutdown the Hypertable service

