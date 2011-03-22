HQL Statement Syntax
--------------------

The Hypertable Query Language (HQL) allows you to create, modify, and query
tables and invoke adminstrative commands.  HQL is interpreted by the following
interfaces:

  * The `hypertable` command line interface (CLI)
  * The `HqlService.hql_exec()` Thrift API method
  * The `Hypertable::HqlInterpreter` C++ class


[`ALTER TABLE`](alter-table.html) - Add and remove columns from a table

[`CREATE NAMESPACE`](create-namespace.html) - Create a namespace 

[`CREATE TABLE`](create-table.html) - Create a table

[`DELETE`](delete.html) - Delete a row or columns from a specific row of a
table

[`DESCRIBE TABLE`](describe-table.html) - Show the table schema

[`DROP NAMESPACE`](drop-namespace.html) - Drop a namespace 

[`DROP TABLE`](drop-table.html) - Remove a table

[`DUMP TABLE`](dump-table.html) - Create efficient backup

[`GET LISTING`](get-listing.html) - Display all existing namespaces and table names

[`INSERT`](insert.html) - Insert data into a table

[`LOAD DATA INFILE`](load-data-infile.html) - Bulk insert data into a table

[`RENAME TABLE`](rename-table.html) - Rename a table

[`SELECT`](select.html) - Query a table

[`SELECT CELLS`](select-cells.html) - Query a table and filter by value

[`SHOW CREATE TABLE`](show-create-table.html) - Show the `CREATE TABLE` command
used to create a table

[`SHOW TABLES`](show-tables.html) - Display all existing table names

[`USE`](use.html) - Use a namespace
