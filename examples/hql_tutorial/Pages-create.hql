USE "/";
CREATE TABLE Pages (
       date,
       "refer-url",
       "http-code",
ACCESS GROUP default ( date, "refer-url", "http-code" )
);
load data infile ROW_KEY_COLUMN=rowkey "examples/hql_tutorial/access.tsv" into table Pages;
