SELECT
------
#### EBNF

    SELECT ('*' | column_family_name [',' column_family_name]*)
      FROM table_name
      [where_clause]
      [options_spec]

    where_clause:
        WHERE where_predicate [AND where_predicate ...]

    where_predicate:
      cell_predicate
      | row_predicate
      | timestamp_predicate

    relop: '=' | '<' | '<=' | '>' | '>=' | '=^'

    cell_spec: row_key ',' qualified_column

    cell_predicate:
      [cell_spec relop] CELL relop cell_spec
      | '(' [cell_spec relop] CELL relop cell_spec
            (OR [cell_spec relop] CELL relop cell_spec)* ')'

    row_predicate:
      [row_key relop] ROW relop row_key
      | '(' [row_key relop] ROW relop row_key
              (OR [row_key relop] ROW relop row_key)* ')'

    timestamp_predicate:
      [timestamp relop] TIMESTAMP relop timestamp

    options_spec:
      (REVS = revision_count
      | LIMIT = row_count
      | INTO FILE 'file_name'
      | DISPLAY_TIMESTAMPS
      | RETURN_DELETES
      | KEYS_ONLY
      | NOESCAPE)*

    timestamp:
      'YYYY-MM-DD HH:MM:SS[.nanoseconds]'

#### Description
<p>
The parser only accepts a single timestamp predicate.  The '=^' operator is the
"starts with" operator.  It will return all rows that have the same prefix as
the operand.

#### Examples

    SELECT * FROM test WHERE ('a' <= ROW <= 'e') and
                             '2008-07-28 00:00:02' < TIMESTAMP < '2008-07-28 00:00:07';
    SELECT * FROM test WHERE ROW =^ 'b';
    SELECT * FROM test WHERE (ROW = 'a' or ROW = 'c' or ROW = 'g');
    SELECT * FROM test WHERE ('a' < ROW <= 'c' or ROW = 'g' or ROW = 'c');
    SELECT * FROM test WHERE (ROW < 'c' or ROW > 'd');
    SELECT * FROM test WHERE (ROW < 'b' or ROW =^ 'b');
    SELECT * FROM test WHERE "farm","tag:abaca" < CELL <= "had","tag:abacinate";
    SELECT * FROM test WHERE "farm","tag:abaca" <= CELL <= "had","tag:abacinate";
    SELECT * FROM test WHERE CELL = "foo","tag:adactylism";
    SELECT * FROM test WHERE CELL =^ "foo","tag:ac";
    SELECT * FROM test WHERE CELL =^ "foo","tag:a";
    SELECT * FROM test WHERE CELL > "old","tag:abacate";
    SELECT * FROM test WHERE CELL >= "old","tag:abacate";
    SELECT * FROM test WHERE "old","tag:foo" < CELL >= "old","tag:abacate";
    SELECT * FROM test WHERE ( CELL = "maui","tag:abaisance" OR
                               CELL = "foo","tag:adage" OR
                               CELL = "cow","tag:Ab" OR
                               CELL =^ "foo","tag:acya");

