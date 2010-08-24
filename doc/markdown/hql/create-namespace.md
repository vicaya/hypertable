CREATE NAMESPACE 
------------
#### EBNF

    CREATE NAMESPACE namespace_name 

#### Description
<p>
`CREATE NAMESPACE`command creates a new namespace. 
If namespace_name starts with '/' it treats the namespace_name as an absolute path
otherwise it considers it to be a sub-namespace relative to the current namespace.
#### Example
    
    hypertable> CREATE NAMESPACE "/test";
    hypertable> USE NAMESPACE "/test";
    hypertable> CREATE NAMESPACE "subtest";
