CREATE NAMESPACE 
------------
#### EBNF

    CREATE NAMESPACE namespace_name 

#### Description
<p>
`CREATE NAMESPACE` command creates a new namespace. 
If 'namespace\_name' starts with '/' it treats the 'namespace\_name' as an absolute path
otherwise it considers it to be a sub-namespace relative to the current namespace.
#### Example
    
    hypertable> CREATE NAMESPACE "/test";
    hypertable> USE "/test";
    hypertable> CREATE NAMESPACE "subtest";
