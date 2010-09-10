DROP NAMESPACE 
----------
#### EBNF

    DROP NAMESPACE [IF EXISTS] namespace_name 

#### Description
<p>
The DROP NAMESPACE removes a namespace. If the IF EXISTS clause is specified, 
the command wont generate an error if the namespace 'namespace\_name' does not exist.
A namespace can only be dropped if it is empty (ie has contains no tables or sub-namespaces
If 'namespace\_name' starts with '/' it treats 'namespace\_name' as an absolute path,
otherwise it considers it to be a sub-namespace relative to the current namespace.

#### Example
    
    hypertable> DROP NAMESPACE "/test/subtest";
    hypertable> DROP NAMESPACE "/test";
