USE 
------------
#### EBNF

    USE namespace_name 

#### Description
<p>
The USE command sets the current namespace.
If 'namespace\_name' starts with '/' it treats the 'namespace\_name' as an absolute path, 
otherwise it considers it to be a sub-namespace relative to the current namespace.

#### Example
<p>    
    hypertable> USE "/";
    hypertable> USE "/test";
    hypertable> USE "subtest";
