USE 
------------
#### EBNF

    USE namespace_name 

#### Description
<p>
The USE command sets the current namespace.
If namespace_name starts with '/' it treats the namespace_name as an absolute path, 
otherwise it considers it to be a sub-namespace relative to the current namespace.

#### Example
    
    hypertable> USE NAMESPACE "/test";
    hypertable> USE NAMESPACE "subtest";
