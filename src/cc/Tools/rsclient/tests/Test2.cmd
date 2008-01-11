update Test2 Test2-data.txt
create scanner Test2[..??] start=Abama row-limit=4
destroy scanner
create scanner Test2[..??] start=Abama row-limit=2 max-versions=1
destroy scanner
quit
