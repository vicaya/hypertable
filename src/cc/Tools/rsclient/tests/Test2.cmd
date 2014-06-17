UPDATE 'test/Test2' "Test2-data.txt";
CREATE SCANNER ON 'test/Test2'[..??] WHERE ROW >= "Abama" LIMIT=4;
DESTROY SCANNER;
CREATE SCANNER ON 'test/Test2'[..??] WHERE ROW >= "Abama" LIMIT=2 REVS=1;
DESTROY SCANNER;
quit
