update Test1 Test1-data.txt
create scanner Test1[..??]
destroy scanner
create scanner Test1[..??] start=Agamemnon end=Agamemnon
destroy scanner
create scanner Test1[..??] start=Agamemnon row-limit=5
destroy scanner
create scanner Test1[..??] row-range=[Agamemnon,Agamemnon]
destroy scanner
create scanner Test1[..??] row-range=[Agamemnon,Aganice]
destroy scanner
create scanner Test1[..??] row-range=(Agamemnon,Aganice]
destroy scanner
create scanner Test1[..??] row-range=[Agamemnon,Aganice)
destroy scanner
create scanner Test1[..??] row-range=(Agamemnon,Aganice)
destroy scanner
quit
