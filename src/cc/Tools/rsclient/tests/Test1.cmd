load range Test1[..kerchoo]
update Test1 Test1-data.txt
create scanner Test1[..kerchoo]
create scanner Test1[..kerchoo] start=Agamemnon end=Agamemnon
create scanner Test1[..kerchoo] start=Agamemnon row-limit=5
create scanner Test1[..kerchoo] row-range=[Agamemnon,Agamemnon]
create scanner Test1[..kerchoo] row-range=[Agamemnon,Aganice]
create scanner Test1[..kerchoo] row-range=(Agamemnon,Aganice]
create scanner Test1[..kerchoo] row-range=[Agamemnon,Aganice)
create scanner Test1[..kerchoo] row-range=(Agamemnon,Aganice)
quit
