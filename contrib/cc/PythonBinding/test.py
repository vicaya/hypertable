import ht

# create client
client = ht.Client("/opt/hypertable/0.9.0.7/")

# open the table
table = client.open_table("my_table")

# create scan specification
scan_spec_builder = ht.ScanSpecBuilder()
scan_spec_builder.add_row_interval("\x00", True, "\xff\xff", True)
scan_spec_builder.add_column("a")

# initialize the scanner with it
scanner = table.create_scanner(scan_spec_builder)

# create cell holding consecutive values from the scanner
cell = ht.Cell()

# scan it!
#while scanner.next(cell):
  # note that instead of cell.value and cell.value_len fields
  # you get single cell.value() function returning the value from the cell
  #print "%s:%s %s" % (cell.row_key, cell.column_family, cell.value())


# create table mutator

mutator = table.create_mutator(10)

families = ['a','b','c']

for i in range(1,100000):
  row = "row-%s" % (i)
  family = families[i % 3]
  qualifier = ""
  value = "value:%s" % i

  mutator.set(row, family, qualifier, value, len(value)) 

mutator.flush()
