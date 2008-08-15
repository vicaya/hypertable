import ht
import random

# create client
client = ht.Client("/opt/hypertable/0.9.0.9/")

# open the table
table = client.open_table("dummy4")

# create scan specification
#scan_spec_builder = ht.ScanSpecBuilder()
#scan_spec_builder.add_row_interval("\x00", True, "\xff\xff", True)
#scan_spec_builder.add_column("a")

# initialize the scanner with it
#scanner = table.create_scanner(scan_spec_builder)

# create cell holding consecutive values from the scanner
#cell = ht.Cell()

# scan it!
#while scanner.next(cell):
  # note that instead of cell.value and cell.value_len fields
  # you get single cell.value() function returning the value from the cell
  #print "%s:%s %s" % (cell.row_key, cell.column_family, cell.value())


# create table mutator

mutator = table.create_mutator(10)

families = ['meta', 'blob']
k = 10000
i = 1
while i < k+1 :
  row = "row-%s" % (i)
  family = families[0]
  qualifier = ""
  value = 'a' * 10 * 1024
  if i % 5000 == 0:
    print "%s/%s" % (i,k)
  mutator.set(row, family, qualifier, value, len(value))
  i = i + 1

mutator.flush()

