import ht

# create client
client = ht.Client("ht", "/opt/hypertable/0.9.0.7/conf/hypertable.cfg")

# open the table
table = client.open_table("METADATA")

# create scan specification
scan_spec = ht.ScanSpec()
scan_spec.start_row = "\x00"
scan_spec.end_row = "\xff\xff"
scan_spec.start_row_inclusive = True
scan_spec.end_row_inclusive = True
scan_spec.max_versions = 1

# initialize the scanner with it
scanner = table.create_scanner(scan_spec)

# create cell holding consecutive values from the scanner
cell = ht.Cell()

# scan it!
while scanner.next(cell):
  # note that instead of cell.value and cell.value_len fields
  # you get single cell.value() function returning the value from the cell
  print "%s:%s %s" % (cell.row_key, cell.column_family, cell.value())

