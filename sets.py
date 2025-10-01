import csv

# Input CSV file (two columns)
infile = "input.csv"

# Output text files
outfile_a = "a_not_b.txt"
outfile_b = "b_not_a.txt"

col_a_vals = []
col_b_vals = []

with open(infile, newline='', encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        col_a_vals.append(row[reader.fieldnames[0]].strip())
        col_b_vals.append(row[reader.fieldnames[1]].strip())

set_a = set(col_a_vals)
set_b = set(col_b_vals)

a_not_b = sorted(set_a - set_b)
b_not_a = sorted(set_b - set_a)

print("Values in Column A but not in Column B:")
print(a_not_b)

print("\nValues in Column B but not in Column A:")
print(b_not_a)

# Write each list to a separate text file
with open(outfile_a, "w", encoding="utf-8") as f:
    for item in a_not_b:
        f.write(item + "\n")

with open(outfile_b, "w", encoding="utf-8") as f:
    for item in b_not_a:
        f.write(item + "\n")

print(f"\n[INFO] Wrote {len(a_not_b)} values to {outfile_a}")
print(f"[INFO] Wrote {len(b_not_a)} values to {outfile_b}")
