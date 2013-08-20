#!/usr/bin/python
import sys

if len(sys.argv) == 3:
    input_file = sys.argv[1]
    outdir = sys.argv[2]
else:
    print("usage: ./split.py input_file outdir")
    sys.exit(1)

#parse input query
input = open(input_file)
queries = []
query = ""
name = ""
for line in input.readlines():
    if line[0] == ">":
        if name != "":
            queries.append([name, query])
        name = line
        query = ""
    else:
        query += line
if name != "":
    queries.append([name, query])
input.close()

#output each files
block = {}
count = 0
for query in queries:
    outfile = open("%s/%07d.fasta" % (outdir, count), "w")
    outfile.write(query[1])
    outfile.close()
    count+=1
