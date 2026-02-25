#!/usr/bin/env python3

from os import environ, makedirs
from random import randint, sample, seed
import string

# CONFIGURATION (override with environment variables when needed)
N = int(environ.get("GPUQO_TABLES", "40"))
ROWS = int(environ.get("GPUQO_ROWS", "200"))
R = int(environ.get("GPUQO_QUERIES_PER_SIZE", "10"))
SEED = int(environ.get("GPUQO_SEED", "3735928559"), 0)
MAX_QUERY_SIZE = min(N - 1, int(environ.get("GPUQO_MAX_QUERY_SIZE", "16")))

# CONSTANTS

CENTRAL_TABLE_PATTERN="""CREATE TABLE T0 (
    pk INT PRIMARY KEY,
%s
);
"""

TABLE_PATTERN="""CREATE TABLE T%d (
    pk INT PRIMARY KEY
);
"""

FK_PATTERN="""ALTER TABLE T0
ADD FOREIGN KEY (t%d) REFERENCES T%d(pk);
"""

# FUNCTIONS

def make_create_tables(n):
    out = ""
    columns = ",\n".join(["t%d INT" % i for i in range(1,n)])
    out += CENTRAL_TABLE_PATTERN % columns
    for i in range(1,n):
        out += TABLE_PATTERN % i
    return out

def make_foreign_keys(n):
    out = ""
    for i in range(1,n):
        out += FK_PATTERN % (i,i)
    return out

def make_insert_into(n, size):
    out = ""
    for i in range(1,n):
        out += f"INSERT INTO T{i} (pk)\nVALUES\n"
        values = [f"    ({j})" for j in range(size)]
        out += ",\n".join(values)
        out += ";\n\n"

    columns = ', '.join([f"t{i}" for i in range(1,n)])
    out += f"INSERT INTO T0 (pk, {columns})\nVALUES\n"
    values = [f"    ({j}, {', '.join([str(randint(0,size-1)) for i in range(1,n)])})" for j in range(size)]
    out += ",\n".join(values)
    out += ";\n\n"
    return out

def make_query(N, n):
    qs = sample(list(range(1,N)), n-1)
    from_clause = ", ".join(["T%d" % j for j in qs] + ["T0"])
    where_clause = " AND ".join(["T0.t%d = T%d.pk" % (j,j) for j in qs])
    return f"SELECT * FROM {from_clause} WHERE {where_clause}; -- {n}"

# EXECUTION

seed(SEED)
labels = [f"{a}{b}" for a in string.ascii_lowercase for b in string.ascii_lowercase]

with open("schema.sql", "w") as f:
    f.write(make_create_tables(N))
    f.write("\n")
    f.write(make_foreign_keys(N))
    f.write("\n")

with open("load.sql", "w") as f:
    f.write(make_insert_into(N, ROWS))
    f.write("\n")

makedirs("queries", exist_ok=True)
for n in range(2, MAX_QUERY_SIZE + 1):
    for i in range(R):
        with open(f"queries/{n:02d}{labels[i]}.sql", "w") as f:
            f.write(make_query(N, n))
            f.write("\n")
