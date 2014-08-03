# -*- coding: utf-8 -*-

import sys

from mysql2db import MySqlToSqlite



if len(sys.argv) != 3:
    print "Use sys.argv[0] backup_file output.db"
    sys.exit(1)

infile = sys.argv[1]
outfile = sys.argv[2]

c = MySqlToSqlite()
c.convert(infile, outfile)
