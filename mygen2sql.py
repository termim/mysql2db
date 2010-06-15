import sys
import os

from mysql2db import ConverterToSqlite
from rarfile import RarFile

if len(sys.argv) != 3:
    print "Use sys.argv[0] genlib_backup_file output.db"
    sys.exit(1)

infile = sys.argv[1]
outfile = sys.argv[2]

if os.path.splitext(infile)[1].upper() == '.RAR':
    rf = RarFile(infile)
    print rf.infolist()
    fn = rf.namelist()[0]
    print rf.namelist()
    print len(rf.read(fn))
