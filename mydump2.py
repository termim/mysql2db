# -*- coding: utf-8 -*-

from os.path import *
import argparse

from mysql2db import *



def convert2sql(args):
    c = MySqlDumpToSqlDump()
    if args.extract_schema_only:
        args.schema_only = True
    for f in args.infiles:
        print f
        c.convert(f, args.output, overwrite=False, verbose=args.verbose,
                  skip_schema=args.data_only, schema_only=args.schema_only,
                  convert_schema=not args.extract_schema_only)


def convert2sqlite(args):
    c = MySqlToSqlite()
    for f in args.infiles:
        print f
        c.convert(f, args.output, overwrite=False, verbose=args.verbose,
                  skip_schema=args.data_only, schema_only=args.schema_only)


def main():
    parser = argparse.ArgumentParser(prog='mydump2')

    subparsers = parser.add_subparsers(dest='subcmd',
                                       metavar='CONVERSION',
                                       title="Conversion type",
                                       description="output destination",
                                       help='use CONVERSION [-h|--help] for more info')

    parser_sql = subparsers.add_parser('sql', help='Convert MySql dump into Sql dump')
    parser_sql.set_defaults(func=convert2sql)

    parser_sqlite = subparsers.add_parser('sqlite', help='Convert MySql dump into Sqlite database')
    parser_sqlite.set_defaults(func=convert2sqlite)

    for p in (parser_sql, parser_sqlite):
        p.add_argument('-v', '--verbose', action='store_true', help='write progress information to stdout')
        p.add_argument('-a', '--append', action='store_true', help='append to the output file if exists')
        group = p.add_mutually_exclusive_group()
        group.add_argument('-d', '--data-only', action='store_true', help='skip tables creation on output')
        group.add_argument('-s', '--schema-only', action='store_true', help='create tables only on output')
        if p == parser_sql:
            group.add_argument('-e', '--extract-schema-only', action='store_true', help='do not convert schema')
        p.add_argument('infiles', metavar='INFILE', nargs='+', help='input MySql dump file')
        p.add_argument('-o','--output', metavar='OUTFILE', help='output file')

    try:
        args = parser.parse_args()
        if exists(args.output) and not args.append:
            os.remove(args.output)
    except Exception as ex:
        print ex
        parser.exit(1)

    print args
    parser.exit(args.func(args))



if __name__ == "__main__":
    main()
