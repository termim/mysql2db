# -*- coding: utf-8 -*-
import os.path
import re
from collections import OrderedDict
import gzip


class Insert:

    def __init__(self):
        self.statements = []
        self.tablenames = []
        self.last = ''
        self.sep = re.compile(r"([^\\]'|\d)[)],[(]")
        self.match = re.compile("INSERT\s+INTO\s+`([^`]+)`\s+VALUES\s+[(]\s*(.*)", re.IGNORECASE).match


    def match_quote(self, m):
        backsl = m.group(2)
        q = "'"
        if len(backsl) % 2:
            backsl = backsl[:-1]
            q = "''"
        return m.group(1) + backsl + q


    findquotes = re.compile("'").findall
    re_quote = re.compile(r"([^\\]?)([\\]+)'")

    def next(self):
        while self.statements:
            n = 0
            ll = ""
            try:
                while 1:
                    table_idx, data = self.statements[n]
                    l = data[:]
                    l = self.re_quote.sub(self.match_quote, l)
                    l = l.replace("\u2019", "''")
                    l = l.replace(r'\"', '"')
                    n += 1
                    ll += l
                    if len(self.findquotes(ll)) % 2 == 0: break
                    ll += "),("
                yield 'INSERT INTO "{}" VALUES ({});'.format(self.tablenames[table_idx], ll)
            except:
                print 'c:',(data)
                for i in range(min(5, len(self.statements))):
                    print '%d:' %i, (self.statements[i][1])
                raise
            for _ in range(n):
                self.statements.pop(0)


    def feed(self, data):
        if self.last:
            data, self.last = self.last + data, ""
        start = 0
        m = self.match(data)
        if m:
            self.tablenames.append(m.group(1))
            start = m.start(2)
        self.feed_split(data, start)


    def feed_split(self, data, start):
        table_idx = len(self.tablenames) - 1
        lst = self.sep.split(data[start:])
        n, N = 0, len(lst)-1
        while n < N:
            l = lst[n] + lst[n+1]
            self.statements.append((table_idx, l))
            n += 2
        last = lst[-1].strip()
        if last.endswith(");"):
            self.statements.append((table_idx, last[:-2]))
        else:
            self.last = lst[-1]



class Table:

    creatematch = re.compile("^CREATE TABLE\s+`(\w+)`\s+[(]", re.IGNORECASE).match

    def __init__(self, s):
        #print s
        cm = self.creatematch(s)
        if not cm:
            raise Exception("Unknown create: <%s>" % s)
        self.name = cm.group(1)
        self.columns = OrderedDict()
        self.pk = None
        self.keys = {}
        self.done = False


    re_sub_length = re.compile("([(]\d+[)])").sub
    keymatch = re.compile(
        "(?P<qual>PRIMARY|UNIQUE|FOREIGN)?"
        "\s*(?:INDEX|KEY)"
        "\s+(?P<keyname>`\w+`)?"
        "(?:\s+USING BTREE)?"
        "[^(]*(?:[(]`)(?P<columns>[^)]+)(?:`[)])"
        , re.IGNORECASE).search

    def match_key(self, line):
        l = self.re_sub_length("", line)
        m = self.keymatch(l)
        if not m:
            return
        qual, keyname, columns = m.groups()
        keyname = keyname.replace('`', '"') if keyname else ''
        columns = '"{}"'.format(columns.replace('`', '"'))

        #stmt = 'CREATE INDEX test1_id_index ON test1 (id);'
        self.keys[keyname] = qual, columns
        if qual and qual.upper() == "PRIMARY":
            self.pk = keyname
        return (keyname, qual, columns)


    colmatch = re.compile(
        "\s*`(?P<colname>\w+)`"
        "\s+(?P<coltype>[\w]+)"
        "\s*(?:[(]?)(?P<collen>[\d,]+)?(?:[)]?)"
        "\s*(?P<rest>.*)", re.IGNORECASE).match
    notnull = re.compile("NOT NULL", re.IGNORECASE).search
    autoincrement = re.compile("AUTO_INCREMENT", re.IGNORECASE).search
    comment = re.compile("(COMMENT\s+'[^']*')", re.IGNORECASE).search
    primarykey = re.compile("PRIMARY\s+KEY\s*([(][^)]+[)])*", re.IGNORECASE).search

    typemap = {
        'TINYINT': 'INTEGER', 'SMALLINT': 'INTEGER', 'MEDIUMINT': 'INTEGER', 'INT': 'INTEGER', 'INTEGER': 'INTEGER', 'BIGINT': 'INTEGER',
        'REAL': 'REAL', 'DOUBLE': 'DOUBLE', 'FLOAT': 'FLOAT',
        'DECIMAL': 'NUMERIC',
        'DATE': 'DATE', 'TIME': 'TIME', 'TIMESTAMP': 'TIMESTAMP', 'DATETIME': 'DATETIME',
        'YEAR': 'INTEGER',
        'CHAR': 'TEXT', 'VARCHAR': 'TEXT', 'TINYTEXT': 'TEXT', 'TEXT': 'TEXT', 'MEDIUMTEXT': 'TEXT', 'LONGTEXT': 'TEXT',
        'BINARY': 'BLOB', 'VARBINARY': 'BLOB', 'TINYBLOB': 'BLOB', 'BLOB': 'BLOB', 'MEDIUMBLOB': 'BLOB', 'LONGBLOB': 'BLOB',
        'ENUM': 'TEXT',
        'SET': 'TEXT',
    }

    def match_col(self, line):
        l = line#self.re_sub_length("", line)
        m = self.colmatch(l)
        if not m: return
        colname, coltype, collen, rest = m.groups()
        print (colname, coltype, collen, rest)
        coltype = self.typemap[coltype.upper()]
        self.columns[colname] = '"{}" {}'.format(colname, coltype)
        return colname, coltype, collen, rest


    re_engine = re.compile("[)] ENGINE=", re.IGNORECASE).search

    def match_end(self, line):
        self.done = self.re_engine(line) is not None
        return self.done


    _re_charset = re.compile(".*\s+(CHARACTER\s+SET\s+\w+)", re.IGNORECASE).match

    def feed(self, line):
        if self.done:
            raise Exception("Already done")

        for match in (self.match_col, self.match_key,):
            try:
                if match(line): return
            except:
                print "|%s|" % line
                raise

        if self.match_end(line):
            return True

        raise Exception("Unknown line: <%s>" % line)


    def image(self):
        s = 'CREATE TABLE "{}" (\n{}\n);'.format(
            self.name,
            ',\n'.join(self.columns.values()))
        return s



class Converter:

    def __init__(self, file_in):
        self.file_in = file_in
        self.insert_match = re.compile("^INSERT INTO `(.*)` VALUES [(](.*)[)];$").match
        self.Zsub = re.compile(r"([^\\])\\Z").sub
        self.Qsub = re.compile(r"([^\\])\\'").sub
        self.ins = Insert()


    def convert(self, file_out, overwrite=False):
        if self.file_in.endswith(".gz"):
            self.fin = gzip.open(self.file_in)
        else:
            self.fin = open(self.file_in)
        self.open_out(file_out, overwrite)
        self.do_convert()
        self.fin.close()
        self.close_out()


    def open_out(self, file_out, overwrite=False):
        if os.path.exists(file_out) and overwrite:
            os.remove(file_out)
            self.fout = open(file_out, 'w')
        else:
            self.fout = open(file_out, 'a')


    def close_out(self):
        self.fout.close()


    def out(self, lines, eol=True):
        if isinstance(lines, (str,)):
            lines = (lines,)
        for line in lines:
            self.fout.write(line)
            if eol:
                self.fout.write("\n")


    def begin(self):
        self.out("BEGIN TRANSACTION;")


    def commit(self):
        self.out("COMMIT;")


    def create_table(self, table):
        if table.name == 'service': return
        self.out(table.image())


    def do_insert(self, query):
        self.out(query)


    def insert(self, line):
        self.ins.feed(line)
        for stmt in self.ins.next():
            try:
                self.do_insert(stmt)
            except:
                print ("stmt=<|%s|>" % stmt)
                raise


    def do_convert(self):
        tbl = None
        i,j = 0,0
        for l in self.fin:
            #l = l.decode()
            i += 1
            l = l.strip()
            if not l:
                continue
            if l.startswith("INSERT INTO"):
                if tbl: raise Exception("Parse error <%s>" % l)
                self.insert(l)
                j += 1
                print ("-- ",i,j,len(l))
            elif (l.startswith("/*") or
                  l.startswith("--") or
                  l.startswith("SET ") or
                  l.startswith("USE ") or
                  l.startswith("DROP TABLE ") or
                  l.startswith("CREATE DATABASE ")
                  ):
                if tbl: raise Exception("Parse error <%s>" % l)
            elif l.startswith("LOCK TABLES "):
                if tbl: raise Exception("Parse error <%s>" % l)
                self.begin()
            elif l.startswith("UNLOCK TABLES"):
                if tbl: raise Exception("Parse error <%s>" % l)
                self.commit()
            elif l.startswith("CREATE TABLE "):
                tbl = Table(l)
            elif tbl:
                if tbl.feed(l):
                    self.create_table(tbl)
                    tbl = None
            else:
                self.insert(l)



class ConverterToSqlite(Converter):

    def open_out(self, dbfile, overwrite=False):
        import sqlite3
        if os.path.exists(dbfile) and overwrite:
            os.remove(dbfile)
        self.conn = sqlite3.connect(dbfile, isolation_level=None)
        self.curs = self.conn.cursor()
        self.curs.execute("PRAGMA cache_size=10000")
        self.curs.execute("PRAGMA synchronous=0")
        self.curs.execute("PRAGMA journal_mode=MEMORY")


    def close_out(self, commit=True):
        if commit: self.conn.commit()
        self.conn.close()


    def begin(self):
        print ("BEGIN")
        self.curs.execute("BEGIN")


    def commit(self):
        print ("COMMIT")
        self.curs.execute("COMMIT")


    def create_table(self, table):
        #if table.name == 'service': return
        print (table.image())
        self.curs.execute(table.image())


    def do_insert(self, query):
        try:
            self.curs.execute(query)
        except:
            print (query)
            raise
