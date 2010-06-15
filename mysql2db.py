# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
del sys.setdefaultencoding

import os.path
import codecs
import re

regions = re.compile(r"(?P<non_literal>[^']+)|(?P<literal>'(?:[^']|(?:''))*')").finditer

def parseSimpleString(s):
    #s = s.replace(r"\'","''")
    Qsub = re.compile(r"([^\\])\\'").sub
    s = Qsub(r"\1''", s)
    regions = []
    fields = []
    def mlen(seq):
        n = 0
        for s in seq:
            if s[0] == "'":
                n += 1
            else:
                n += s.count(',')-1

    for match in regions(s):
        non_literal, literal = match.groups()
        if non_literal:
            if non_literal.startswith('),('):
                if regions:
                    if mlen(regions[-1]) != mlen(fields): print "BADLEN1:",mlen(regions[-1]),mlen(fields),"\n",''.join(regions[-1]),"\n",''.join(fields)
                regions.append(fields)
                fields = []
                if len(non_literal) > 3:
                    fields = [non_literal[3:]]
            else:
                fields.append(non_literal)
                if regions:
                    if mlen(regions[-1]) < mlen(fields): print "BADLEN2:",mlen(regions[-1]),mlen(fields),"\n",''.join(regions[-1]),"\n",''.join(fields)
        elif literal:
            if len(fields) >= n and n>0: print "literal:",literal
            fields.append(literal)
    if regions:
        if mlen(regions[-1]) < mlen(fields): print "BADLEN3:",mlen(regions[-1]),mlen(fields),"\n",''.join(regions[-1]),"\n",''.join(fields)
    regions.append(fields)

    return regions

def parseString(s):
    if s.find(r"\'") < 0:
        return parseSimpleString(s)
    if s.find(r"\\'") < 0:
        return parseSimpleString(s.replace(r"\'","''"))
    return [[y.replace("||||||||||",r"\\") for y in x] for x in parseSimpleString(s.replace(r"\\","||||||||||").replace(r"\'","''"))]

class Table:

    def __init__(self, s):
        cm = re.match(u"^CREATE TABLE\s+`(\w+)`\s+[(]", s)
        if cm:
            self.name = cm.group(1)
        else:
            raise Exception, "Unknown create: <%s>" % s
        self.field_names = []
        self.fields = {}
        self.pk = None

    def addcol(self, l):
        name,coldef = l[1:].split('`')
        try:
            i = coldef.index(u" on update")
            coldef = coldef[1:i]
        except ValueError:
            pass
        self.fields[name] = coldef.replace("unsigned", "")
        self.field_names.append(name)

    def add(self, l):
        if l[0] == '`':
            self.addcol(l)
        elif l.startswith(u"PRIMARY KEY"):
            m = re.match(u"^PRIMARY KEY\s+.*[(]`(\w+)`[)]", l)
            if m:
                pk = m.group(1)
                if pk in self.fields:
                    if self.fields[pk].endswith("auto_increment"):
                        self.fields[pk] = "integer not null primary key autoincrement"
                    else:
                        self.pk = pk
            else:
                raise Exception, "Unknown primary key: <%s>" % l
        elif l.lower().startswith(u"fulltext key"):
            pass
        elif l.startswith(u") ENGINE="):
            return True
        return False

    def image(self):
        fields = ['"%s" %s' % (n,self.fields[n]) for n in self.field_names]
        if self.pk:
            fields.append('PRIMARY KEY ("%s")' % self.pk)
        s = u'CREATE TABLE "%s" (\n%s\n);' % (self.name,
                                         ',\n'.join(fields))
        return s

class Converter:

    def __init__(self, file_in):
        self.file_in = file_in
        self.insert_match = re.compile(u"^INSERT INTO `(.*)` VALUES [(](.*)[)];$").match
        self.Zsub = re.compile(r"([^\\])\\Z").sub
        self.Qsub = re.compile(r"([^\\])\\'").sub
        self.ins = ''

    def convert(self, file_out):
        self.fin = open(self.file_in, mode='r')
        #self.fin = codecs.open(self.file_in, mode='r', encoding='utf-8')
        self.open_out(file_out)
        self.do_convert()
        self.fin.close()
        self.close_out()

    def open_out(self, file_out):
        self.fout = codecs.open(file_out, mode='w', encoding='utf-8')

    def close_out(self):
        self.fout.close()

    def out(self, lines, eol=True):
        if isinstance(lines, (str, unicode)):
            lines = (lines,)
        for line in lines:
            self.fout.write(line)
            if eol:
                self.fout.write("\n")

    def begin(self):
        self.out(u"BEGIN TRANSACTION;")

    def commit(self):
        self.out(u"COMMIT;")

    def create_table(self, table):
        if table.name == u'service': return
        self.out(table.image())

    def do_insert(self, query):
        self.out(query)

    def insert(self, line):
        if self.ins:
            line = self.ins + line
            self.ins = ''
        m = self.insert_match(line)
        Rre = re.compile("['][)],[(]").split
        if m:
            table_name, data = m.groups()
            if table_name == u'service': return
            #data = self.Qsub(r"\1''",
            #                 self.Zsub(r"\1\\Z",
            #                           data.replace(r'\"','"').replace(r"\'\'","''''"))).replace(r"\\\'","\\''")
            #for x in data.split(u"),("):
            l = Rre(data)
            lenl = len(l)
            for i,x in enumerate(l):
                suffix = "'"
                if i == lenl-1:
                    suffix = ""
                x = self.Qsub(r"\1''",
                              self.Zsub(r"\1\\Z",
                                        x.replace(r'\"','"').replace(r"\'\'","''''"))).replace(r"\\\'","\\''")
                try:
                    self.do_insert(u'''INSERT INTO "%s" VALUES (%s%s);''' %(table_name, x, suffix))
                except:
                    print "line=<%s>" % line
                    print "x=<%s>" % x
                    raise
        else:
            self.ins = line

    def do_convert(self):
        tbl = None
        i,j = 0,0
        for l in self.fin:
            i += 1
            l = l.strip()
            if not l:
                continue
            if l.startswith(u"INSERT INTO"):
                if tbl: raise Exception, "Parse error <%s>" % l
                self.insert(l)
                j += 1
                print "-- ",i,j,len(l)
            elif (l.startswith(u"/*") or
                  l.startswith(u"--") or
                  l.startswith(u"SET ") or
                  l.startswith(u"USE ") or
                  l.startswith(u"DROP TABLE ") or
                  l.startswith(u"CREATE DATABASE ")
                  ):
                if tbl: raise Exception, "Parse error <%s>" % l
            elif l.startswith(u"LOCK TABLES "):
                if tbl: raise Exception, "Parse error <%s>" % l
                self.begin()
            elif l.startswith(u"UNLOCK TABLES"):
                if tbl: raise Exception, "Parse error <%s>" % l
                self.commit()
            elif l.startswith(u"CREATE TABLE "):
                tbl = Table(l)
            elif tbl:
                if tbl.add(l.strip(',')):
                    self.create_table(tbl)
                    tbl = None
            else:
                self.insert(l)

class ConverterToSqlite(Converter):

    def open_out(self, dbfile, overwrite=True):
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
        print "BEGIN"
        self.curs.execute("BEGIN")

    def commit(self):
        print "COMMIT"
        self.curs.execute("COMMIT")

    def create_table(self, table):
        if table.name == u'service': return
        print table.image()
        self.curs.execute(table.image())

    def do_insert(self, query):
        try:
            self.curs.execute(query)
        except:
            print query
            raise



if __name__ == '__main__':

    fdump = u"/home/termim/books/gen.lib.rus.ec/backup/upd-5part/backup/backup_ba.sql"
    fout = u"backup_ba_out.sql"

    c = Converter(fdump)
    #fout = u"backup_ba_out.db"
    #c = ConverterToSqlite(fdump)
    c.convert(fout)
