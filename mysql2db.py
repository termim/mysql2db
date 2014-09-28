# -*- coding: utf-8 -*-
import os.path
import re
import gzip



class Insert:


    def __init__(self):
        self.statements = []
        self.tablenames = []
        self.last = ''
        self.sep = re.compile(r"([^\\]'|\d)[)],[(]")
        self.match_insert = re.compile("\s*INSERT\s+INTO\s+`([^`]+)`\s+VALUES\s+[(]\s*(.*)", re.IGNORECASE).match


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
        m = self.match_insert(data)
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



class Column:


    typemap = {
        'TINYINT': 'SMALLINT', 'SMALLINT': 'SMALLINT',
        'MEDIUMINT': 'INTEGER', 'INT': 'INTEGER', 'INTEGER': 'INTEGER',
        'BIGINT': 'BIGINT',
        'REAL': 'REAL', 'DOUBLE': 'DOUBLE', 'FLOAT': 'REAL',
        'DECIMAL': 'NUMERIC', 'NUMERIC': 'NUMERIC',
        'DATE': 'DATE', 'TIME': 'TIME', 'TIMESTAMP': 'TIMESTAMP', 'DATETIME': 'DATETIME',
        'YEAR': 'INTEGER',
        'CHAR': 'TEXT', 'VARCHAR': 'TEXT', 'TINYTEXT': 'TEXT', 'TEXT': 'TEXT', 'MEDIUMTEXT': 'TEXT', 'LONGTEXT': 'TEXT',
        'BINARY': 'BLOB', 'VARBINARY': 'BLOB', 'TINYBLOB': 'BLOB', 'BLOB': 'BLOB', 'MEDIUMBLOB': 'BLOB', 'LONGBLOB': 'BLOB',
        'ENUM': 'TEXT',
        'SET': 'TEXT',
        'BIT': 'INTEGER',
    }

    colmatch = re.compile(
        "\s*`(?P<colname>\w+)`"
        "\s+(?P<coltype>[\w]+)"
        "\s*(?P<collen>[(][^()]+[)])?(?:[)]?)"
        "\s*(?P<binary>BINARY)?"
        "\s*(?P<unsigned>UNSIGNED)?"
        "\s*(?P<zerofill>ZEROFILL)?"
        "\s*(?P<charset>CHARACTER SET\s+\w+)?"
        "\s*(?P<collate>COLLATE\s+\w+)?"

        "\s*(?P<notnull>NOT NULL|NULL)?"
        "\s*(?P<default>DEFAULT\s+'[^']*')?"
        "\s*(?P<autoincrement>AUTO_INCREMENT)?"
        "\s*(?P<key>UNIQUE KEY|PRIMARY KEY|UNIQUE|KEY)?"
        "\s*(?P<comment>COMMENT\s+'[^']*')?"
        "\s*(?:COLUMN_FORMAT \w+)?"

        "\s*(?P<reference>.*)", re.IGNORECASE).match


    @classmethod
    def match(cls, line):
        m = cls.colmatch(line)
        if not m: return
        return cls(m)


    def __init__(self, matcho):
        self.matcho = matcho
        for key, value in matcho.groupdict().items():
            setattr(self, key, value)
        self.originaltype = self.coltype
        self.coltype = self.typemap[self.coltype.upper()]
        if self.collen:
            self.collen = [int(x.strip()) if x.strip().isdigit() else x.strip() for x in self.collen[1:-1].split(',')]
        if self.default:
            self.default = self.default[len("DEFAULT"):].strip()
            if self.coltype in ('SMALLINT', 'INTEGER', 'BIGINT'):
                self.default = int(self.default.strip("'"))
            elif self.coltype in ('REAL', 'DOUBLE', 'NUMERIC'):
                self.default = float(self.default.strip("'"))
        if self.comment:
            self.comment = self.comment[len("COMMENT"):].strip()
        if self.charset:
            self.charset = self.charset[len("CHARACTER SET"):].strip()
        if self.collate:
            self.collate = self.collate[len("COLLATE"):].strip()
        if self.binary:
            self.coltype = "BLOB"


    def hasIndex(self):
        return self.key is not None and self.key.upper() == "KEY"


    def constraints(self):
        cons = []
        if self.key is not None:
            if self.key.upper() in ("UNIQUE KEY", "UNIQUE"):
                cons.append("UNIQUE")
            elif self.key.upper() == "PRIMARY KEY":
                cons.append("PRIMARY KEY")
        if self.notnull:
            cons.append("NOT NULL")
        return cons


    def sql(self, flavor, skip_constraints=True):
        lst = ['"{}"'.format(self.colname)]
        lst.append("{}".format(self.coltype))
        if not skip_constraints:
            lst.extend(self.constraints())
        if self.autoincrement:
            lst.append("AUTOINCREMENT")
        if self.default is not None:
            lst.append("DEFAULT {}".format(self.default))
        s = ' '.join(lst)
        return s



class _Constraint:

    re_symbol    = "\s*(?:CONSTRAINT)?\s*(?P<symbol>`\w+`)?\s*"
    re_indextype = "\s*(?P<indextype>USING BTREE|USING HASH)?"
    re_indexcols = "\s*(?P<indexcols>[(][^()]+[)])"


    @classmethod
    def match(cls, line):
        m = cls.keymatch(line)
        if not m: return
        return cls(m)




    def __init__(self, matcho):
        self.matcho = matcho
        for key, value in matcho.groupdict().items():
            setattr(self, key, value)
            if key == 'symbol' and value is not None:
                self.symbol = value.strip('`')
            if key == 'indexname' and value is not None:
                self.indexname = value.strip('`')
            if key == 'indexcols' and value is not None:
                self.indexcols = [x.strip('`') for x in value.strip(')(').split(',')]
            #if key == 'check' and value is not None:
                #self.check = value[5:].strip()


    def sql(self, flavor):
        return


    def index(self, table_name, flavor):
        return



class Index(_Constraint):


    keymatch = re.compile(
        _Constraint.re_symbol +
        "(?P<unique>UNIQUE)?\s*"
        "(?P<fulltext>FULLTEXT|SPATIAL)?\s*"
        "(?P<key>INDEX|KEY)?\s*(?P<indexname>`\w+`)?" +
        _Constraint.re_indextype + _Constraint.re_indexcols,
        re.IGNORECASE).match


    def sql(self, flavor):
        if self.unique:
            return 'UNIQUE ("{}")'.format('", "'.join(self.indexcols))


    def index(self, table_name, flavor):
        if not self.unique:
            return 'CREATE INDEX "{}" ON "{}" ("{}")'.format(
                        self.indexname, table_name, '", "'.join(self.indexcols))



class PrimaryKey(_Constraint):


    keymatch = re.compile(
        _Constraint.re_symbol +
        "(?P<pkey>PRIMARY KEY)" +
        _Constraint.re_indextype + _Constraint.re_indexcols,
        re.IGNORECASE).match


    def sql(self, flavor):
        return '{} ("{}")'.format(self.pkey, '", "'.join(self.indexcols))



class ForeignKey(_Constraint):


    keymatch = re.compile(
        _Constraint.re_symbol +
        "(?P<fkey>FOREIGN KEY)" +
        _Constraint.re_indexcols,
        re.IGNORECASE).match


    def sql(self, flavor):
        return '{} ("{}")'.format(self.fkey, '", "'.join(self.indexcols))



class Check(_Constraint):


    keymatch = re.compile(
        "\s*CHECK\s+(?P<check>[(].*[)])",
        re.IGNORECASE).match


    def sql(self, flavor):
        return '{} ("{}")'.format(self.fkey, '", "'.join(self.indexcols))



def Constraint(line):
    for cls in (Index, PrimaryKey, ForeignKey, Check):
        m = cls.keymatch(line)
        if m:
            return cls(m)
    return None



class Table:

    creatematch = re.compile("^CREATE(?:\s+TEMPORARY)?\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`(\w+)`(?:\s+[(])?",
                    re.IGNORECASE).match


    @classmethod
    def match(cls, line):
        m = cls.creatematch(line)
        if not m: return
        return cls(m)


    def __init__(self, matcho):
        self.src = [matcho.string]
        self.name = matcho.group(1)
        self.columns = []
        self.constraints = []
        self.pk = None
        self.keys = {}
        self.done = False


    def match_col(self, line):
        column = Column.match(line)
        if column:
            self.columns.append(column)
            return column

    def match_constraint(self, line):
        constraint = Constraint(line)
        if constraint:
            self.constraints.append(constraint)
            return constraint


    re_engine = re.compile("[)] ENGINE=", re.IGNORECASE).search

    def match_end(self, line):
        self.done = self.re_engine(line) is not None
        return self.done


    def feed(self, line):
        if self.done:
            raise Exception("Already done")

        self.src.append(line)

        for match in (self.match_col, self.match_constraint,):
            try:
                if match(line): return
            except:
                print "|%s|" % line
                raise

        if self.match_end(line):
            return True

        raise Exception("Unknown line: <%s>" % line)


    def sql(self, flavor, skip_constraints=True):
        if not self.done:
            raise Exception("Table {} is not done yet".format(self.name))
        l = []
        for column in self.columns:
            l.append(column.sql(flavor, skip_constraints))
        s = 'CREATE TABLE "{}" (\n    {}\n);'.format(self.name, ',\n    '.join(l))
        return s


    def source(self):
        return "".join(self.src)



class MySqlDumpReader(object):


    flavor=None


    def __init__(self):
        self.insert_match = re.compile("^INSERT INTO `(.*)` VALUES [(](.*)[)];$").match
        self.ins = Insert()
        self.tables = []
        self.verbose = False
        self.skip_schema = False
        self.schema_only = False
        self.convert_schema = True


    def convert(self, file_in, file_out, overwrite=False, skip_schema=False, schema_only=False, verbose=False, convert_schema=True):
        self.verbose = verbose
        self.skip_schema = skip_schema
        self.schema_only = schema_only
        self.convert_schema = convert_schema
        self.open_in(file_in)
        self.open_out(file_out, overwrite)
        self.do_convert()
        self.fin.close()
        self.close_out()


    def open_in(self, file_in):
        if file_in.endswith(".gz"):
            self.fin = gzip.open(file_in)
        else:
            self.fin = open(file_in)


    def open_out(self, dbfile, overwrite=False):
        pass


    def close_out(self, commit=True):
        pass


    def begin(self):
        pass


    def commit(self):
        pass


    def create_table(self, table):
        pass


    def insert(self, line):
        pass


    def do_insert(self, query):
        pass


    def do_convert(self):
        tbl = None
        line_number,insert_number = 0,0
        for l in self.fin:
            line_number += 1
            if not l:
                continue
            if l.startswith("INSERT INTO"):
                if tbl: raise Exception("Parse error <%s>" % l)
                if not self.schema_only:
                    self.insert(l)
                if self.verbose:
                    insert_number += 1
                    print ("-- ",line_number,insert_number,len(l))
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
                if not self.schema_only:
                    self.begin()
            elif l.startswith("UNLOCK TABLES"):
                if tbl: raise Exception("Parse error <%s>" % l)
                if not self.schema_only:
                    self.commit()
            else:
                t = Table.match(l)
                if t:
                    if tbl: raise Exception("Ppevious table is not done yet <{}>".format(l))
                    tbl = t
                elif tbl:
                    if tbl.feed(l):
                        if not self.skip_schema:
                            self.create_table(tbl)
                        self.tables.append(tbl)
                        tbl = None
                else:
                    self.insert(l)



class MySqlDumpToSqlDump(MySqlDumpReader):


    flavor='sqlite'


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


    def create_table(self, table):
        if table.name == 'service': return
        if self.convert_schema:
            self.out(table.sql(flavor=self.flavor))
        else:
            self.out(table.source())


    def begin(self):
        self.out("BEGIN TRANSACTION;")


    def commit(self):
        self.out("COMMIT;")


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



class MySqlToSqlite(MySqlDumpToSqlDump):

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
        if self.verbose:
            print ("BEGIN")
        self.curs.execute("BEGIN")


    def commit(self):
        if self.verbose:
            print ("COMMIT")
        self.curs.execute("COMMIT")


    def create_table(self, table):
        #if table.name == 'service': return
        if self.verbose:
            print (table.sql(flavor=self.flavor))
        self.curs.execute(table.sql(flavor=self.flavor))


    def do_insert(self, query):
        try:
            self.curs.execute(query)
        except:
            print (query)
            raise
