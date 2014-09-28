# -*- coding: utf-8 -*-
import unittest
from mysql2db import (Insert, Table, Column,
                      Constraint,
                      Index, PrimaryKey, ForeignKey
                      )



class TestInsert(unittest.TestCase):

    def setUp(self):
        self.ins = Insert()


    def test_empty(self):
        self.assertEqual(0, len(list(self.ins.next())))


    def test_feed(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(1, len(lst))
        self.assertEqual('INSERT INTO "mytable" VALUES (1,2,3,4);', lst[0])


    def test_feed_lower(self):
        fmt = "insert into `MYTABLE` values {};"
        values = "(1,2,3,4)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(1, len(lst))
        self.assertEqual('INSERT INTO "MYTABLE" VALUES (1,2,3,4);', lst[0])


    def test_feed_split_1(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4)"
        stmt = fmt.format(values)
        self.ins.feed(stmt[:len(stmt)/2])
        self.ins.feed(stmt[len(stmt)/2:])
        lst = list(self.ins.next())
        self.assertEqual(1, len(lst))
        self.assertEqual('INSERT INTO "mytable" VALUES (1,2,3,4);', lst[0])


    def test_feed_split_2(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4),(5,6,7,8)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(2, len(lst))
        self.assertEqual('INSERT INTO "mytable" VALUES (1,2,3,4);', lst[0])
        self.assertEqual('INSERT INTO "mytable" VALUES (5,6,7,8);', lst[1])


    def test_feed_split_3(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4),(5,6,7,8),(15,16,17,18)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual('INSERT INTO "mytable" VALUES (1,2,3,4);', lst[0])
        self.assertEqual('INSERT INTO "mytable" VALUES (5,6,7,8);', lst[1])
        self.assertEqual('INSERT INTO "mytable" VALUES (15,16,17,18);', lst[2])
        self.assertEqual(3, len(lst))


    def test_feed_split_1_false_2(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,'a 2001(3),(4), b',4)"
        stmt = fmt.format(values)
        self.ins.feed(stmt[:len(stmt)/2])
        self.ins.feed(stmt[len(stmt)/2:])
        lst = list(self.ins.next())
        self.assertEqual('''INSERT INTO "mytable" VALUES (1,'a 2001(3),(4), b',4);''', lst[0])
        self.assertEqual(1, len(lst), lst)


    def test_feed_split_2_false_3(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4),(5,'\\',77,88),(55,\\'ins\\',',7,8)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(lst[0], '''INSERT INTO "mytable" VALUES (1,2,3,4);''')
        self.assertEqual(lst[1], """INSERT INTO "mytable" VALUES (5,''',77,88),(55,''ins'',',7,8);""")
        self.assertEqual(2, len(lst), lst)


    def test_quote_double(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = r"(1,'\\\'\\\'qwerty\\\'\\\'',8)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(lst[0], r"""INSERT INTO "mytable" VALUES (1,'\\''\\''qwerty\\''\\''',8);""")


    def test_double_quote(self):
        fmt = "INSERT INTO `mytable` VALUES ({});"
        values = r"1,'\"qwerty\"',8"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(lst[0], 'INSERT INTO "mytable" VALUES (1,\'"qwerty"\',8);')


    def test_quote_recursive(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = r"(1,'\\\\\\\'qwerty\\\\\\\'',8)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(lst[0], r"""INSERT INTO "mytable" VALUES (1,'\\\\\\''qwerty\\\\\\''',8);""")


    def test_sub_quote(self):
        s = r"' qwerty \'dfssdf\\aaa\\''"
        r = self.ins.re_quote.sub(self.ins.match_quote, s)
        self.assertEqual(r"' qwerty ''dfssdf\\aaa\\''", r)
        s = r"'\'one\''"
        r = s.replace(r"\'", "''")
        self.assertEqual("'''one'''", r)
        s = r"182,'\'one\'',''"
        r = s.replace(r"\'", "''")
        self.assertEqual("182,'''one''',''", r)
        #return
        s = r"182,'\'Та сторона\'','','Альманах','','',0,'','','','\0','','','','','','','',47560,''"
        #r = self.ins.sub_quote("''", s)
        r = self.ins.re_quote.sub(self.ins.match_quote, s)
        #r = s.replace(r"\'", "''")
        #print
        #print s
        #print r
        #print r"182,'''Та сторона''','','Альманах','','',0,'','','','\0','','','','','','','',47560,''"
        self.assertEqual(r"182,'''Та сторона''','','Альманах','','',0,'','','','\0','','','','','','','',47560,''", r)


class TestColumn(unittest.TestCase):


    def test_BIT(self):
        col = Column.match("`colname` BIT,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER')

        col = Column.match("`colname` BIT(4) NOT NULL,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertEqual(col.collen, [4])
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertEqual(col.notnull, "NOT NULL")
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER NOT NULL')


    def test_INT(self):
        col = Column.match("`colname` INTEGER,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertIsNone(col.key)
        self.assertIsNone(col.comment)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER')

        line = "`colname` INTEGER DEFAULT '1' COMMENT 'a string' COLUMN_FORMAT FIXED,"
        col = Column.match(line)
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertIsNone(col.key)
        self.assertEqual(col.comment, "'a string'", line)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER DEFAULT 1')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER DEFAULT 1')

        col = Column.match("`colname` INTEGER UNIQUE,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.key, "UNIQUE")
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER UNIQUE')

        col = Column.match("`colname` INTEGER UNIQUE KEY,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.key, "UNIQUE KEY")
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER UNIQUE')

        col = Column.match("`colname` INTEGER PRIMARY KEY,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.key, "PRIMARY KEY")
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER PRIMARY KEY')

        col = Column.match("`colname` INTEGER NOT NULL AUTO_INCREMENT,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNotNone(col.notnull)
        self.assertIsNotNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER AUTOINCREMENT')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER NOT NULL AUTOINCREMENT')

        col = Column.match("`colname` INTEGER NOT NULL DEFAULT '13',")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNotNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.default, 13)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER DEFAULT 13')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER NOT NULL DEFAULT 13')

        col = Column.match("`colname` TINYINT UNSIGNED,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'SMALLINT')
        self.assertIsNone(col.collen)
        self.assertIsNotNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" SMALLINT')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" SMALLINT')

        col = Column.match("`colname` TINYINT(4) ZEROFILL,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'SMALLINT')
        self.assertEqual(col.collen, [4])
        self.assertIsNone(col.unsigned)
        self.assertIsNotNone(col.zerofill)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" SMALLINT')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" SMALLINT')
        self.assertIsNone(col.notnull)

        col = Column.match("`colname` INT(4) UNSIGNED ZEROFILL,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertEqual(col.collen, [4])
        self.assertIsNotNone(col.unsigned)
        self.assertIsNotNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER')



    def test_REAL(self):
        for mysqltype in ("REAL", "FLOAT"):
            col = Column.match("`colname` {} UNSIGNED,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'REAL')
            self.assertIsNone(col.collen)
            self.assertIsNotNone(col.unsigned)
            self.assertIsNone(col.zerofill)
            self.assertIsNone(col.notnull)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" REAL')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" REAL')

            col = Column.match("`colname` {}(4,12) ZEROFILL,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'REAL')
            self.assertEqual(col.collen, [4, 12])
            self.assertIsNone(col.unsigned)
            self.assertIsNotNone(col.zerofill)
            self.assertIsNone(col.notnull)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" REAL')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" REAL')

            col = Column.match("`colname` {}( 4, 12 ) UNSIGNED ZEROFILL DEFAULT '13.14',".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'REAL')
            self.assertEqual(col.collen, [4, 12])
            self.assertIsNotNone(col.unsigned)
            self.assertIsNotNone(col.zerofill)
            self.assertIsNone(col.notnull)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" REAL DEFAULT 13.14')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" REAL DEFAULT 13.14')

            col = Column.match("`colname` {}( 4, 12 ) UNSIGNED ZEROFILL NOT null,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'REAL')
            self.assertEqual(col.collen, [4, 12])
            self.assertIsNotNone(col.unsigned)
            self.assertIsNotNone(col.zerofill)
            self.assertEqual(col.notnull, "NOT null")
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" REAL')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" REAL NOT NULL')


    def test_DOUBLE(self):
        col = Column.match("`colname` DOUBLE UNSIGNED,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'DOUBLE')
        self.assertIsNone(col.collen)
        self.assertIsNotNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" DOUBLE')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" DOUBLE')

        col = Column.match("`colname` DOUBLE(4,12) ZEROFILL,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'DOUBLE')
        self.assertEqual(col.collen, [4, 12])
        self.assertIsNone(col.unsigned)
        self.assertIsNotNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" DOUBLE')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" DOUBLE')

        col = Column.match("`colname` DOUBLE(4,12) UNSIGNED ZEROFILL,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'DOUBLE')
        self.assertEqual(col.collen, [4, 12])
        self.assertIsNotNone(col.unsigned)
        self.assertIsNotNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" DOUBLE')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" DOUBLE')


    def test_NUMERIC(self):
        for mysqltype in ("DECIMAL", "NUMERIC"):
            col = Column.match("`colname` {} UNSIGNED,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'NUMERIC')
            self.assertIsNone(col.collen)
            self.assertIsNotNone(col.unsigned)
            self.assertIsNone(col.zerofill)
            self.assertIsNone(col.notnull)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" NUMERIC')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" NUMERIC')

            col = Column.match("`colname` {}(4,12) ZEROFILL,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'NUMERIC')
            self.assertEqual(col.collen, [4, 12])
            self.assertIsNone(col.unsigned)
            self.assertIsNotNone(col.zerofill)
            self.assertIsNone(col.notnull)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" NUMERIC')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" NUMERIC')

            col = Column.match("`colname` {}( 4, 12 ) UNSIGNED ZEROFILL,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'NUMERIC')
            self.assertEqual(col.collen, [4, 12])
            self.assertIsNotNone(col.unsigned)
            self.assertIsNotNone(col.zerofill)
            self.assertIsNone(col.notnull)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" NUMERIC')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" NUMERIC')


    def test_DATE(self):
        col = Column.match("`colname` DATE,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'DATE')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" DATE')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" DATE')


    def test_TIME(self):
        col = Column.match("`colname` TIME,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'TIME')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" TIME')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" TIME')


    def test_TIMESTAMP(self):
        col = Column.match("`colname` TIMESTAMP,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'TIMESTAMP')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" TIMESTAMP')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" TIMESTAMP')


    def test_DATETIME(self):
        col = Column.match("`colname` DATETIME,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'DATETIME')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" DATETIME')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" DATETIME')


    def test_YEAR(self):
        col = Column.match("`colname` YEAR,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'INTEGER')
        self.assertIsNone(col.collen)
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" INTEGER')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" INTEGER')



    def test_CHAR(self):
        for mysqltype in ("CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"):
            col = Column.match("`colname` {},".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'TEXT')
            self.assertIsNone(col.collen)
            self.assertIsNone(col.unsigned)
            self.assertIsNone(col.zerofill)
            self.assertIsNone(col.notnull)
            self.assertIsNone(col.autoincrement)
            self.assertIsNone(col.charset)
            self.assertIsNone(col.collate)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" TEXT')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" TEXT')

            col = Column.match("`colname` {}(4) NOT NULL,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'TEXT')
            self.assertEqual(col.collen, [4])
            self.assertIsNone(col.unsigned)
            self.assertIsNone(col.zerofill)
            self.assertEqual(col.notnull, "NOT NULL")
            self.assertIsNone(col.autoincrement)
            self.assertIsNone(col.charset)
            self.assertIsNone(col.collate)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" TEXT')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" TEXT NOT NULL')

            col = Column.match("`colname` {}(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'TEXT')
            self.assertEqual(col.collen, [4])
            self.assertIsNone(col.unsigned)
            self.assertIsNone(col.zerofill)
            self.assertEqual(col.notnull, "NOT NULL")
            self.assertIsNone(col.autoincrement)
            self.assertEqual(col.charset, "utf8")
            self.assertEqual(col.collate, "utf8_unicode_ci")
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" TEXT')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" TEXT NOT NULL')


    def test_BINARY(self):
        for mysqltype in (
                        "BINARY", "VARBINARY",
                        "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
                        "TINYTEXT BINARY", "TEXT BINARY", "MEDIUMTEXT BINARY", "LONGTEXT BINARY"
                        ):
            line = "`colname` {},".format(mysqltype)
            col = Column.match(line)
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'BLOB', line)
            self.assertIsNone(col.collen)
            self.assertIsNone(col.unsigned)
            self.assertIsNone(col.zerofill)
            self.assertIsNone(col.notnull)
            self.assertIsNone(col.autoincrement)
            self.assertIsNone(col.charset)
            self.assertIsNone(col.collate)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" BLOB')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" BLOB')

            col = Column.match("`colname` {} NOT NULL,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'BLOB')
            self.assertIsNone(col.collen)
            self.assertIsNone(col.unsigned)
            self.assertIsNone(col.zerofill)
            self.assertEqual(col.notnull, "NOT NULL")
            self.assertIsNone(col.autoincrement)
            self.assertIsNone(col.charset)
            self.assertIsNone(col.collate)
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" BLOB')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" BLOB NOT NULL')

            col = Column.match("`colname` {} CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,".format(mysqltype))
            self.assertEqual(col.colname, 'colname')
            self.assertEqual(col.coltype, 'BLOB')
            self.assertIsNone(col.collen)
            self.assertIsNone(col.unsigned)
            self.assertIsNone(col.zerofill)
            self.assertEqual(col.notnull, "NOT NULL")
            self.assertIsNone(col.autoincrement)
            self.assertEqual(col.charset, "utf8")
            self.assertEqual(col.collate, "utf8_unicode_ci")
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" BLOB')
            self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" BLOB NOT NULL')


    def test_ENUM(self):
        col = Column.match("`colname` ENUM('small', 'medium', 'large') NOT NULL,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'TEXT')
        self.assertEqual(col.originaltype, 'ENUM')
        self.assertEqual(col.collen, ["'small'", "'medium'", "'large'"])
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNotNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" TEXT')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" TEXT NOT NULL')


    def test_SET(self):
        col = Column.match("`colname` SET('small', 'medium', 'large') NOT NULL,")
        self.assertEqual(col.colname, 'colname')
        self.assertEqual(col.coltype, 'TEXT')
        self.assertEqual(col.originaltype, 'SET')
        self.assertEqual(col.collen, ["'small'", "'medium'", "'large'"])
        self.assertIsNone(col.unsigned)
        self.assertIsNone(col.zerofill)
        self.assertIsNotNone(col.notnull)
        self.assertIsNone(col.autoincrement)
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=True), '"colname" TEXT')
        self.assertEqual(col.sql(flavor="sqlite", skip_constraints=False), '"colname" TEXT NOT NULL')



class TestConstraint(unittest.TestCase):


    def test_PRIMARY_KEY(self):
        col = Constraint("CONSTRAINT `symbol` PRIMARY KEY (`index_col_name1`,`index_col_name2`),")
        self.assertTrue(isinstance(col, PrimaryKey))
        self.assertEqual(col.symbol, 'symbol')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name1", "index_col_name2"])
        self.assertEqual(col.sql(flavor="sqlite"), 'PRIMARY KEY ("index_col_name1", "index_col_name2")')
        self.assertIsNone(col.index("tbl_name", flavor="sqlite"))


    def test_PRIMARY_KEY_type(self):
        col = Constraint("CONSTRAINT `symbol` PRIMARY KEY USING BTREE (`index_col_name1`,`index_col_name2`),")
        self.assertTrue(isinstance(col, PrimaryKey))
        self.assertEqual(col.symbol, 'symbol')
        self.assertEqual(col.indextype, "USING BTREE")
        self.assertEqual(col.indexcols, ["index_col_name1", "index_col_name2"])
        self.assertEqual(col.sql(flavor="sqlite"), 'PRIMARY KEY ("index_col_name1", "index_col_name2")')
        self.assertIsNone(col.index("tbl_name", flavor="sqlite"))


    def test_KEY(self):
        col = Constraint("KEY `index_name` (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertIsNone(col.unique)
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertEqual(col.index("tbl_name", flavor="sqlite"), 'CREATE INDEX "index_name" ON "tbl_name" ("index_col_name")')
        self.assertIsNone(col.sql(flavor="sqlite"))


    def test_KEY_2(self):
        col = Constraint("KEY `index_name` (`index_col_name1`,`index_col_name2`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name1", "index_col_name2"])
        self.assertIsNone(col.unique)
        self.assertEqual(col.index("tbl_name", flavor="sqlite"), 'CREATE INDEX "index_name" ON "tbl_name" ("index_col_name1", "index_col_name2")')
        self.assertIsNone(col.sql(flavor="sqlite"))


    def test_KEY_type(self):
        col = Constraint("KEY `index_name` USING BTREE (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertEqual(col.indextype, "USING BTREE")
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNone(col.unique)
        self.assertEqual(col.index("tbl_name", flavor="sqlite"), 'CREATE INDEX "index_name" ON "tbl_name" ("index_col_name")')
        self.assertIsNone(col.sql(flavor="sqlite"))


    def test_KEY_2_type(self):
        col = Constraint("KEY `index_name` USING HASH (`index_col_name1`,`index_col_name2`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertEqual(col.indextype, "USING HASH")
        self.assertEqual(col.indexcols, ["index_col_name1", "index_col_name2"])
        self.assertIsNone(col.unique)
        self.assertEqual(col.index("tbl_name", flavor="sqlite"), 'CREATE INDEX "index_name" ON "tbl_name" ("index_col_name1", "index_col_name2")')
        self.assertIsNone(col.sql(flavor="sqlite"))


    def test_INDEX(self):
        col = Constraint("INDEX `index_name` (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNone(col.unique)
        self.assertEqual(col.index("tbl_name", flavor="sqlite"), 'CREATE INDEX "index_name" ON "tbl_name" ("index_col_name")')
        self.assertIsNone(col.sql(flavor="sqlite"))


    def test_INDEX_2(self):
        col = Constraint("INDEX `index_name` (`index_col_name1`,`index_col_name2`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name1", "index_col_name2"])
        self.assertIsNone(col.unique)
        self.assertEqual(col.index("tbl_name", flavor="sqlite"), 'CREATE INDEX "index_name" ON "tbl_name" ("index_col_name1", "index_col_name2")')
        self.assertIsNone(col.sql(flavor="sqlite"))


    def test_INDEX_type(self):
        col = Constraint("INDEX `index_name` USING BTREE (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertEqual(col.indextype, "USING BTREE")
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNone(col.unique)
        self.assertEqual(col.index("tbl_name", flavor="sqlite"), 'CREATE INDEX "index_name" ON "tbl_name" ("index_col_name")')
        self.assertIsNone(col.sql(flavor="sqlite"))


    def test_INDEX_2_type(self):
        col = Constraint("INDEX `index_name` USING HASH (`index_col_name1`,`index_col_name2`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertEqual(col.indextype, "USING HASH")
        self.assertEqual(col.indexcols, ["index_col_name1", "index_col_name2"])
        self.assertIsNone(col.unique)
        self.assertEqual(col.index("tbl_name", flavor="sqlite"), 'CREATE INDEX "index_name" ON "tbl_name" ("index_col_name1", "index_col_name2")')
        self.assertIsNone(col.sql(flavor="sqlite"))


    def test_UNIQUE_INDEX(self):
        col = Constraint("UNIQUE INDEX `index_name` (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.symbol)
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNotNone(col.unique)
        self.assertEqual(col.sql(flavor="sqlite"), 'UNIQUE ("index_col_name")')
        self.assertIsNone(col.index("tbl_name", flavor="sqlite"))


    def test_UNIQUE_KEY(self):
        col = Constraint("UNIQUE KEY `index_name` (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNotNone(col.unique)
        self.assertEqual(col.sql(flavor="sqlite"), 'UNIQUE ("index_col_name")')
        self.assertIsNone(col.index("tbl_name", flavor="sqlite"))


    def test_FULLTEXT_INDEX(self):
        col = Constraint("FULLTEXT INDEX `index_name` (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNotNone(col.fulltext)


    def test_FULLTEXT_KEY(self):
        col = Constraint("FULLTEXT KEY `index_name` (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNotNone(col.fulltext)


    def test_SPATIAL_INDEX(self):
        col = Constraint("SPATIAL INDEX `index_name` (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNotNone(col.fulltext)


    def test_SPATIAL_KEY(self):
        col = Constraint("SPATIAL KEY `index_name` (`index_col_name`),")
        self.assertTrue(isinstance(col, Index))
        self.assertEqual(col.indexname, 'index_name')
        self.assertIsNone(col.indextype)
        self.assertEqual(col.indexcols, ["index_col_name"])
        self.assertIsNotNone(col.fulltext)


    def test_FOREIGN_KEY(self):
        col = Constraint("CONSTRAINT `symbol` FOREIGN KEY (`col_name1`,`col_name2`) REFERENCES `tbl_name` (`f_col_name1`,`f_col_name2`),")
        self.assertTrue(isinstance(col, ForeignKey))
        self.assertIsNotNone(col.fkey)
        self.assertEqual(col.symbol, 'symbol')
        self.assertEqual(col.indexcols, ["col_name1", "col_name2"])


    def test_CHECK(self):
        col = Constraint(" CHECK (col_name1>col_name2),")
        self.assertIsNotNone(col.check)
        self.assertEqual(col.check, "(col_name1>col_name2)")


    def test_no_match(self):
        s= ") ENGINE=InnoDB AUTO_INCREMENT=237442 DEFAULT CHARSET=utf8;"
        col = Constraint(s)
        self.assertIsNone(col)



class TestTable(unittest.TestCase):


    def setUp(self):
        self.col1 = "  `pnb` varchar(254) DEFAULT '' COMMENT 'UNIMARC.personal name $b',"
        self.col2 = " `aid` int(10) unsigned NOT NULL AUTO_INCREMENT,"
        self.col3 = "  PRIMARY KEY (`aid`) USING BTREE,"
        self.col4 = " `aid` int(10) unsigned NOT NULL AUTO_INCREMENT  PRIMARY KEY,"


    def test_create(self):
        tbl = Table.match("CREATE TABLE `my_table` (")
        self.assertEqual(tbl.name, 'my_table')
        tbl = Table.match("CREATE  TABLE     `tbl_name`")
        self.assertEqual(tbl.name, 'tbl_name')
        tbl = Table.match("CREATE TEMPORARY TABLE `tbl_name`")
        self.assertEqual(tbl.name, 'tbl_name')
        tbl = Table.match("CREATE   TABLE IF NOT EXISTS `tbl_name`")
        self.assertEqual(tbl.name, 'tbl_name')
        tbl = Table.match("CREATE TEMPORARY TABLE IF NOT EXISTS `tbl_name`")
        self.assertEqual(tbl.name, 'tbl_name')
        tbl = Table.match("CREATE TEMPORARY TABLE IF NOT EXISTS `tbl_name` (")
        self.assertEqual(tbl.name, 'tbl_name')

        self.assertRaises(Exception, tbl.sql)


    def test_match_end(self):
        tbl = Table.match("CREATE TABLE `my_table` (")
        #tbl.feed(") ENGINE=InnoDB AUTO_INCREMENT=237442 DEFAULT CHARSET=utf8;")
        stmt = ") ENGINE=InnoDB AUTO_INCREMENT=237442 DEFAULT CHARSET=utf8;"
        res = tbl.match_end(stmt)
        self.assertIsNotNone(res)


    def test_empty(self):
        tbl = Table.match("CREATE TABLE `my_table` (")
        tbl.feed(") ENGINE=InnoDB AUTO_INCREMENT=237442 DEFAULT CHARSET=utf8;")
        self.assertTrue(tbl.done)


    def test_1(self):
        tbl = Table.match("CREATE TABLE `my_table` (")
        tbl.feed("`field1` int")
        tbl.feed(") ENGINE=InnoDB AUTO_INCREMENT=237442 DEFAULT CHARSET=utf8;")
        self.assertTrue(tbl.done)
        self.assertEqual(
                         tbl.sql(flavor="sqlite", skip_constraints=True),
        '''CREATE TABLE "my_table" (
    "field1" INTEGER
);''')
        self.assertEqual(
                         tbl.sql(flavor="sqlite", skip_constraints=False),
        '''CREATE TABLE "my_table" (
    "field1" INTEGER
);''')


    def test_1_UNIQUE(self):
        tbl = Table.match("CREATE TABLE `my_table` (")
        tbl.feed("`field1` int UNIQUE")
        tbl.feed(") ENGINE=InnoDB AUTO_INCREMENT=237442 DEFAULT CHARSET=utf8;")
        self.assertTrue(tbl.done)
        self.assertEqual(
                         tbl.sql(flavor="sqlite", skip_constraints=True),
        '''CREATE TABLE "my_table" (
    "field1" INTEGER
);''')
        self.assertEqual(
                         tbl.sql(flavor="sqlite", skip_constraints=False),
        '''CREATE TABLE "my_table" (
    "field1" INTEGER UNIQUE
);''')


    #def test_match_key_1(self):
        #key = " KEY `KeyName` (`FirstName`(20)), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '"KeyName"')
        #self.assertEqual(res[1], None)
        #self.assertEqual(res[2], '"FirstName"')
#
#
    #def test_match_key_2(self):
        #key = " KEY `KeyName` (`col1`,`col2`), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '"KeyName"')
        #self.assertEqual(res[1], None)
        #self.assertEqual(res[2], '"col1","col2"')
#
#
    #def test_match_primary_key_1(self):
        #key = " PRIMARY KEY `KeyName` (`FirstName`(20)), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '"KeyName"')
        #self.assertEqual(res[1], "PRIMARY")
        #self.assertEqual(res[2], '"FirstName"')
#
#
    #def test_match_primary_key_2(self):
        #key = " PRIMARY KEY (`bid`,`aid`), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '')
        #self.assertEqual(res[1], "PRIMARY")
        #self.assertEqual(res[2], '"bid","aid"')
#
#
    #def test_match_primary_key_3(self):
        #key = " PRIMARY KEY USING BTREE (`bid`,`aid`), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '')
        #self.assertEqual(res[1], "PRIMARY")
        #self.assertEqual(res[2], '"bid","aid"')
#
#
    #def test_match_unique_key_1(self):
        #key = " UNIQUE KEY `FirstName` (`FirstName`(20)), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '"FirstName"')
        #self.assertEqual(res[1], "UNIQUE")
        #self.assertEqual(res[2], '"FirstName"')
#
#
    #def test_match_unique_key_2(self):
        #key = " UNIQUE KEY `FirstName` (`FirstName`, `Page`), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '"FirstName"')
        #self.assertEqual(res[1], "UNIQUE")
        #self.assertEqual(res[2], '''"FirstName", "Page"''')
#
#
    #def test_match_unique_key_3(self):
        #key = " UNIQUE KEY `FirstName` USING BTREE (`FirstName`, `Page`), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '"FirstName"')
        #self.assertEqual(res[1], "UNIQUE")
        #self.assertEqual(res[2], '"FirstName", "Page"')
#
#
    #def test_match_foreign_key(self):
        #key = " FOREIGN KEY `KeyName` (`FirstName`(20)), "
        #res = self.tbl.match_key(key)
        #self.assertIsNotNone(res)
        #self.assertEqual(res[0], '"KeyName"')
        #self.assertEqual(res[1], "FOREIGN")
        #self.assertEqual(res[2], '"FirstName"')



if __name__ == "__main__":
    unittest.main()

