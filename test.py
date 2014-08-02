# -*- coding: utf-8 -*-
import unittest
from mysql2db import Insert, Table



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


class TestTable(unittest.TestCase):


    def setUp(self):
        self.tbl = Table("CREATE TABLE `my_table` (")
        self.col1 = "  `pnb` varchar(254) DEFAULT '' COMMENT 'UNIMARC.personal name $b',"
        self.col2 = " `aid` int(10) unsigned NOT NULL AUTO_INCREMENT,"
        self.col3 = "  PRIMARY KEY (`aid`) USING BTREE,"
        self.col4 = " `aid` int(10) unsigned NOT NULL AUTO_INCREMENT  PRIMARY KEY,"


    def test_create(self):
        self.assertEqual(self.tbl.name, 'my_table')


    def test_re_colmatch(self):
        m = self.tbl.colmatch(self.col1)
        self.assertIsNotNone(m)
        colname, coltype, collen, rest = m.groups()
        colname, coltype, collen, rest = m.groups()
        self.assertEqual(colname, "pnb")
        self.assertEqual(coltype, "varchar")
        self.assertEqual(collen, "254")
        self.assertEqual(rest, "DEFAULT '' COMMENT 'UNIMARC.personal name $b',")


    def test_re_comment(self):
        m = self.tbl.comment(self.col1)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1), "COMMENT 'UNIMARC.personal name $b'")


    def test_re_autoincrement(self):
        self.assertIsNone(self.tbl.autoincrement(self.col1))
        self.assertIsNotNone(self.tbl.autoincrement(self.col2))


    def test_re_notnull(self):
        self.assertIsNone(self.tbl.notnull(self.col1))
        self.assertIsNotNone(self.tbl.notnull(self.col2))


    def test_re_primarykey(self):
        m = self.tbl.primarykey(self.col3)
        self.assertIsNotNone(m)
        pkey = m.group(1)
        self.assertEqual(pkey, "(`aid`)")

        m = self.tbl.primarykey(self.col4)
        self.assertIsNotNone(m)
        pkey = m.group(1)
        self.assertIsNone(pkey)


    def test_match_col_1(self):
        col = "  `bid` int(10) unsigned NOT NULL DEFAULT '0',"
        m = self.tbl.colmatch(col)
        self.assertIsNotNone(m)


    def test_match_col_2(self):
        col = "  `bid` int unsigned NOT NULL DEFAULT '0',"
        m = self.tbl.colmatch(col)
        self.assertIsNotNone(m)


    def test_match_key_1(self):
        key = " KEY `KeyName` (`FirstName`(20)), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '"KeyName"')
        self.assertEqual(res[1], None)
        self.assertEqual(res[2], '"FirstName"')


    def test_match_key_2(self):
        key = " KEY `KeyName` (`col1`,`col2`), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '"KeyName"')
        self.assertEqual(res[1], None)
        self.assertEqual(res[2], '"col1","col2"')


    def test_match_primary_key_1(self):
        key = " PRIMARY KEY `KeyName` (`FirstName`(20)), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '"KeyName"')
        self.assertEqual(res[1], "PRIMARY")
        self.assertEqual(res[2], '"FirstName"')


    def test_match_primary_key_2(self):
        key = " PRIMARY KEY (`bid`,`aid`), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '')
        self.assertEqual(res[1], "PRIMARY")
        self.assertEqual(res[2], '"bid","aid"')


    def test_match_primary_key_3(self):
        key = " PRIMARY KEY USING BTREE (`bid`,`aid`), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '')
        self.assertEqual(res[1], "PRIMARY")
        self.assertEqual(res[2], '"bid","aid"')


    def test_match_unique_key_1(self):
        key = " UNIQUE KEY `FirstName` (`FirstName`(20)), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '"FirstName"')
        self.assertEqual(res[1], "UNIQUE")
        self.assertEqual(res[2], '"FirstName"')


    def test_match_unique_key_2(self):
        key = " UNIQUE KEY `FirstName` (`FirstName`, `Page`), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '"FirstName"')
        self.assertEqual(res[1], "UNIQUE")
        self.assertEqual(res[2], '''"FirstName", "Page"''')


    def test_match_unique_key_3(self):
        key = " UNIQUE KEY `FirstName` USING BTREE (`FirstName`, `Page`), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '"FirstName"')
        self.assertEqual(res[1], "UNIQUE")
        self.assertEqual(res[2], '"FirstName", "Page"')


    def test_match_foreign_key(self):
        key = " FOREIGN KEY `KeyName` (`FirstName`(20)), "
        res = self.tbl.match_key(key)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], '"KeyName"')
        self.assertEqual(res[1], "FOREIGN")
        self.assertEqual(res[2], '"FirstName"')


    def test_match_end(self):
        stmt = ") ENGINE=InnoDB AUTO_INCREMENT=237442 DEFAULT CHARSET=utf8;"
        res = self.tbl.match_end(stmt)
        self.assertIsNotNone(res)



if __name__ == "__main__":
    unittest.main()

