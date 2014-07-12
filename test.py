import unittest
from mysql2db import Insert



class TestInsert(unittest.TestCase):

    def setUp(self):
        self.ins = Insert()


    def test_empty(self):
        self.assertEqual(0, len(list(self.ins.next())))


    def test_feed_1(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(1, len(lst))
        self.assertEqual('INSERT INTO "mytable" VALUES (1,2,3,4);', lst[0])


    def test_feed_1_lower(self):
        fmt = "insert into `MYTABLE` values {};"
        values = "(1,2,3,4)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(1, len(lst))
        self.assertEqual('INSERT INTO "MYTABLE" VALUES (1,2,3,4);', lst[0])


    def test_feed_1_split(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4)"
        stmt = fmt.format(values)
        self.ins.feed(stmt[:len(stmt)/2])
        self.ins.feed(stmt[len(stmt)/2:])
        lst = list(self.ins.next())
        self.assertEqual(1, len(lst))
        self.assertEqual('INSERT INTO "mytable" VALUES (1,2,3,4);', lst[0])


    def test_feed_2(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4),(5,6,7,8)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        self.assertEqual(2, len(lst))
        self.assertEqual('INSERT INTO "mytable" VALUES (1,2,3,4);', lst[0])
        self.assertEqual('INSERT INTO "mytable" VALUES (5,6,7,8);', lst[1])


    def test_feed_2r(self):
        fmt = "INSERT INTO `mytable` VALUES {};"
        values = "(1,2,3,4),(5,'\\',77,88),(55,\\'ins\\',',7,8)"
        self.ins.feed(fmt.format(values))
        lst = list(self.ins.next())
        print lst
        self.assertEqual(2, len(lst))



if __name__ == "__main__":
    unittest.main()
    
