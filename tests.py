#!/usr/bin/env python
# coding: utf8

import unittest, os, shutil
from dalp import DAL, DALplus, RawSQLView
from gluon import *
from time import sleep

working_directory = 'database'
dbname = 'testtest'
testdsn = 'postgres:psycopg2://postgres:postgres@localhost:5432/%(dbname)s' % locals()

class BaseTest(unittest.TestCase):

    def setUp(self):

        # clean working dir before start
        if os.path.exists(working_directory):
            shutil.rmtree(working_directory)
        os.makedirs(working_directory)

        DB = DAL('postgres:psycopg2://postgres:postgres@localhost:5432/postgres')
        DB.set_folder(working_directory)
        db_exists = "SELECT 1 FROM pg_database WHERE datname='%(dbname)s'" % globals()
        res = DB.executesql(db_exists)
        assert res, "Please create the test db called %(dbname)s" % globals()
        db_is_empty = "SELECT COUNT(DISTINCT '%(dbname)s') FROM information_schema.columns WHERE table_schema = '%(dbname)s'" % globals()
        res = DB.executesql(db_is_empty)
        assert res and res[0][0]==0, "WARNING! test db is not empty!"
        self._DB = DB

    def tearDown(self):

        drop_schema = "drop schema public cascade; create schema public;"
        db = DALplus(testdsn, check_reserved=['all'])
        db.executesql(drop_schema)
        db.commit()

        # cleaning working dir after use
        if os.path.exists(working_directory):
            shutil.rmtree(working_directory)
         

class TestRawSQLView(BaseTest):

    def setUp(self):
        BaseTest.setUp(self)
        self.db = DALplus(testdsn, check_reserved=['all'])
        self.tt = self.db.define_table('tt', Field('name'))
        self.db.commit()

    def tearDown(self):
        self.tt.drop('cascade')
        self.db.commit()
        BaseTest.tearDown(self)

    def test_create_view(self):
        
        self.tt.bulk_insert([dict(name='pippo%s' % n) for n in range(10)])
        self.db.commit()
        query = self.tt.name.startswith('pippo')
        res = RawSQLView._create_view('vv', query)
        self.db.commit()
        
        test = "select exists(select * from information_schema.tables where table_name='vv')"
        res = self.db.executesql(test)
        self.assertTrue(res and res[0][0])

class TestDALplus(BaseTest):
 
    def setUp(self):
        BaseTest.setUp(self)
        self.db = DALplus(testdsn)
        self.tt = self.db.define_table('tt', Field('name'))
        self.db.commit()

    def tearDown(self):
        self.tt.drop('cascade')
        self.db.commit()
        BaseTest.tearDown(self)

    def test_fail_init(self):
        """ """
        try:
            DALplus('sqlite://storage.sqlite',pool_size=1,check_reserved=['all'])
        except NotImplementedError:
            res = False
        else:
            res = True
        self.assertFalse(res)

    def test_create_view(self):
        self.tt.bulk_insert([dict(name='pippo%s' % n) for n in range(10)])
        self.db.commit()
        query = self.tt.name.startswith('pippo')
        template = self.db.create_view('vv', query, self.tt.ALL)
        self.db.commit()
        self.db.define_view('vv', template)
        res = self.db(self.db.vv.name.startswith('pippo')).select()
        self.assertEqual(len(res), 10)

def runTest():

    cases = (
        TestRawSQLView,
        TestDALplus
    )

    for case in cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(case)
        unittest.TextTestRunner(verbosity=2).run(suite)

runTest()
