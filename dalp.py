#!/usr/bin/env python
# coding: utf8

reimport = False
try:
    from gluon import *
except ImportError:
    # it comes when test is run
    import sys
    # append the relative path to gluon
    sys.path.append('../../../../')
    from gluon import *

from gluon.dal import DAL, SQLALL
from gluon.storage import Storage

class RawSQLView(object):
    """ """

    q = dict(
        creation = """DROP VIEW IF EXISTS %(name)s CASCADE; CREATE OR REPLACE VIEW %(name)s AS %(sql)s""",
        test = """SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '%(name)s';""",
        get_cols = "SELECT column_name FROM information_schema.columns WHERE table_name='%(tablename)s';",
        select_first = """SELECT * FROM %(name)s LIMIT 1;"""
    )

    @staticmethod
    def cleansql(sql):
        return " ".join(sql.split()).replace('\n', '').strip()

    @classmethod
    def query(cls, id, **kw):
        return cls.cleansql(cls.q[id]) % kw

    @classmethod
    def _create_view(cls, name, query, *args, **kw):
        replace = None if not 'replace' in kw else kw.pop('replace')
        db = query.db
        if replace or len(db.executesql(cls.query('test', name=name)))==0:
            sql = db(query)._select(*args, **kw)
            db.executesql(cls.query('creation', name=name, sql=sql))
        return [i[0] for i in db.executesql(cls.query('get_cols', tablename=name))]

class DALplus(DAL):

    engine = None
    supported_engines = ('postgres', )

    def __init__(self, *args, **kw):
        DAL.__init__(self, *args, **kw)
        engine = self._dbname.split(':')[0]
        if not engine in self.supported_engines:
            raise NotImplementedError('%s is not one of the supported engines: %s' % (self._dbname, ', '.join(self.supported_engines))) 

    def create_view(self, name, query, *args, **kw):
        """ Creates the view on the DB engine;
        returns a Table object you can pass to define_view method to make the 
        view to be recognized by the model.
        """
        RawSQLView._create_view(name, query, *args, **kw)
        fields_e_co = []
        for co in args:
            if isinstance(co, Field):
                fields_e_co.append(co)
            elif isinstance(co, SQLALL):
                fields_e_co.append(co._table)
        if len(fields_e_co)>0:
            return self.Table(self, name, *fields_e_co)
        else:
            return None

    def define_view(self, name, *fields, **kw):
        """ Shortcut for define a table with migrate option set to False """
        return self.define_table(name, *fields, migrate=False, **kw)
