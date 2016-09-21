import warnings
import string
import re

import sqlalchemy as sa

from coast_guard import config
from coast_guard import errors

import schema
import obslog
from coast_guard import utils

null = lambda x: x
toround_re = re.compile(r"_R(-?\d+)?$")


def fancy_getitem(self, key):
    filterfunc = null
    if (type(key) in (type('str'), type(u'str'))) and key.endswith("_L"):
        filterfunc = string.lower
        key = key[:-2]
    elif (type(key) in (type('str'), type(u'str'))) and key.endswith("_U"):
        filterfunc = string.upper
        key = key[:-2]
    elif (type(key) in (type('str'), type(u'str'))) and toround_re.search(key):
        head, sep, tail = key.rpartition('_R')
        digits = int(tail) if tail else 0
        filterfunc = lambda x: round(x, digits)
        key = head
    elif (type(key) in (type('str'), type(u'str'))) and key.startswith("date:"):
        fmt = key[5:]
        key = 'start_mjd'
        filterfunc = lambda mjd: utils.mjd_to_datetime(mjd).strftime(fmt)
    elif (type(key) in (type('str'), type(u'str'))) and (key == "secs"):
        key = 'start_mjd'
        filterfunc = lambda mjd: int((mjd % 1)*24*3600+0.5)
    if key in self:
        return filterfunc(super(self.__class__, self).__getitem__(key))
    else:
        matches = [k for k in self.keys() if k.startswith(key)]
        if len(matches) == 1:
            return filterfunc(super(self.__class__, self).__getitem__(matches[0]))
        elif len(matches) > 1:
            raise errors.BadColumnNameError("The column abbreviation "
                                            "'%s' is ambiguous. "
                                            "('%s' all match)" %
                                            (key, "', '".join(matches)))
        else:
            raise errors.BadColumnNameError("The column '%s' doesn't exist! "
                                            "(Valid column names: '%s')" %
                                            (key, "', '".join(sorted(self.keys()))))


sa.engine.RowProxy.__getitem__ = fancy_getitem
    

def before_cursor_execute(conn, cursor, statement, parameters, \
                            context, executemany):
    """An event to be executed before execution of SQL queries.

        See SQLAlchemy for details about event triggers.
    """
    # Step back 7 levels through the call stack to find
    # the function that called 'execute'
    msg = str(statement)
    if executemany and len(parameters) > 1:
        msg += "\n    Executing %d statements" % len(parameters)
    elif parameters:
        msg += "\n    Params: %s" % str(parameters)
    utils.print_debug(msg, "queries", stepsback=6)


def on_commit(conn):
    """An event to be executed when a transaction is committed.

        See SQLAlchemy for details about event triggers.
    """
    utils.print_debug("Committing database transaction.", 'database', \
                        stepsback=7)


def on_rollback(conn):
    """An event to be executed when a transaction is rolled back.
        
        See SQLAlchemy for details about event triggers.
    """
    utils.print_debug("Rolling back database transaction.", 'database', \
                        stepsback=7)
        

def on_begin(conn):
    """An event to be executed when a transaction is opened.
        
        See SQLAlchemy for details about event triggers.
    """
    utils.print_debug("Opening database transaction.", 'database', \
                        stepsback=7)


def on_sqlite_connect(dbapi_conn, conn_rec):
    """An even to be execute when sqlite connections
        are established. This turns on foreign key support.

        See SQLAlchemy for details about activating SQLite's
        foreign key support:
        http://docs.sqlalchemy.org/en/rel_0_7/dialects/sqlite.html#foreign-key-support
    
        Inputs:
            dbapi_conn: A newly connected raw DB-API connection 
                (not a SQLAlchemy 'Connection' wrapper).
            conn_rec: The '_ConnectionRecord' that persistently 
                manages the connection.

        Outputs:
            None
    """
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def get_engine(url):
    """Given a DB URL string return the corresponding DB engine.

        Input:
            url: A DB URL string.

        Output:
            engine: The corresponding DB engine.
    """
    # Create the database engine
    engine = sa.create_engine(url)
    if engine.name == 'sqlite':
        sa.event.listen(engine, "connect", on_sqlite_connect)
    sa.event.listen(engine, "before_cursor_execute",
                        before_cursor_execute)
    if config.debug.is_on('database'):
        sa.event.listen(engine, "commit", on_commit)
        sa.event.listen(engine, "rollback", on_rollback)
        sa.event.listen(engine, "begin", on_begin)
    return engine


class Database(object):
    def __init__(self, db='effreduce'):
        """Set up a Database object using SQLAlchemy.

            Inputs:
                db: The name of the database to connect to.
                    Options are 'effreduce' and 'obslog'
                    (Default: effreduce)
        """
        if db == 'effreduce':
            url = config.dburl
            self.metadata = schema.metadata
        elif db == 'obslog':
            url = config.obslog_dburl
            self.metadata = obslog.metadata
        else:
            raise errors.DatabaseError("Database (%s) is not recognized. "
                                       "Cannot connect." % db)
        self.engine = get_engine(url)
        if not self.is_created():
            raise errors.DatabaseError("The database (%s) does not appear " \
                                    "to have any tables. Be sure to run " \
                                    "'create_tables.py' before attempting " \
                                    "to connect to the database." % \
                                            self.engine.url.database)

        # The database description (in metadata)
        self.tables = self.metadata.tables

    def get_table(self, tablename):
        return self.tables[tablename]

    def __getitem__(self, key):
        return self.get_table(key)

    def __getattr__(self, key):
        return self.get_table(key)

    def is_created(self):
        """Return True if the database appears to be setup
            (i.e. it has tables).

            Inputs:
                None

            Output:
                is_setup: True if the database is set up, False otherwise.
        """
        with self.transaction() as conn:
            table_names = self.engine.table_names(connection=conn)
        return bool(table_names)

    def transaction(self, *args, **kwargs):
        """Return a context manager delivering a 'Connection'
            with a 'Transaction' established. This is done by
            calling the 'begin' method of 'self.engine'.

            See http://docs.sqlalchemy.org/en/rel_0_7/core/connections.html
                        #sqlalchemy.engine.base.Engine.begin

            Inputs:
                Arguments are passed directly to 'self.engine.begin(...)'

            Output:
                context: The context manager returned by 
                    'self.engine.begin(...)'
        """
        return self.engine.begin(*args, **kwargs)

    @staticmethod
    def select(*args, **kwargs):
        """A staticmethod for returning a select object.

            Inputs:
                ** All arguments are directly passed to 
                    'sqlalchemy.sql.select'.

            Outputs:
                select: The select object returned by \
                    'sqlalchemy.sql.select'.
        """      
        return sa.sql.select(*args, **kwargs)

