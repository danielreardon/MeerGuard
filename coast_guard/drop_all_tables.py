#!/usr/bin/env python

"""
Script to drop all tables.
Based on http://www.sqlalchemy.org/trac/wiki/UsageRecipes/DropEverything

Patrick Lazarus, Oct 30, 2012
"""

import database

db = database.Database()
db.engine.echo = True

# gather all data first before dropping anything.
# some DBs lock after things have been dropped in 
# a transaction.

metadata = database.sa.schema.MetaData()

tbs = []
all_fks = []

for table in db.metadata.tables.values():
    tbs.append(table)
    all_fks.extend([c for c in table.constraints if \
                    isinstance(c, database.sa.ForeignKeyConstraint)])

with db.transaction() as conn:
    for fkc in all_fks:
        try:
            conn.execute(database.sa.schema.DropConstraint(fkc))
        except:
            print "Skipping foreign key: %s" % fkc
    for table in tbs:
        try:
            conn.execute(database.sa.schema.DropTable(table))
        except:
            print "Skipping table: %s" % table

