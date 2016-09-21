#!/usr/bin/env python
import database
import utils
import config


def main():
    engine = database.get_engine(config.dburl)
    database.schema.metadata.create_all(engine)
            # Add this argument to "create_all" to make specific tables:
            # tables=[database.schema.metadata.tables['qctrl'], database.schema.metadata.tables['reattempts']]


if __name__=='__main__':
    parser = utils.DefaultArguments(\
                description="Create database tables.")
    args = parser.parse_args()
    main()
