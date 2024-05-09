import os

import duckdb


def createDatabase():
    '''The function createDatabase()
    creates a database 'warehouse.db'
    in the rootdir returns a reference
    to the database.
    '''
    # Create and Connect to the database
    duckdb.connect('warehouse.db')
    return


def main():
    createDatabase()


if __name__ == "__main__":
    main()
