# Common utils used in different scripts
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_session(db, echo=False):
    ''' create db connection and sqlalchemy engine
    return a session to interact with db
    currently sqlite but not for long

    echo defaults to False, but if you want for debugging,
    just pass echo=True and sql statements will print to console
    '''
    # grab the folder where this script lives
    basedir = os.path.abspath(os.path.dirname(__file__))
    DATABASE = db
    # define the full path for the database
    DATABASE_PATH = os.path.join(basedir, DATABASE)
    engine = create_engine(f'sqlite:///{DATABASE_PATH}', echo=echo)
    Session = sessionmaker(bind=engine)

    return Session()


if __name__ == '__main__':
    print('Why are you running this? It should only be imported.')
