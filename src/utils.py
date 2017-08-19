# Common utils used in different scripts
from os import environ

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_session(env='DEV', echo=False):
    ''' Create db connection and sqlalchemy engine
        Return a session to interact with db

        Echo defaults to False, but if you want for debugging,
        just pass echo=True and sql statements will print to console'''
    if env == 'DEV':
        pg_user = environ['POSTGRES_USER']
        pg_pw = environ['POSTGRES_PW']
        db = 'bikeshare'
    elif env == 'TST':
        pg_user = environ['POSTGRES_USER_TST']
        pg_pw = environ['POSTGRES_PW_TST']
        db = 'bikeshare_tst'

    host = 'localhost'
    port = '5432'
    engine = create_engine(f'postgres://{pg_user}:{pg_pw}@{host}:{port}/{db}',
                            echo=echo)
    Session = sessionmaker(bind=engine)

    return Session()


if __name__ == '__main__':
    print('Why are you running this? It should only be imported.')
