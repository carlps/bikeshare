# Common utils used in different scripts
from os import environ

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_session(db='bikeshare', echo=False):
    ''' create db connection and sqlalchemy engine
    return a session to interact with db

    echo defaults to False, but if you want for debugging,
    just pass echo=True and sql statements will print to console
    '''
    pg_user = environ['POSTGRES_USER']
    pg_pw = environ['POSTGRES_PW']
    host = 'localhost'
    port = '5432'
    engine = create_engine(f'postgres://{pg_user}:{pg_pw}@{host}:{port}/{db}',
                            echo=echo)
    Session = sessionmaker(bind=engine)

    return Session()


if __name__ == '__main__':
    print('Why are you running this? It should only be imported.')
