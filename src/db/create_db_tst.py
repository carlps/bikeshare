''' src.db.create_db
    create all models in postgres db '''


from os import environ

from sqlalchemy import create_engine

from ..models import Base, System_Region, Station_Information
from ..models import Station_Status, Load_Metadata
from ..utils import get_session


def get_engine():
    pg_user = environ['POSTGRES_USER_TST']
    pg_pw = environ['POSTGRES_PW_TST']
    host = 'localhost'
    port = '5432'
    db = 'bikeshare_tst'

    return create_engine(f'postgres://{pg_user}:{pg_pw}@{host}:{port}/{db}')


def create_db():
    engine = get_engine()
    Base.metadata.create_all(engine)
    create_triggers()


def create_triggers():
    with open('src/db/create_triggers.sql', 'r') as sql_file:
        statements = sql_file.read().split('\n\n')
    session = get_session(env='TST')
    connection = session.connection()
    for sql in statements:
        connection.execute(sql)
    session.commit()
    session.close()


def drop_all_tables():
    engine = get_engine()
    Base.metadata.drop_all(engine)


def main():
    create_db()

if __name__ == '__main__':
    main()
