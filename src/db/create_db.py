''' src.db.create_db
    create all models in postgres db '''


from os import environ

from sqlalchemy import create_engine

from ..models import Base, System_Region, Station_Information
from ..models import Station_Status, Load_Metadata


def get_engine():
    pg_user = environ['POSTGRES_USER']
    pg_pw = environ['POSTGRES_PW']
    host = 'localhost'
    port = '5432'
    db = 'bikeshare'

    return create_engine(f'postgres://{pg_user}:{pg_pw}@{host}:{port}/{db}')


def create_db():
    engine = get_engine()
    Base.metadata.create_all(engine)


def main():
    create_db()


if __name__ == '__main__':
    main()
