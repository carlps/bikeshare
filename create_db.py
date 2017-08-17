from sys import argv

from src.db import create_db, create_db_tst

if __name__ == '__main__':
    if argv[1] == 'dev' or argv[1] == '-d':
        create_db.main()
    elif argv[1] == 'tst' or argv[1] == '-t':
        create_db_tst.main()
    else:
        print('pass "dev" or "-d" for dev, "tst" or "-t" for test.')
