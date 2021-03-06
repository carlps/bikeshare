'''
test.test_utils
'''

import unittest

from sqlalchemy.orm.session import Session

from src.utils import get_session

#############
#   Tests   #
#############


class UtilsTestCase(unittest.TestCase):

    def test_get_session_returns_Session(self):
        self.assertIsInstance(get_session(env='TST'), Session)


if __name__ == '__main__':
    unittest.main()
