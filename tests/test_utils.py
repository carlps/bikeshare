'''
test.test_utils
'''

import unittest

from sqlalchemy.orm.session import Session

from utils import get_session

class UtilsTestCase(unittest.TestCase):

	def test_get_session_returns_Session(self):
		self.assertTrue(isinstance(get_session(),Session))
		

def run():
	unittest.main()