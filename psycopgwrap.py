#!/usr/bin/env python
#
#  A wrapper around the psycopg2 code that makes some things easier.  For
#  example, returns are dictionaries, the "query" method creates and
#  automatically releases cursors.
#
#  You may also want to see Martin Blais' antiorm, which does some similar
#  things: http://furius.ca/antiorm/
#
#  Written by Sean Reifschneider <jafo@tummy.com>
#  Placed in the public domain.
#  No rights reserved.

'''
A wrapper around psycopg2 to make common things easier.

Examples:

	from psycopgwrap import Database as db
	db.connect('dbname=testdb')
	user = db.query('SELECT * FROM users WHERE name = %s', name)
	if user == None:
		print 'No such user "%s"' % name
	print 'User id: %s' % user.id   #  or user.id_
	for row in db.query('SELECT * FROM status WHERE id = %s', 500):
		print 'id:', row.['id']
	
Other examples:

	db.insert('users', name = 'Al Bert')
	db.insert('users', { name : 'Al Bert' })

	user_count = db.queryone('SELECT COUNT(*) FROM users')[0]  #  0-th column

	if db.queryone("SELECT * FROM users WHERE name = 'Al Bert'"):
		print 'Al is in the house'
	else: print 'No such user'

	try: db.commit()
	except: db.rollback()
'''

from psycopg2 import ProgrammingError, InterfaceError


######################
class RowHelper(list):
	'''Helper for the rows that allows for accessing the database row items
	as either a dictionary (row['columnname']), a list (row[0], list(row)),
	or an attribute (row.columnname or row.columnname_ for disambiguation
	from dictionary methods, etc...).
	'''

	########################
	def __init__(self, row):
		#  this is needed so that "[0]" and "list(rowhelper)" work
		super(RowHelper, self).__init__(row)
		self._original_row_object = row

	############################
	def __getattr__(self, attr):
		#  row object attributes override database columns
		if hasattr(self._original_row_object, attr):
			return getattr(self._original_row_object, attr)

		#  database columns prefixed with an underscore
		if attr.endswith('_') and self._original_row_object.has_key(attr[:-1]):
			return(self._original_row_object[attr[:-1]])

		#  database columns
		if self._original_row_object.has_key(attr):
			return(self._original_row_object[attr])

		#  this should raise an AttributeError
		getattr(self._original_row_object, attr)

	#############################
	def __getitem__(self, index):
		#  this is needed so that "['key']" works
		return self._original_row_object[index]


###########################
class CursorHelper(object):
	###########################
	def __init__(self, cursor):
		self.cursor = cursor
		self.currentRecord = None

	###################
	def __iter__(self):
		while self.cursor.rownumber < self.cursor.rowcount:
			yield(self[self.cursor.rownumber])

	#############################
	def __getitem__(self, index):
		if index < 0:
			index = self.cursor.rowcount + index
		if index < self.cursor.rownumber - 1:
			raise NotImplementedError('Cannot get records except sequentially')
		while index >= self.cursor.rownumber:
			row = self.cursor.fetchone()
			if not row:
				self.currentRecord = None
				raise IndexError('list index out of range')

			record = RowHelper(row)
			self.currentRecord = record
		return(self.currentRecord)

	##################
	def __del__(self):
		if self.cursor:
			#  cursor is sometimes already closed
			try: self.cursor.close()
			except InterfaceError: pass
			self.cursor = None


###################
#  database wrapper
class DatabaseClass:
	################
	def __init__(self):
		self.initialized = False
		self.connectString = None


	##################
	def __del__(self):
		self.close()


	########################################
	def connect(self, connectString = None):
		if connectString == None:
			from pgcredentials import connectString
		self.connectString = connectString
		self.setup()


	################
	def setup(self):
		if self.initialized: return
		if not self.connectString:
			raise ValueError('No connection string set, call '
					'"connection(connectString)" first.')

		from psycopg2.extras import DictConnection
		self.connection = DictConnection(self.connectString)
		self.initialized = True


	################
	def close(self):
		if not self.initialized: return
		self.connection.close()
		self.connection = None
		self.initialized = False


	##############################
	def query(self, query, *args):
		self.setup()
		cursor = self.connection.cursor()
		cursor.execute(query, args)
		return(CursorHelper(cursor))


	##########################
	def queryone(self, *args):
		'''Like query(), but if you are expecting only one result.
		Either returns None if there were no results returned, or the row.
		'''
		try:
			ret = self.query(*args)[0]
		except IndexError:
			return(None)
		return(ret)


	###############################################
	def insert(self, table, dict = None, **kwargs):
		'''Insert a row into the specified table, using the keyword arguments
		or dictionary elements as the fields.  If a dictionary is specified
		with keys matching kwargs, then the dictionary takes precedence.
		For example:

		   insert('users', name = 'Sean', uid = 10, password = 'xyzzy')

		will run the SQL:

			INSERT INTO users ( name, uid, password ) VALUES ( 'Sean', 10, 'xyzzy')
		'''
		if dict is not None: kwargs.update(dict)
		values = kwargs.values()
		cmd = ('INSERT INTO %s ( %s ) VALUES ( %s )'
				% ( table, ','.join(kwargs.keys()),
				','.join(['%s'] * len(values)), ))
		self.query(cmd, *values)


	#################
	def commit(self):
		self.setup()
		self.connection.commit()


	###################
	def rollback(self):
		self.setup()
		self.connection.rollback()

Database = DatabaseClass()

##########################
if __name__ == '__main__':
	import sys, unittest

	if not 'test' in sys.argv:
		sys.stderr.write('ERROR: You need to run with the "test" argument to '
				'run test suite\n')
		sys.exit(1)

	class testBase(unittest.TestCase):
		@classmethod
		def setUp(self):
			db = DatabaseClass()
			db.connect('dbname=template1')
			try:
				db.query('END')
				db.query('DROP DATABASE psycopgwraptest')
			except ProgrammingError:
				db.rollback()
			db.query('END')
			db.query('CREATE DATABASE psycopgwraptest')
			db.commit()
			self.db = DatabaseClass()
			self.db.connect('dbname=psycopgwraptest')
			self.db.query('CREATE TABLE indexes ( value INTEGER );')
			for i in xrange(100):
				self.db.query('INSERT INTO indexes ( value ) VALUES ( %s )', i)
			self.db.commit()

		def tearDown(self):
			self.db.close()
			db = DatabaseClass()
			db.connect('dbname=template1')
			db.query('END')
			db.query('DROP DATABASE psycopgwraptest')
			db.commit()

		def test_Indexes(self):
			db = self.db

			count = db.query("SELECT COUNT(*) FROM indexes")[0][0]
			self.assertEqual(count, 100)

			rows = db.query('SELECT * FROM indexes ORDER BY value')
			self.assertEqual(rows[0]['value'], 0)
			self.assertEqual(rows[0]['value'], 0)
			self.assertEqual(rows[1]['value'], 1)
			self.assertEqual(rows[2]['value'], 2)
			self.assertRaises(NotImplementedError, rows.__getitem__, 1)
			self.assertEqual(rows[-4]['value'], 96)
			self.assertEqual(rows[-3]['value'], 97)
			self.assertEqual(list(rows), [[98], [99]])

		def test_Attributes(self):
			db = self.db

			rows = db.query('SELECT * FROM indexes ORDER BY value')
			self.assertEqual(rows[0].value, 0)
			self.assertEqual(rows[0].value, 0)
			self.assertEqual(rows[1].value, 1)
			self.assertEqual(rows[2]._value, 2)
			self.assertRaises(NotImplementedError, rows.__getitem__, 1)
			self.assertEqual(rows[-4]._value, 96)
			self.assertEqual(rows[-3].value, 97)
			self.assertEqual(rows[-3].items(), [('value', 97)])

		def test_One(self):
			db = self.db
			self.assertEqual(
					db.queryone("SELECT * FROM indexes WHERE value = 10"),
					[10])
			self.assertEqual(
					db.queryone("SELECT * FROM indexes WHERE value = 1010"),
					None)

		def test_Insert(self):
			db = self.db
			for i in range(200, 250):
				db.insert('indexes', value = i)
			for i in range(300, 369):
				db.insert('indexes',  { 'value' : i })

			count = db.queryone("SELECT COUNT(*) FROM indexes "
					"WHERE value >= 200 AND value < 300")[0]
			self.assertEqual(count, 50)
			count = db.queryone("SELECT COUNT(*) FROM indexes "
					"WHERE value >= 300 AND value < 400")[0]
			self.assertEqual(count, 69)

	suite = unittest.TestLoader().loadTestsFromTestCase(testBase)
	unittest.TextTestRunner(verbosity=2).run(suite)
