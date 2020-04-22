#pip install recordclass

from recordclass import make_dataclass
from functools import reduce

class DataBase():
	db_data = {}
	transaction = {}
	tr_nested_level = []
	tr_history = []

	def start(self):
		while True:
			cmd = input('>> ')
			if not cmd: continue
			if cmd=='END': break
			cmd = cmd.split()
			if cmd[0]=='GET' and len(cmd)==2 and cmd[1].isalpha():
				print(data_base.get(cmd[1]))
			elif cmd[0]=='SET' and len(cmd)==3 and cmd[1].isalpha() and cmd[2]!='NULL':
				data_base.set(cmd[1],cmd[2])
			elif cmd[0]=='UNSET' and len(cmd)==2 and cmd[1].isalpha():
				data_base.unset(cmd[1])
			elif cmd[0]=='COUNTS' and len(cmd)==2:
				print(data_base.counts(cmd[1]))
			elif cmd[0]=='BEGIN' and len(cmd)==1:
				data_base.begin()
			elif cmd[0]=='COMMIT' and len(cmd)==1:
				data_base.commit()
			elif cmd[0]=='ROLLBACK' and len(cmd)==1:
				data_base.rollback()
			elif cmd[0]=='HISTORY' and len(cmd)==1:
				print(data_base.history())
			else: print('Incorrect format')

	def get(self, key):
		value = self.transaction.get(key)
		if value:
			return value
		value = self.db_data.get(key)
		if value:
			return value
		else:
			return 'NULL'

	def set(self, key, value):
		if self.tr_nested_level:
			self.transaction[key] = value
		else:
			self.db_data[key] = value

	def unset(self, key):
		if self.tr_nested_level:
				self.transaction[key] = 'NULL'
		elif self.db_data.get(key):
			del self.db_data[key]

	def counts(self, value):
		return len([x for x in self.db_data.values() if x==value])

	def begin(self):
		if self.transaction:
			self.transaction = {}
		self.tr_nested_level.append(self.transaction)

	def commit(self):
		if self.tr_nested_level:
			tr = self.get_tr_for_commit()
			self.db_merge(tr)
			self.tr_history.append(self.dict_to_dataclass(tr))
			self.tr_nested_level = []
			self.transaction = {}

	def rollback(self):
		if len(self.tr_nested_level)==1:
			self.tr_nested_level = []
			self.transaction = {}
		elif len(self.tr_nested_level)>1:
			self.tr_nested_level.pop()
			self.transaction = self.tr_nested_level[-1]

	def history(self):
		print('Transaction history:')
		return(self.tr_history)

	def get_tr_for_commit(self):
		if self.tr_nested_level:
			return reduce(lambda a,b: self.tr_merge(a,b), self.tr_nested_level[::-1])
		return {}

	def tr_merge(self, a, b):
		self.tr_history.append(self.dict_to_dataclass(a))
		return {**a, **b}

	def db_merge(self, tr):
		for key,value in tr.items():
			if value=='NULL' and self.db_data.get(key):
				del self.db_data[key]
			else:
				self.db_data[key] = value

	def dict_to_dataclass(self, dict):
		keys = (x for x in dict.keys())
		values = (x for x in dict.values())
		return make_dataclass(f'tr_{len(self.tr_history)+1}', keys)(*values)

data_base = DataBase()
data_base.start()