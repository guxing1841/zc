"""\
Minimum heap and maximum heap
Copyright (C) Zhou Changrong
"""

class heap_node():
	def __init__(self, data = None):
		self.index = -1
		if data != None:
			self.data = data
		else:
			self.data = None
def LEFT(i):
	return 2*(i+1)-1
def RIGHT(i):
	return 2*(i+1)
def PARENT(i):
	return (i+1)/2-1
class heap():
	def node(self, data = None):
		"""node(data = None) -> heap_node object"""
		return heap_node(data)
		
	def __init__(self, size = 128, compar = cmp):
		if size > 0:
			self.size = size
		else:
			self.size = 128
		self.heap = [None] * self.size
		self.compar = compar
		self.last_index = -1

	def up(self, index):
		node = self.heap[index]
		parent = PARENT(index)
		while (index > 0 and self.compar(node.data, self.heap[parent].data) < 0):
			self.heap[index] = self.heap[parent]
			self.heap[index].index = index
			index = parent
			parent = PARENT(index)
		node.index = index
		self.heap[index] = node

	def down(self, index):
		node = self.heap[index]
		last_index = self.last_index;
		while True:
			left = LEFT(index)
			right = RIGHT(index)
			if (left > last_index):
				break
			elif (right == last_index or self.compar(self.heap[left].data, self.heap[right].data) <= 0):
				min = left
			else:
				min = right
			if (self.compar(node.data, self.heap[min].data) <= 0):
				break
			self.heap[min].index = index
			self.heap[index] = self.heap[min]
			index = min
		node.index = index
		self.heap[index] = node

	def insert(self, node):
		"""insert(node) -> bool
Insert a heap_node object to heap"""
		if (self.last_index+1 >= self.size):
			self.heap.extend([None] * self.size)
			self.size += self.size
		self.last_index += 1
		self.heap[self.last_index] = node
		node.index = self.last_index
		self.up(self.last_index)
		return True

	def delete(self, node):
		"""delete(node) -> bool
Delete a heap_node object from heap"""

		index = node.index
		last_index = self.last_index
		if (index < 0 or last_index < 0 or index > last_index):
			return False
		if (self.heap[index] != node):
			return False
		self.last_index -= 1
		if index != last_index:
			self.heap[index] = self.heap[last_index]
			self.heap[index].index = index
			self.up(index)
			self.down(index)
		node.index = -1
		self.heap[last_index] = None
		return True
	def top(self):
		"""top() -> top node"""
		if (self.last_index < 0):
			return None
		return self.heap[0]
		
	
		
