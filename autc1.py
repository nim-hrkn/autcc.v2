import sys
import os
import hashlib


simpledb={}

hash_generator=hashlib.md5()
hash_generator.update("hiori kino")

class JobNode: 
	""" job node definition """
	def __init__(self,whoami,input_keys,func,output_keys):
		self._myname=whoami
		# keys to check DB
		self._input_keys=input_keys # key=list of [variable_name,id]
		self._output_keys=output_keys
		# function to be called
		self._func=func
		# save values from DB
		self._input_values={}
		self._output_values={}

	def check_and_start(self):
		db=simpledb
		print "check",self._input_keys
		found=1
		for key in self._input_keys:
			id_=self._input_keys[key]
			print "check",key,"and",id_
			if id_ in db:
				print" found", key
				found=found and 1
			else:
				found=found and 0 
		print "start?=",found
		if found:
			print self._myname,"start func"
			self._func()
		return found 

	def show(self):
		print self._myname,self._input_keys,self._output_keys,self._input_values,self._output_values


class JobNetwork:
	"""define network. This is a helper class.
	  real network in defined in node and DB """
	def __init__(self):
		self._network=()
		pass
	def define(self,id_,parent,child):
		parent_node=parent[0]
		parent_key=parent[1]
		if parent_key in parent_node._output_keys:
			parent_node._output_keys[parent_key] = id_
		else:
			print "failed to connect",parent ,"with", id
			sys.exit(1000)
		child_node=child[0]
		child_key=child[1]
		if child_key in child_node._input_keys:
			child_node._input_keys[child_key] = id_
		else:
			print "failed to connect",child ,"with", id
			sys.exit(1001)


class JobList():
	""" a list of jobode"""
	def __init__(self):
		self._list=[]
	def append(self,node):
		print "call append",node
		self._list.append(node)
	def check_and_start(self):
		for node in self._list:
			node.check_and_start()

def test3():
	joblist=JobList()

        node1= JobNode ("node1", {"a":"","b":"","c":""},funcA,{"x":"","y":""} )
        node2= JobNode ("node2", {"a":"","b":""},funcB,{"x":""} )

        graph=JobNetwork()
        graph.define("1",[node1,"x"],[node2,"a"])
        graph.define("2",[node1,"x"],[node2,"b"])
	print "node1"; node1.show()
	print "node2"; node2.show()

	joblist._list.append(node1)
	joblist._list.append(node2)

	#node2.check_and_start()
	joblist.check_and_start()
	simpledb["1"]="100"
	simpledb["2"]="200"
	#node2.check_and_start()
	joblist.check_and_start()


def foo1():
		if parent_output_key in parent_node._output_values:
			child_node._input_values[child_input_key]= parent_node._output_values[parent_output_key]


def funcA():
	print "running fundA"
def funcB():
	print "running fundB"

def test2():
	graph=JobNetwork()
	node1= JobNode ( ("a","b","c"),funcA(),("x","y") )
	node2= JobNode ( ("a","b"),funcB(),("x") )
	node1._output_values["x"]="run"
	graph.set(node1,"x",node2,"a") 
	print node1._output_values
	print node2._input_values

		
def test1():
	"""test of JobNode"""
	dica={}
	dica["a"]="input"
	dica["b"]="input2"
	dicb={}
	dicb["a"]="output"

	node1= JobNode ( ("a","b","c"),funcA,("x") )
	node1.check_start(dica)
	node2= JobNode ( ("a","b"),funcB,("x") )
	node2.check_start(dica)

test3()


