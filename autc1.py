import sys
import os
import hashlib


simpledb={}

class hashGenerator:
	def __init__(self):
		self._hash_generator=hashlib.md5()
		self._hash_generator.update("hiori kino")
		self._i=0
	def get(self):
		self._i+=1
		self._hash_generator.update(str(self._i))
		return self._hash_generator.hexdigest()


hash_generator=hashGenerator()


class JobNode: 
	""" job node definition """
	def __init__(self,whoami,input_keys,func,output_keys):
		self._myname=whoami
		# keys to check DB
		self._input_keys=input_keys # keys = dictionary of  variable_name: list_of_link_id
		self._output_keys=output_keys # keys = dictionary of  variable_name: list_of_link_id 
		# function to be called
		self._func=func
		# save values from DB
		self._input_values={}  # dictionary of variable name : value
		self._output_values={}

		self._finished=0

	def operation_OR(self):
		    db=simpledb
		    values={}
                    found=0
		# assume that len of _output_keys is 1
		    for key in self._output_keys:
				outputid = self._output_keys[key]  # outputid is a list 
				if len(outputid)==0:
					print "error in operation_OR"
					print "outputid is null"
					sys.exit(100000)
	
                    for key in self._input_keys:
                        id_=self._input_keys[key]
                        if id_ in db:
				values[id_]=db[id_]
				found= 1

	  	    if found:
				for id_ in outputid:
					db[id_]=values

		    return found


	def check_and_start(self):
		db=simpledb
		found=0

		if isinstance(self._func,basestring):
			if self._func=="OR":
				found=self.operation_OR()
		else:
		    found=1
		    for key in self._input_keys:
			id_=self._input_keys[key]
			if id_ in db:
				found=found and 1
			else:
				found=found and 0 
		    if found:
			print self._input_keys
			print self._output_keys
			if self._finished==0:
				self._func(self._input_keys,self._output_keys)
				self._finished=1
		return found 

	def force_start(self):
		if self._finished==0:
                	self._func(self._input_keys,self._output_keys)
			self._finished=1
		return 1

	def show(self):
		print self._myname,self._input_keys,self._output_keys,self._input_values,self._output_values


class JobNetwork:
	"""define network. This is a helper class.
	  real network in defined in node and DB """
	def __init__(self):
		self._network=()
		pass
	def define(self,parent,child,id_=""):
		if len(id_)==0:
			id_=hash_generator.get()  # is is string 
		parent_node=parent[0]
		parent_key=parent[1]
		if parent_key in parent_node._output_keys:
			parent_node._output_keys[parent_key].append( id_ )
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
		self._list.append(node)
	def check_and_start(self):
		for node in self._list:
			node.check_and_start()

class RunnableNode:
	""" accept input and output """
	def __init__(self,args):
		self._input_keys=args[0]
		self._output_keys=args[1]

		self._input_values={}
		for key in self._input_keys:
			id_=self._input_keys[key]
			value=simpledb[id_]
			self._input_values[key]=value

		print "run"
		print self._input_keys
		print self._input_values

		print self._output_keys

	def writeoutput(self, outputvalues):
                self._output_values={}
                for key in self._output_keys:
                        id_=self._output_keys[key]
                        simpledb[id_]=output_values[key]

def test3():
	joblist=JobList()

        node1= JobNode ("node1", {},funcA,{"x":[],"y":[]} )
        node2= JobNode ("node2", {"a":"","b":""},funcB,{"x":[]} )
        node3= JobNode ("node3", {"a":"","b":""},"OR",{"x":["100"]} )

        graph=JobNetwork()
        graph.define([node1,"x"],[node2,"a"])
        graph.define([node1,"y"],[node2,"b"])
	graph.define([node1,"y"],[node3,"a"])
	graph.define([node2,"x"],[node3,"b"])

	print "-------------------------"
	node1.show()
	node2.show()
	node3.show()
	print "simpledb",simpledb

	joblist._list.append(node1)
	joblist._list.append(node2)
	joblist._list.append(node3)

	print "-------------------------node1.sart()"
	node1.force_start()
	#node2.check_and_start()
	joblist.check_and_start()


	node1.show()
	node2.show()
	node3.show()
	print "simpledb",simpledb

	joblist.check_and_start()

	print "-------------------------"
	node1.show()
	node2.show()
	node3.show()
	print "simpledb",simpledb

def funcA(*args):
	print "running ",funcA.__name__
	input_keys= args[0]
	for key in input_keys:
		print "key=",key,"inputport",input_keys[key]
	output_keys= args[1]
	i=1
        for key in output_keys:
                print "key=",key,"outputport",output_keys[key]
		for port in output_keys[key] : 
			simpledb[ port ] = i*10
		 	i+=1

def funcB(*args):
	print "running ",funcB.__name__
        input_keys= args[0]
        for key in input_keys:
                print "key=",key,"inputport",input_keys[key]
        output_keys= args[1]
	i=1
        for key in output_keys:
                print "key=",key,"outputport",output_keys[key]
		for port in output_keys[key] : 
			simpledb[ port ] = i*100
			i+=1

test3()


