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


	def operation_OR(self):
		    print "oepration_OR",self._myname
		    db=simpledb
		    values={}
                    found=0
		    for key in self._output_keys:
				outputid = self._output_keys[key]
				if len(outputid)==0:
					print "error in operation_OR"
					print "outputid is null"
					sys.exit(100000)
	
                    for key in self._input_keys:
                        id_=self._input_keys[key]
                        print "check",key,"and",id_
                        if id_ in db:
				values[id_]=db[id_]
				print "id=",id_,"value=",values
				print "_outputkeys=",self._output_keys
				found= 1

	  	    if found:
				print "found and set outputid=", outputid, "value=",values
				db[outputid]=values
		    return found


	def check_and_start(self):
		db=simpledb
		found=0
		print "check",self._input_keys

		if isinstance(self._func,basestring):
			print "string",self._func
			if self._func=="OR":
				found=self.operation_OR()
		else:
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
        node3= JobNode ("node3", {"a":"","b":""},"OR",{"x":"100"} )

        graph=JobNetwork()
        graph.define("1",[node1,"x"],[node2,"a"])
        graph.define("2",[node1,"x"],[node2,"b"])
	graph.define("3",[node1,"y"],[node3,"a"])
	graph.define("4",[node2,"x"],[node3,"b"])

	print "node1"; node1.show()
	print "node2"; node2.show()

	joblist._list.append(node1)
	joblist._list.append(node2)
	joblist._list.append(node3)

	#node2.check_and_start()
	joblist.check_and_start()
	simpledb["1"]="100"
	simpledb["2"]="200"
	#node2.check_and_start()
	joblist.check_and_start()

	simpledb["3"]="300"
	simpledb["4"]="400"

	print "simpledb",simpledb

	joblist.check_and_start()

	print "simpledb",simpledb

def funcA():
	print "running fundA"
def funcB():
	print "running fundB"


test3()


