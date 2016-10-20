#!/usr/bin/env python 
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
		#self._input_key_port=input_keys # keys = dictionary of  variable_name: list_of_link_id
		self._input_key_port={}
		for key in input_keys:
			self._input_key_port[key]=""
		#self._output_key_portlist=output_keys # keys = dictionary of  variable_name: list_of_link_id 
		self._output_key_portlist={}
		for key in output_keys:
			self._output_key_portlist[key]=[]

		# values
		self._input_values={}
		for key in input_keys:
			self._input_values[key]=""
		self._output_values={}
		for key in output_keys:
			self._output_values[key]=""

		# function to be called
		self._func=func
		self._finished=0

	def operation_OR(self):
		    print "enter",self.operation_OR.__name__
		    db=simpledb
		    values={}
                    found=0
		# assume that len of _output_key_portlist is 1
		    for key in self._output_key_portlist:
				outputid = self._output_key_portlist[key]  # outputid is a list 
				if len(outputid)==0:
					print "error in operation_OR"
					print "outputid is null"
					sys.exit(100000)
	
                    for key in self._input_key_port:
                        id_=self._input_key_port[key]
                        if id_ in db:
				values[id_]=db[id_]
				self._input_values[key]= values[id_]
				found= 1

		# collect data
		    values2=[]
		    for v in values:
				values2.append(values[v])

	  	    if found:
			for key in self._output_values: 
				self._output_values[key]=values2
			for id_ in outputid:
				db[id_]=values2

		    return found

	def operation_OUTPUT(self):
		    print "enter",self.operation_OUTPUT.__name__
		    self.port2inputvalue()
		    print self._input_values



	def check_and_start(self):
		first_status=self._finished
		print "trying",self.check_and_start.__name__, self._myname,first_status
		db=simpledb
		found=0
		if self._finished==1:
			return first_status+self._finished

		if isinstance(self._func,basestring):
			if self._func=="OR":
				found=self.operation_OR()
				self._finished=1
			elif self._func=="OUTPUT":
				found=self.operation_OUTPUT()
				self._finished=1
		else:
		      found=1
		      for key in self._input_key_port:
			id_=self._input_key_port[key]
			if id_ in db:
				found=found and 1
			else:
				found=found and 0 
		      if found:
				self.port2inputvalue()
				self._output_values = self._func(self._input_values,self._output_values)
				print self.check_and_start.__name__, "output_values=",self._output_values
				self.outputvalue2port()
				self._finished=1
		print "fin",self.check_and_start.__name__, self._myname,first_status,self._finished
		return first_status+self._finished

        def port2inputvalue(self,errorstop=False):
		print "coming",self.port2inputvalue.__name__
        	for key in self._input_key_port:
			port=self._input_key_port[key]
			if port in simpledb:
				value = simpledb[port]
				self._input_values[key]=value
			else:
			    if errorstop:
				print "failed to find",key, "in self._input_values"
				print "name=",self._myname
				print "in ", self.port2inputvalue.__name__
				sys.exit(500000)
		print self.port2inputvalue.__name__, "inputvalue=",self._input_values

	def outputvalue2port(self):
		print "coming",self.outputvalue2port.__name__
		print self.outputvalue2port.__name__, "outputvalue=",self._output_values
        	for key in self._output_values: 
                	value=self._output_values[key]
                	print "key=",key,"values",value
                	for port in self._output_key_portlist[key] :
                        	simpledb[ port ] = value

	def force_start(self):
                self._output_values = self._func(self._input_values,self._output_values)
                self.outputvalue2port()
                self._finished=1
		return 1

	def show(self):
		#print self._myname,self._input_key_port,self._output_key_portlist,self._input_values,self._output_values, self._finished
		print self._myname,self._func,self._input_values,self._output_values, self._finished
			


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
		if parent_key in parent_node._output_key_portlist:
			parent_node._output_key_portlist[parent_key].append( id_ )
		else:
			print "failed to connect",parent ,"with", id
			sys.exit(1000)
		child_node=child[0]
		child_key=child[1]
		if child_key in child_node._input_key_port:
			child_node._input_key_port[child_key] = id_
		else:
			print "failed to connect",child ,"with", id
			sys.exit(1001)


class JobnodeList():
	""" a list of jobode"""
	def __init__(self):
		self._list=[]
	def append(self,node):
		self._list.append(node)
	def check_and_start(self):
		for node in self._list:
			ret= node.check_and_start()
			print self.check_and_start.__name__,"ret=",ret
			if ret==1:
				return 
	def show(self):
		for x in self._list:
			x.show()

class RunnableNode:
	""" accept input and output """
	def __init__(self,args):
		self._input_key_port=args[0]
		self._output_key_portlist=args[1]

		self._input_values={}
		for key in self._input_key_port:
			id_=self._input_key_port[key]
			value=simpledb[id_]
			self._input_values[key]=value

		print "run"
		print self._input_key_port
		print self._input_values

		print self._output_key_portlist

	def writeoutput(self, outputvalues):
                self._output_values={}
                for key in self._output_key_portlist:
                        id_=self._output_key_portlist[key]
                        simpledb[id_]=output_values[key]

def test3():
	nodelist=JobnodeList()

        node1= JobNode ("node1", [],funcA,["x","y"] )
        node2= JobNode ("node2", ["a","b"],funcB,["x"] )
        node3= JobNode ("node3", ["a","b"],"OR",["x"])
	node4= JobNode("node4", ["a"],"OUTPUT",[]) 
	node5= JobNode("node5",["a"],funcC,["x"])

        graph=JobNetwork()
        graph.define([node1,"x"],[node2,"a"])
        graph.define([node1,"y"],[node2,"b"])
	graph.define([node1,"y"],[node3,"a"])
	graph.define([node5,"x"],[node3,"b"])
	graph.define([node3,"x"],[node4,"a"])

	print "-------------------------node status"
	nodelist.show()
	print "simpledb",simpledb

	nodelist._list.append(node1)
	nodelist._list.append(node2)
	nodelist._list.append(node3)
	nodelist._list.append(node4)
	nodelist._list.append(node5)

	print "-------------------------node1.sart()"
	node1.show()
	node1.force_start()
	print "simpledb",simpledb

	for i in range(3):
		nodelist.check_and_start()

		print "-------------------------node status",i
		nodelist.show()
		print "simpledb",simpledb

def funcA(*args):
	print "running ",funcA.__name__

	print args
	i=1
	output={}
	for x in args[1]:
		output[x]=str(i*100)
		i+=1
	print funcA.__name__,"output=",output
	return output

def funcB(*args):
	print "running ",funcB.__name__

	print args
	i=1
	output={}
	for x in args[1]:
		print "arg=(",x,")"
		output[x]=str(i*10)
	print funcB.__name__,"output=",output
	return output

def funcC(*args):
        print "running ",funcB.__name__

        print args
        i=1
        output={}
        for x in args[1]:
                print "arg=(",x,")"
                output[x]=str(i*10)
        print funcB.__name__,"output=",output
        return output


test3()


