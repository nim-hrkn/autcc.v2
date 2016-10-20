#!/usr/bin/env python 
import sys
import os
import hashlib
import pymongo

class simpleDB: 
	def __init__(self):
		self._client = pymongo.MongoClient('localhost', 27017)
		self._db = self._client.my_database
		self._co = self._db.my_collection

	def insert_one(self,dic):
		print self.insert_one.__name__,dic
        	self._co.insert_one(dic)

	def find_one(self,cond):
		cf=self._co.find_one(cond)
		return cf

	def show(self):
		print "simpeDB.show"
		for x in self._co.find():
			print x

	def drop_collection(self):
		self._co.drop()

simpledb=simpleDB()
simpledb.drop_collection()


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
		""" keys to check DB
		self._input_key_port=input_keys # keys = dictionary of  variable_name: list_of_link_id """
		self._input_key_port={}
		for key in input_keys:
			self._input_key_port[key]=""
		"""self._output_key_portlist=output_keys # keys = dictionary of  variable_name: list_of_link_id  """
		self._output_key_portlist={}
		for key in output_keys:
			self._output_key_portlist[key]=[]

		self._input_values={}
		for key in input_keys:
			self._input_values[key]=""
		self._output_values={}
		for key in output_keys:
			self._output_values[key]=""

		""" function to be called"""
		self._func=func
		self._finished=0

	def operation_OR(self):
		    print "enter",self.operation_OR.__name__
		    values={}
                    found=0
	  	    """ assume that len of _output_key_portlist is 1"""
		    for key in self._output_key_portlist:
				outputid = self._output_key_portlist[key]  # outputid is a list 
				if len(outputid)==0:
					print "error in operation_OR"
					print "outputid is null"
					sys.exit(100000)
	
                    for key in self._input_key_port:
                        id_=self._input_key_port[key]
			dic={"link":id_}
                        #if id_ in db:
			db=simpledb.find_one(dic)
			if db:
				values[id_]=db["value"]
				self._input_values[key]= values[id_]
				found= 1

		    """ collect data """
		    values2=[]
		    for v in values:
				values2.append(values[v])

	  	    if found:
			for key in self._output_values: 
				self._output_values[key]=values2
			for id_ in outputid:
				#db[id_]=values2
				simpledb.insert_one({"link":id_,"value":values2})

		    return found

	def operation_OUTPUT(self):
		    """ output opeartion"""
		    print "enter",self.operation_OUTPUT.__name__
		    self.port2inputvalue()
		    print self._input_values



	def check_and_start(self):
		""" check whether all the input are there and, then run function"""
		first_status=self._finished
		print "trying",self.check_and_start.__name__, self._myname,first_status
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
			dic={"link":id_}
			db=simpledb.find_one(dic)
			if db:
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
			dic={"link":port}
			db=simpledb.find_one(dic)
			if db:
				value = db["value"]
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
                        	#simpledb[ port ] = value
				simpledb.insert_one( {"link": port, "value":value } )

	def force_start(self):
                self._output_values = self._func(self._input_values,self._output_values)
                self.outputvalue2port()
                self._finished=1
		return 1

	def show(self):
		print self._myname,self._func,self._input_values,self._output_values, self._finished
			


class JobNetwork:
	"""define network. This is a helper class.
	  real network in defined in node and DB """
	def __init__(self):
		self._network=()
		pass
	def define(self,parent,child,id_=""):
		if len(id_)==0:
			id_=hash_generator.get()  
			""" is is string"""
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

	def graphviz(self):
		str0="""digraph graph_name {

  graph [
    charset = "UTF-8",
    bgcolor = "#EDEDED",
    rankdir = TB,
    nodesep = 1.1,
    ranksep = 1.05
  ];

  node [
    shape = record,
    fontname = "Migu 1M",
    fontsize = 12,
  ];
"""
		s=""
		for node in self._list:
			t= node._myname
			str1="{"
			input_=[]
			for i in node._input_key_port:
				input_.append("<i_"+i+">"+i)
			str1+= "|".join(input_)
			str1+="}"
			str2=node._myname
			style=""
			if isinstance(node._func,basestring):
				str2+="/"+node._func
				if node._func=="OR":
					style=", style=rounded"
				elif node._func=="OUTPUT":
					style=",  shape=\"invtriangle\" "
			str3="{"
			output_=[]
                        for i in node._output_key_portlist:
                                output_.append("<o_"+i+">"+i)
                        str3+= "|".join(output_)
                        str3+="}"
			if node._func=="OUTPUT":
				t+="  [ label=\""+node._myname+"/"+"|".join([i for i in node._input_key_port])+"\" "+style+" ];\n"
			else:
				t+=" [ label=\"{"+"|".join([str1,str2,str3])+"}\" "+style+" ];\n"

			s+=t

		for node in self._list:
			for key in node._output_key_portlist:
				for oport in node._output_key_portlist[key]:
					iname,iport=self.find_inputlink(oport)
					s+= node._myname+":o_"+key+ " -> "+ iname+":i_"+iport+";\n"
					

		return str0+s+"}"

	def find_inputlink(self,oport):
		for node in self._list:
                        for key in node._input_key_port:
				if oport== node._input_key_port[key]:
					return node._myname,key

		print self.find_inputlink,__name__,"failed to find input port"
		print "inputport=", oport
		sys.exit(40000)



def test3():

        node1= JobNode("node1", [],funcA,["x","y"] )
        node2= JobNode("node2", ["a","b"],funcB,["x"] )
        node3= JobNode("node3", ["a","b"],"OR",["x"])
	node4= JobNode("node4", ["a"],"OUTPUT",[]) 
	node5= JobNode("node5",["a"],funcC,["x"])
	node6= JobNode("node6",["a"],funcD,["z"])
	node7= JobNode("node7",["a"],funcE,["x"])

        graph=JobNetwork()
        graph.define([node1,"x"],[node2,"a"])
        graph.define([node1,"y"],[node2,"b"])
	graph.define([node1,"y"],[node3,"a"])
	graph.define([node5,"x"],[node3,"b"])
	graph.define([node3,"x"],[node4,"a"])
	graph.define([node3,"x"],[node6,"a"])
	graph.define([node6,"z"],[node7,"a"])


	nodelist=JobnodeList()
	nodelist._list.append(node1)
	nodelist._list.append(node2)
	nodelist._list.append(node3)
	nodelist._list.append(node4)
	nodelist._list.append(node5)
	nodelist._list.append(node6)
	nodelist._list.append(node7)

	print "-------------------------node1.sart()"
	node1.show()
	node1.force_start()
	simpledb.show()

	for i in range(5):
		nodelist.check_and_start()

		print "-------------------------node status",i
		nodelist.show()
		simpledb.show()

	
	with open("graph.dot","w") as f:
		s=nodelist.graphviz()
		f.write(s)
		print "graph.dot is made."


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
	input_=[]
	for x in args[0]:
		v=args[0][x]
		input_.append(v)
	output={}
	for x in args[1]:
		v=args[1][x]
		i=input_
		i.extend([funcB.__name__,x])
		output[x]="+".join(i)
	print funcB.__name__,"output=",output
	return output

def funcC(*args):
        print "running ",funcC.__name__

	print args
	input_=[]
	for x in args[0]:
		v=args[0][x]
		input_.append(v)
	output={}
	for x in args[1]:
		v=args[1][x]
		i=input_
		i.extend([funcC.__name__,x])
		output[x]="+".join(i)
	print funcC.__name__,"output=",output
	return output


def funcD(*args):
        print "running ",funcD.__name__

	print args
	input_=[]
	for x in args[0]:
		v=args[0][x]
		input_.append(v[0])
	print "input_",input_
	output={}
	for x in args[1]:
		v=args[1][x]
		i=input_
		i.extend([funcD.__name__,x])
		output[x]="+".join(i)
	print funcD.__name__,"output=",output
	return output




def funcE(*args):
        print "running ",funcE.__name__

	print args
	input_=[]
	for x in args[0]:
		v=args[0][x]
		input_.append(v)
	output={}
	for x in args[1]:
		v=args[1][x]
		i=input_
		i.extend([funcE.__name__,x])
		output[x]="+".join(i)
	print funcE.__name__,"output=",output
	return output




test3()


