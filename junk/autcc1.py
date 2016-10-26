#!/usr/bin/env python

import pymongo
from bson.objectid import ObjectId
import sys
import os
import hashlib
import copy
import datetime


class hashGenerator:
        def __init__(self,key=""):
                #self._hash_generator=hashlib.md5()
		self._firstkey="hiori kino"
		if len(key)==0:
			key=datetime.datetime.today()
			self._firstkey += key.__str__()
		else:
			self._firstkey=key
                #self._hash_generator.update("hiori kino")
                #self._i=0
        def get(self,key=""):
                #self._i+=1
                #self._hash_generator.update(str(self._i))
		if len(key)==0:
			key=datetime.datetime.today().__str__()
                return hashlib.md5(self._firstkey+key).hexdigest()

hash_generator=hashGenerator("hiori kino")



class simpleDB:
	""" wrapper for mongoDB"""
	def __init__(self):
		self._client = pymongo.MongoClient('localhost', 27017)
		self._db = self._client.my_database
		self._co = self._db.my_collection

	def insert_one(self,cond):
		return self._co.insert_one(cond)

	def find_one(self,cond):
		return self._co.find_one(cond)

	def update(self,target,cond):
		ret= self._co.update(target,cond)
		r=self._co.find(target)
		print "update"
		print r
		print 

	def find(self,cond):
		datalist=[]
		for data in self._co.find(cond):
			datalist.append( data )
		return datalist

	def insert_and_find_one(self,cond):
                ret=simpledb.insert_one(cond)
                return simpledb.find_one({"node_id":cond["node_id"]})

	def show(self,cond={}):
		for i,data in enumerate(self.find(cond)):
			print i,data

	def save(self,cond):
		self._co.save(cond)

	def drop(self):
		self._co.drop()

simpledb=simpleDB()
simpledb.drop()


class JobNode:
	""" job node difinition"""
	def __init__(self,myname,input_keys,func,output_keys, node_id="",input_operation_type="1"):
		""" input_operation_type = 1
					N-AND
					N-OR"""
		if len(node_id)==0:
			node_id=hash_generator.get(myname)
		input_port={}
		for x in input_keys:
			input_port[x]= []
		output_port= {}
		for x in output_keys:
			output_port[x]=[]
		input_values={}
		for x in input_keys:
			input_values[x]= None
		output_values={}
		for x in output_keys:
			output_values[x]=None
		self._dic={ "node_id":node_id, "myname":myname,"input_ports":input_port,"output_ports":output_port, "input_values":input_values, "output_values":output_values,"status":"created","input_operation_type":input_operation_type }
		self._dic=simpledb.insert_and_find_one(self._dic)
		self._func=func

	def show(self,mode="simple"):
		if mode=="simple":
			print self._dic["node_id"],self._dic["myname"],self._dic["input_values"],self._dic["output_values"]
		else:
			print self._dic

	def get_data(self):
		self._dic=simpledb.find_one({"node_id":self._dic["node_id"]})

	def save_data(self):
		simpledb.insert_one(self._dic)

	def update_data(self):
		print "update data", self._dic["node_id"],self._dic["myname"]
		print self._dic
		simpledb.update({"node_id":self._dic["node_id"]},self._dic)


	def state2number(self):
		status=self._dic["status"]
                if status=="created":
                        initialstate=0
                elif status=="running":
                        initialstate=-100
                elif status=="finished":
                        initialstate=1
		else:
			print "status error",status
			sys.exit(1000001)
		return initialstate


	def start(self):
		"""check the inputport and start if the condition is fulfilled"""
		self.get_data()
		initialstate=self.state2number()

		iop=self._dic["input_operation_type"]

		print "iop=",iop,self._dic["myname"]

		if iop=="1":
			#check_all_the_port
			print "check input port"
			iport=InputPortOperation(self._dic["input_ports"],self._dic["input_values"])
			found,values=iport.get1() 
			if found:  # now all the data are in the input ports
				# change the status
				print 
				print "start ",self._dic["myname"]
				print 
				self._dic["status"]="running"
				self._dic["input_values"]=values
				self.update_data()

				inputvalues=self._dic["input_values"]
				outputvalues=self._dic["output_values"]
				# outputvalues are used to check ouput variables
				outputvalues=self._func(inputvalues,outputvalues)
				self._dic["output_values"]=outputvalues
				self._dic["status"]="finished"
				self.update_data()

				oport=OutputPortOperation(self._dic["output_values"],self._dic["output_ports"])
				oport.put()

		return initialstate +  self.state2number()


class InputPortOperation:
	""" format = 
	{"_id":ObjectId(...), "link_id": link_id, "value": somevalue } """
	def __init__(self,portlist,valuelist,iop="1"):
		self._portlist=portlist
		self._valuelist=valuelist
		self._iop=iop
	def get1(self):
		valuelist={}
		found=1
		for var in self._valuelist:
			link=self._portlist[var][0] # assume that the number of the link to each port is one
			value=simpledb.find_one({"link_id":link})
			if value:
				valuelist[var]=value["value"]
				found = found and 1
			else:
				valuelist[var]=None
				found = found and 0

		return found , valuelist 

class OutputPortOperation:
	""" send data to the output port"""
	def __init__(self,valuelist,portlist):
		self._portlist=portlist
		self._valuelist=valuelist
	def put(self):
                for var in self._valuelist:
                        portlist=self._portlist[var]
			for link in portlist:
				dic={"link_id":link, "value": self._valuelist[var]}
				print
				print "insert",dic
				print
				simpledb.insert_one(dic)


class JobNetwork:
        """define network. This is a helper class.
          real network in defined in node and DB """
        def __init__(self):
                self._network=()
                pass
        def define(self,parent,child,id_=""):
                if len(id_)==0:
                        link_id=hash_generator.get()  
                        """ is is string"""
                parent_node=parent[0]
		parent_node.get_data()
                parent_key=parent[1]
		print "node"
		print parent_node._dic
		print parent_key
		print "output_ports=",parent_node._dic["output_ports"]
                if parent_key in parent_node._dic["output_ports"]:
                        parent_node._dic["output_ports"][parent_key].append( link_id )
                else:
                        print "failed to connect",parent ,"with", link_id
                        sys.exit(1000)
                child_node=child[0]
		child_node.get_data()
                child_key=child[1]
                if child_key in child_node._dic["input_ports"]:
                        child_node._dic["input_ports"][child_key].append( link_id )
                else:
                        print "failed to connect",child ,"with", link_id
                        sys.exit(1001)

		parent_node.update_data()
		child_node.update_data()

		
class JobnodeList():
        """ a list of jobode"""
        def __init__(self):
                self._list=[]
        def append(self,node):
                self._list.append(node)
        def start(self):
                for node in self._list:
                        ret= node.start()
                        print self.start.__name__,"ret=",ret
                        if ret==1:
                                return 
        def show(self):
                for i,x in enumerate(self._list):
                        print i
			x.show()


def funcB(*args):
        print "running ",funcB.__name__

        print args
        input_=[]
        for x in args[0]:
                v=args[0][x]
                input_.append(v)
        i=1
        output={}
        for x in args[1]:
                v=args[1][x]
                i=copy.deepcopy(input_)
                i.extend([funcB.__name__,x])
                output[x]="+".join(i)
        print funcB.__name__,"output=",output
        return output


def test1():

        node1= JobNode("node1", [],funcB,["x1","y1"] )
        node2= JobNode("node2", ["a2"],funcB,["x2"] )
        node3= JobNode("node3", ["a3"],funcB,["x3","y3"])
        node4= JobNode("node4", ["a4","b4"],funcB,["x4"])
        node5= JobNode("node5", ["a5","b5"],funcB,["x5"])

        graph=JobNetwork()
        graph.define([node1,"x1"],[node2,"a2"])
        graph.define([node1,"y1"],[node3,"a3"])
        graph.define([node3,"x3"],[node4,"a4"])
        graph.define([node3,"y3"],[node4,"b4"])
        graph.define([node1,"y1"],[node5,"b5"])
        graph.define([node4,"x4"],[node5,"a5"])

        nodelist=JobnodeList()
        nodelist.append(node1)
        nodelist.append(node2)
        nodelist.append(node3)
        nodelist.append(node4)
        nodelist.append(node5)

        print "-------------------------nodelist.show()"

        nodelist.show()
        print "<-----------simpledb.show"
	simpledb.show()

        print "-------------------------start()"

        for i in range(5):
                print "<-------------------------node status",i
                nodelist.start()

                print "<-------------------------nodelist.show",i
                nodelist.show()
                print "<-----------simpledb.show"
		simpledb.show()
                print ">-------------------------node status",i


test1()


