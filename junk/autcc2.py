#!/usr/bin/env python

import pymongo
from bson.objectid import ObjectId
import sys
import os
import hashlib
import copy
import datetime
import subprocess


class hashGenerator:
        def __init__(self,key=""):
                #self._hash_generator=hashlib.md5()
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


class JobnodeTemplate:
	""" job node temprate"""
	def __init__(self,myname,input_keys,func,output_keys, node_id,input_operation_type,dynamic):
		dic={"template_name":myname,"input_keys":input_keys, "func":func, "output_keys":output_keys, 
			"template_node_id":node_id, "input_operation_type":input_operation_type,
			"dynamic":dynamic}
		ret = simpledb.insert_one(dic)

class JobNode:
	""" job node difinition"""
	def __init__(self,myname,input_keys,func,output_keys, node_id="",input_operation_type="1",dynamic=False):
		""" input_operation_type = 1
					N_AND
					N_OR"""
		#make_job_node_template = JobnodeTemplate(myname,input_keys,func,output_keys, 
		#	node_id,input_operation_type,dynamic)
		if len(node_id)==0:
			node_id=hash_generator.get(myname)

		self._dic={ "node_id":node_id, "myname":myname, "func":func,"input_operation_type":input_operation_type ,"dynamic":dynamic,"graph":[] }
		input_port,output_port,input_values,output_values = self.initial_param()
		self._dic.update({ "input_ports":input_port, "output_ports":output_port, "input_values":input_values, "output_values":output_values})
		self._dic["status"]="created" 
		self._dic=simpledb.insert_and_find_one(self._dic)

	def initial_param(self):
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
		return input_port,output_port,input_values,output_values

	def reset_data(self,node_id=""):
		# change status to the initial state
		# link is regenerated
		node=simpledb.find_one( {"myname": self._dic["myname"]} )
		graph_def=node._dic["graph"]
		input_port,output_port,input_values,output_values = self.initial_param()
		self._dic.update({ "input_ports":input_port, "output_ports":output_port, "input_values":input_values, "output_values":output_values})
		self._dic["status"]="created" 
		
		graph=JobNetwork()
		for s,t,c in graph_def:
			if c=="dynamic":
				graph.define(g[0],g[1])

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

		#if iop=="1":
		if True:
			#check_all_the_port
			print "check input port"
			iport=InputPortOperation(self._dic["input_ports"],self._dic["input_values"],iop=iop)
			found,values=iport.get() 
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
				print
				print "calling process"
				print "inputvalues=",inputvalues
				print "outputvalues=",inputvalues
				#outputvalues=self._func(inputvalues,outputvalues)
				cmd=self._dic["func"]
				print "cmd=",cmd
				retcode=subprocess.call(cmd,shell=True)
				print "outputvalues=",inputvalues
				print 
				self._dic["output_values"]=outputvalues
				self._dic["status"]="finished"
				self.update_data()

				oport=OutputPortOperation(self._dic["output_values"],self._dic["output_ports"])
				oport.put()

				if self._dic["status"]=="finished":
					#change status
					self.reset_data()

		return initialstate +  self.state2number()


class InputPortOperation:
	""" format = 
	{"_id":ObjectId(...), "link_id": link_id, "value": somevalue } """
	def __init__(self,portlist,valuelist,iop="1"):
		self._portlist=portlist
		self._valuelist=valuelist
		self._iop=iop
	def get(self):
		if self._iop=="1":
			ret=self.get1()
		elif self._iop=="N_AND":
			ret=self.getN_AND()
		elif self._iop=="N_OR":
			ret=self.getN_OR()
		else:
			print "iop error", self._iop
			sys.exit(1000)

		return ret
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

	def getN_OR(self):
                valuelist=[]
                found=0
                #for var in self._valuelist:  # assume that len(self._valuelist)==1
		if True:
		    var=self._valuelist.keys()[0]
		
                    links=self._portlist[var]
		    print "links=",links
		    for link in links:
                        value=simpledb.find_one({"link_id":link})
			print "found , link, value=",value
                        if value:
                                valuelist.append(value["value"])
                                found = found or 1
                        else:
                                valuelist.append(None)
                                found = found or 0

                return found , {var:valuelist}

        def getN_AND(self):
		print "getN_AND,start, valuelist",self._valuelist
                valuelist=[]
                found=1
                #for var in self._valuelist:  # assume that len(self._valuelist)==1
                if True:
                    var=self._valuelist.keys()[0]
		    print "portlist",self._portlist,self._portlist[var]

                    links=self._portlist[var]
                    for link in links:
                        value=simpledb.find_one({"link_id":link})
			print "found , link, value=",value
                        if value:
                                valuelist.append(value["value"])
                                found = found and 1
                        else:
                                valuelist.append(None)
                                found = found and 0

		print "getN_AND,result",found,valuelist
                return found , {var:valuelist}



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


class jobNetworkTemplate:
	""" template into DB"""
	def __init__(self,parent,child,id_,dynamic):
		parent_node =parent[0]
		parent_key =parent[1]
		child_node =child[0]
		child_key =child[1]
		dic{"jobnetwork_parent_node": parent_node,
		"jobnetwork_parent_key": parent_key,
		"jobnetwork_child_node": child_node,
		"jobnetwork_child_key": child_key,
		"template_id": id_,
		"dynamic":dynamic }
		simpledb.insert_on(dic)
		
class JobNetwork:
        """define network. This is a helper class.
          real network in defined in node and DB """
        def __init__(self):
                self._network=()
                pass
        def define(self,parent,child,id_="", creation_type="static"):

		#register=jobNetworkTemplate(parent,child,id_,dynamic)

                if len(id_)==0:
                        link_id=hash_generator.get()  
                        """ is is string"""
                parent_node=parent[0]
		parent_node.get_data()
		parent_node._dic["graph"].append([ parent,child,creation_type ] )
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
		child_node._dic["graph"].append([ parent,child,creation_type ] )
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


def test1():

	funcB="python funcB.py"
	funcmerge="python funcmerge.py"
	funcOR="python funcOR.py"

        node1= JobNode("node1", [],funcB,["x1","y1"] )
        #node3= JobNode("node3", ["a3"],funcB,["x3","y3"])
        #node4= JobNode("node4", ["a4","b4"],funcB,["x4"])
        #node5= JobNode("node5", ["a5","b5"],funcB,["x5"])

        graph=JobNetwork() 
        #graph.define([node1,"y1"],[node3,"a3"])
        #graph.define([node3,"x3"],[node4,"a4"])
        #graph.define([node3,"y3"],[node4,"b4"])
        #graph.define([node1,"y1"],[node5,"b5"])
        #graph.define([node4,"x4"],[node5,"a5"])

        nodelist=JobnodeList()
        nodelist.append(node1)
        #nodelist.append(node3)
        #nodelist.append(node4)
        #nodelist.append(node5)

	if False:
            node2= JobNode("node2", ["a2"],funcB,["x2"] )
            graph.define([node1,"x1"],[node2,"a2"])
            nodelist.append(node2)
	    loopmerge=JobNode("loopmerge",["m1"],funcmerge,[],input_operation_type="N_AND")
	    for i in range(4):
		name="loop"+str(i)
		nodeloop=JobNode(name , ["i1"],funcB,["o1"])
		graph.define(["node2","x2"],[name,"i1"])
		graph.define([name,"o1"],["loopmerge","m1"])
		nodelist.append(nodeloop)
	    nodelist.append(loopmerge)

	if True:
                node2= JobNode("node2", ["a2"],funcB,["x2"] )
                graph.define(["node1","x1"],["node2","a2"])
                nodelist.append(node2)
		node3=JobNode("node3",["a3"],funcOR,["x3"],input_operation_type="N_OR",dynamic=True)
		node4=JobNode("node4",["a4"],funcB,["x4"],dynamic=True)
		graph.define(["node2","x2"],["node3","a3"])
		graph.define(["node3","x3"],["node4","a4"])
		graph.define(["node4","x4"],["node3","a3"])
		nodelist.append(node3)
		nodelist.append(node4)


        print "-------------------------nodelist.show()"

        nodelist.show()
        print "<-----------simpledb.show"
	simpledb.show()

        print "-------------------------start()"

        for i in range(7):
                print "<-------------------------node status",i
                nodelist.start()

                print "<-------------------------nodelist.show",i
                nodelist.show()
                print "<-----------simpledb.show"
		simpledb.show()
                print ">-------------------------node status",i


test1()


