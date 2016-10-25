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


class DBbase(object):
	""" wrapper for mongoDB"""
	def __init__(self,my_database,my_collection,mainkey=""):
		self._client = pymongo.MongoClient('localhost', 27017)
		self._db = self._client[my_database]
		self._co = self._db[my_collection]
		#self._co_history = self._db[my_collection+"_history"]
		self._mainkey=mainkey
	def insert_one(self,cond):
		return self._co.insert_one(cond)
	def find_one(self,dic):
		value=dic[self._mainkey]
		r = self._co.find_one({self._mainkey:value})
		print "r=",r
		return r
	def update(self,cond):
		target=cond[self._mainkey]
		ret= self._co.update({self._mainkey:target},cond)
		r=self._co.find({self._mainkey:target})
	def find(self,value):
		datalist=[]
		for data in self._co.find({self._mainkey:value}):
			datalist.append( data )
		return datalist
	def insert_and_find_one(self,cond):
		value=cond[self._mainkey]
                ret=self.insert_one(cond)
                return self.find_one({self._mainkey:value})
	def show(self,cond={}):
                print self._client, self._db, self._co, self._mainkey
		for i,data in enumerate(self.find(cond)):
			print i,data
	def save(self,cond):
		self._co.save(cond)
	def drop(self):
		self._co.drop()

class dataFlowDB(DBbase):
        """ wrapper for mongoDB"""
        def __init__(self,my_database="nodedb",my_collection="dataflow",main_key="data_id"):
		super(dataFlowDB,self).__init__(my_database,my_collection)


class JobnodeDB(DBbase):
        """ wrapper for mongoDB"""
        def __init__(self,my_database="nodedb",my_collection="jobnode",main_key="myname"):
		super(JobnodeDB,self).__init__(my_database,my_collection,main_key)

class JobnodeFinishedDB(DBbase):
        """ wrapper for mongoDB"""
        def __init__(self,my_database="nodefinishedDB",my_collection="jobnode",main_key="myname"):
                super(JobnodeFinishedDB,self).__init__(my_database,my_collection,main_key)

class LinkDB(DBbase):
        def __init__(self,my_database="nodedb",my_collection="link",main_key="link_id"):
                super(LinkDB,self).__init__(my_database,my_collection,main_key)



class JobNode:
	""" job node difinition"""
	def __init__(self,myname,input_keys,func,output_keys, node_id="",input_operation_type="1",creation_type="static",data_life="replace"):
		""" input_operation_type = 1
					N_AND
					N_OR"""
	
		self._mainkey="myname"
		self._jobnode_db =JobnodeDB()
		self._jobnode_finished_db =JobnodeFinishedDB()

		if len(node_id)==0:
			node_id=hash_generator.get(myname)
		self._dic={ "node_id":node_id, "myname":myname, "func":func,
		"input_operation_type":input_operation_type ,"creation_type":creation_type,
                 "data_life":data_life }
		self._dic.update(self.template(input_keys,func,output_keys))
		print "dic=",self._dic

		self._dic=self._jobnode_db.insert_and_find_one(self._dic)
		print "__init__",self._dic


	def template(self,input_keys,func,output_keys):
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
		dic ={"input_ports":input_port, "output_ports":output_port, 
		"input_values":input_values, "output_values":output_values,
		"exec_time":None , "exec_id":None,"finished_time":None,"status":"created"}
		return dic


	def show(self,mode="simple"):
		self.get_data()
		if mode=="simple":
			print self._dic["node_id"],self._dic["myname"],self._dic["input_values"],self._dic["output_values"]
		else:
			print self._dic

	def get_data(self):
		self._dic=self._jobnode_db.find_one(self._dic)
		print "get_data",self._dic
		return self._dic

	#def save_data(self,dic=None):
	#	if isinstane(dic,None.Type):
	#		dic=self._dic	
	#	self._jobnode_db.insert_one(dic)

	def update_data(self):
		print "update data", self._dic["node_id"],self._dic["myname"]
		print self._dic
		self._jobnode_db.update(self._dic)

	def reset_data(self):
		self.get_data()
		print "not implemented yet"
		sys.exit(1000000)
		#if self._dic["creation_type"]=="dynamic":
		#	self._dic.update( self.template() )

	def save_finished_data(self):
		print "------------------------------"
		print "save finishd data", self._dic["node_id"],self._dic["myname"]
		print self._dic
		dic=copy.deepcopy(self._dic)
		del dic["_id"]
		self._jobnode_finished_db.insert_one(dic)


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

		print "iop=",iop,self._dic[self._mainkey]

		if True:
			#check_all_the_port
			print "check input port"
			print "input_ports=", self._dic["input_ports"]
			print "output_ports=",self._dic["input_values"]
			iport=InputPortOperation(self._dic["input_ports"],self._dic["input_values"],iop=iop)
			found,values=iport.get() 
			if found:  # now all the data are in the input ports
				# change the status
				print 
				print "start ",self._dic[self._mainkey]
				print 
				self._dic["status"]="running"
				self._dic["input_values"]=values
				today =datetime.datetime.today().__str__()
				self._dic["exec_id"] =  hash_generator.get(self._dic[self._mainkey]+today)
				self._dic["exec_time"]= today
				self.update_data()

				inputvalues=self._dic["input_values"]
				outputvalues=self._dic["output_values"]
				# outputvalues are used to check ouput variables
				print
				print "calling process"
				print "inputvalues=",inputvalues
				print "outputvalues=",inputvalues
				# outputvalues=self._func(inputvalues,outputvalues)
				cmd=self._dic["func"]
				print "cmd=",cmd
				retcode=subprocess.call(cmd,shell=True)
				print "outputvalues=",inputvalues
				print 
				self._dic["output_values"]=outputvalues
				self._dic["status"]="finished"
				self._dic["finished_time"]=today
				self.update_data()

				self.save_finished_data()

				oport=OutputPortOperation(self._dic["output_values"],self._dic["output_ports"])
				oport.put()

				# change state of the node
				

		return initialstate +  self.state2number()


class InputPortOperation:
	""" format = 
	{"_id":ObjectId(...), "link_id": link_id, "value": somevalue } """
	def __init__(self,portlist,valuelist,iop="1"):
		self._portlist=portlist
		self._valuelist=valuelist
		self._iop=iop
		self._dataflowdb=dataFlowDB()
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
			print "get1,var=",var
			print "get1,portlist",self._portlist[var]
			link=self._portlist[var][0] # assume that the number of the link to each port is one
			value=self._dataflowdb.find_one({"dataid":link})
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
                        value=self._dataflowdb.find_one({"data_id":link})
			print "---> found , link, value=",value
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
                        value= self._dataflowdb.find_one({"data_id":link})
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
		self._dataflowdb=dataFlowDB()
	def put(self):
                for var in self._valuelist:
                        portlist=self._portlist[var]
			for link in portlist:
				dic={"data_id":link, "value": self._valuelist[var]}
				print
				print "insert",dic
				print
				self._dataflowdb.insert_one(dic)

class jobNetworkTemplate:
	""" template into DB"""
	def __init__(self,parent,child,id_,creation="static",treatment="replace"):
		parent_node =parent[0]
		parent_key =parent[1]
		child_node =child[0]
		child_key =child[1]
		print "network_init",parent, child
		if len(id_)==0:
			id_=hash_generator.get( "".join([parent_node,parent_key,child_node,child_key]))
		self._dic={"parent_node": parent_node,
		"parent_key": parent_key,
		"child_node": child_node,
		"child_key": child_key,
		"link_id": id_,
		"creation":creation,
		"treatment":treatment }
		
class JobNetwork:
        """define network. This is a helper class.
          real network in defined in node and DB """
        def __init__(self):
                self._network=()
		self._linkdb=LinkDB()

        def define(self,parent,child,id_="",dynamic=False):

		template =jobNetworkTemplate(parent,child,id_,dynamic)
		# register in LinkDB
		self._linkdb.insert_one(template._dic)
		link_id=template._dic["link_id"]
		nodedb=JobnodeDB()

		# I may use array in the fugure to define them at once

		# register in the parent node
                parent_name=parent[0]
		parent_dic= nodedb.find_one({"myname":parent_name})
                parent_key=parent[1]
		print "node--->"
		print parent_dic
		print "<---node"
		print parent_key
		print "output_ports=",parent_dic["output_ports"]
                if parent_key in parent_dic["output_ports"]:
                        parent_dic["output_ports"][parent_key].append( link_id )
                else:
                        print "failed to connect",parent ,"with", link_id
                        sys.exit(1000)
		nodedb.update(parent_dic) 

		# register in the child node
                child_name=child[0]
                child_dic=nodedb.find_one({"myname":child_name})
                child_key=child[1]
                if child_key in child_dic["input_ports"]:
                        child_dic["input_ports"][child_key].append( link_id )
                else:
                        print "failed to connect",child ,"with", link_id
                        sys.exit(1001)
		nodedb.update( child_dic)

		
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

			# if job=finished
			# reset node and reset link 

                        if ret==1:
                                return 
        def show(self):
                for i,x in enumerate(self._list):
                        print i
			x.show()


def test1():

        db0=JobnodeFinishedDB()
	db0.drop()
	db1=JobnodeDB()
	db1.drop()
	db2=LinkDB()
	db2.drop()
	db3=dataFlowDB()
	db3.drop()

	funcB="python funcB.py"
	funcmerge="python funcmerge.py"
	funcOR="python funcOR.py"

        node1= JobNode("node1", [],funcB,["x1","y1"] )
        node3= JobNode("node3", ["a3"],funcB,["x3","y3"])
        node4= JobNode("node4", ["a4","b4"],funcB,["x4"])
        node5= JobNode("node5", ["a5","b5"],funcB,["x5"])

	print "-----------shwo each node----------------"
	print node1._dic["myname"]; node1.show()
	print node3._dic["myname"]; node3.show()
	print node4._dic["myname"]; node4.show()
	print node5._dic["myname"]; node5.show()
	print "---------------------------"

        graph=JobNetwork()
        graph.define(["node1","y1"],["node3","a3"])
        graph.define(["node3","x3"],["node4","a4"])
        graph.define(["node3","y3"],["node4","b4"])
        graph.define(["node1","y1"],["node5","b5"])
        graph.define(["node4","x4"],["node5","a5"])


        nodelist=JobnodeList()
        nodelist.append(node1)
        nodelist.append(node3)
        nodelist.append(node4)
        nodelist.append(node5)

	nodelist.show()


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

	if False:
                node2= JobNode("node2", ["a2"],funcB,["x2"] )
                graph.define(["node1","x1"],["node2","a2"])
                nodelist.append(node2)
		node3=JobNode("node3",["a3"],funcOR,["x3"],input_operation_type="N_OR",creation_type="dynamic")
		node4=JobNode("node4",["a4"],funcB,["x4"],creation_type="dynamic")
		graph.define(["node2","x2"],["node3","a3"])
		graph.define(["node3","x3"],["node4","a4"])
		graph.define(["node4","x4"],["node3","a3"])
		nodelist.append(node3)
		nodelist.append(node4)


        print "-------------------------nodelist.show()"

        nodelist.show()


        for i in range(4):
                print "<-------------------------node status",i
                nodelist.start()

                nodelist.show()
                print "<-----------simpledb.show"


test1()

