#!/usr/bin/env python

""" Copyright 2016, Hiori Kino

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


from __future__ import print_function

import pymongo
from bson.objectid import ObjectId
import sys
import os
import hashlib
import copy
import datetime
import subprocess
import json
import time

from graphviz import Digraph


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

class funcStyle:
	def __init__(self,func="",inputstyle="",outputstyle=""):
		if len(inputstyle)==0:
			inputstyle={"type":"json", "filename":["_input.json"]}
	
		if len(outputstyle)==0:
			outputstyle={"type":"json","templatefilename":["_outputtemplate.json"],"filename":["_output.json"]}
		self._dic={"cmd":func, "inputstyle":inputstyle, "outputstyle":outputstyle} 
		self._dryrun=False

	def from_dic(self,func):
		self._dic=func

	def run(self,inputvalues,outputvalues):
		#for key in self._dic:
		#	print "key=",key
		#	print self._dic[key]
		#print "self.run()",self._dic
		#print "inputstyle", self._dic["inputstyle"]
		inputfilename=self._dic["inputstyle"]["filename"][0]

		with open(inputfilename,"w") as f:
			json.dump(inputvalues,f)

		outputfilename=self._dic["outputstyle"]["templatefilename"][0]

		with open(outputfilename,"w") as f:
			json.dump(outputvalues,f)

		cmd=self._dic["cmd"]

		ret=-1
		if not self._dryrun:
			ret=subprocess.call(cmd,shell=True)
			print( "subprocess, ret=",ret, "cmd=",cmd )
		if ret!=0:
			sys.exit(30000)

		outputfilename=self._dic["outputstyle"]["filename"][0]
		outputvalues=[]
                with open(outputfilename,"r") as f:
			outputvalues=json.load(f)
			found=1

		return [ret,outputvalues]

	def show(self):
		print( "<funcStyle.show()" )
		print( self._dic )
		print( self._dryrun )
		print( "funcStyle.show()>" )
	

class DBbase(object):
	""" wrapper for mongoDB"""
	def __init__(self,my_database,my_collection,mainkey):
		self._my_database=my_database
		self._my_collection=my_collection
		self._client = pymongo.MongoClient('localhost', 27017)
		self._db = self._client[my_database]
		self._co = self._db[my_collection]
		#self._co_history = self._db[my_collection+"_history"]
		self._mainkey=mainkey
	def delete_many(self,cond):
		return self._co.delete_many(cond)
	def insert_one(self,cond):
		return self._co.insert_one(cond)
	def find_one(self,dic):
		value=dic[self._mainkey]
		r = self._co.find_one({self._mainkey:value})
		return r
	def update(self,cond):
		target=cond[self._mainkey]
		ret= self._co.update({self._mainkey:target},cond)
		r=self._co.find({self._mainkey:target})
		return r
	def find(self,value):
		datalist=[]
		if len(value)==0:
		    for data in self._co.find():
			datalist.append( data )
		else:
		    for data in self._co.find({self._mainkey:value}):
			datalist.append( data )
		return datalist
	def insert_and_find_one(self,cond):
		value=cond[self._mainkey]
                ret=self.insert_one(cond)
                return self.find_one({self._mainkey:value})
	def show(self,cond={}):
                print( self._client, self._db, self._co, self._mainkey )
		for i,data in enumerate(self.find(cond)):
			print( i,data )
	def save(self,cond):
		self._co.save(cond)
	def drop(self):
		self._co.drop()

class dataFlowDB(DBbase):
        """ wrapper for mongoDB"""
        def __init__(self,my_database="nodedb",my_collection="dataflow",main_key="data_id"):
		super(dataFlowDB,self).__init__(my_database,my_collection,main_key)


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


class JobNodeTemplate:
	"""template for JonNode"""
	def __init__(self,input_keys={},output_keys={}):
		self._dic={}
                self._dic={ "node_id":None, "myname":None, "func":None,
                "input_operation_type":None ,"creation_type":None}
		self._dic.update(self.keys_initialvalues(input_keys,output_keys))
		self._mainkey="myname"

		#possible_var = [ {"status":["created","running","finished"]} ]


	def keys_initialvalues(self,input_keys,output_keys):
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


class JobNode:
	""" job node difinition"""
	def __init__(self,myname,input_keys,func,output_keys, node_id="",input_operation_type="1",creation_type="static" ):  #,data_life="replace"):
		""" input_operation_type = 1
					N_AND
					N_OR"""
	
		self._jobnode_db =JobnodeDB()
		self._jobnode_finished_db =JobnodeFinishedDB()

		template=JobNodeTemplate(input_keys,output_keys)
		self._dic=template._dic
		self._mainkey=template._mainkey

		self._accept_dic = {"creation_type": ["dynamic","static"], 
				"input_operation_type":["1","N_AND","N_OR"],
				"status":["created","running","finished","waiting"]}

		if len(node_id)==0:
			node_id=hash_generator.get(myname)
		dic={ "node_id":node_id, "myname":myname, "func":func,
		"input_operation_type":input_operation_type ,"creation_type":creation_type}
		for key in dic:
			#if key in self._dic:
			#	self._dic[key]=dic[key]
			self.check_and_set_dic(key,dic[key])

                # "data_life":data_life }
		#self._dic.update(self.template(input_keys,func,output_keys))


		self._dic=self._jobnode_db.insert_and_find_one(self._dic)



	#def template(self,input_keys,func,output_keys):
	#	input_port={}
	#	for x in input_keys:
	#		input_port[x]= []
	#	output_port= {}
	#	for x in output_keys:
	#		output_port[x]=[]
	#	input_values={}
	#	for x in input_keys:
	#		input_values[x]= None
	#	output_values={}
	#	for x in output_keys:
	#		output_values[x]=None
	#	dic ={"input_ports":input_port, "output_ports":output_port, 
	#	"input_values":input_values, "output_values":output_values,
	#	"exec_time":None , "exec_id":None,"finished_time":None,"status":"created"}
	#	return dic

	def reset_dic(self):
                input_values={}
		input_keys = self._dic["input_values"]
                for x in input_keys:
                	input_values[x]= None
                output_values={}
		output_keys = self._dic["output_values"]
                for x in output_keys:
                        output_values[x]=None
		dic ={ "input_values":input_values, "output_values":output_values,
                "exec_time":None , "exec_id":None,"finished_time":None,"status":"waiting"}
		for  key in dic:
			#if key in self._dic:
			#	self._dic[key]=dic[key]
			self.check_and_set_dic(key,dic[key])
		#self._dic.update(dic)	
		return self._dic 


	def show(self,mode="simple"):
		self.get_data()
		if mode=="simple":
			print( self._dic["node_id"],self._dic["myname"],self._dic["input_values"],self._dic["output_values"] )
		else:
			print( self._dic )

	def get_data(self):
		self._dic=self._jobnode_db.find_one(self._dic)

		return self._dic


	def update_data(self):

		self._jobnode_db.update(self._dic)


	def save_finished_data(self):



		dic=copy.deepcopy(self._dic)
		del dic["_id"]
		print() 
		print( "data_finished",dic["myname"],dic["input_values"],dic["output_values"] )
		print()
		self._jobnode_finished_db.insert_one(dic)


	#def state2number(self):
	#	status=self._dic["status"]
        #        if status=="created":
        #                initialstate=0
        #        elif status=="running":
        #                initialstate=-100
        #        elif status=="finished":
        #                initialstate=1
	#	else:
	#		print( "state2number(),status error",status )
	#		sys.exit(1000001)
	#	return initialstate


	def check_and_set_dic(self,key,value):
		found=False
		if key in self._dic:
			if key in self._accept_dic:
				if value in self._accept_dic[key]:
					found=True
			else:
				found=True
		if found:
			self._dic[key]=value
		else:
			print( "unknown key,key=",key,value )
			sys.exit(30000)


	def start(self):
		"""check the inputport and start if the condition is fulfilled"""
		self.get_data()
		initialstate=self._dic["status"]
		finalstate=initialstate

		iop=self._dic["input_operation_type"]

		#print "iop=",iop,self._dic[self._mainkey], self._dic["status"]

		if initialstate in ["created","waiting"] :
			#check_all_the_port

			iport=InputPortOperation(self._dic["input_ports"],self._dic["input_values"],iop=iop)
			found,values=iport.get() 
			if found:  # now all the data are in the input ports
				# change the status
				print() 
				print( "start ",self._dic[self._mainkey] )
				#print "seld._dic=",self._dic
				print() 
				#self._dic["status"]="running"
				self.check_and_set_dic("status","running")
				#self._dic["input_values"]=values
				self.check_and_set_dic("input_values",values)
				today =datetime.datetime.today().__str__()
				#self._dic["exec_id"] =  hash_generator.get(self._dic[self._mainkey]+today)
				self.check_and_set_dic("exec_id",hash_generator.get(self._dic[self._mainkey]+today))
				#self._dic["exec_time"]= today
				self.check_and_set_dic("exec_time",today)
				self.update_data()

				inputvalues=self._dic["input_values"]
				outputvalues=self._dic["output_values"]
				# outputvalues are used to check ouput variables


				print( "inputvalues=",inputvalues )

				# outputvalues=self._func(inputvalues,outputvalues)
				func=self._dic["func"]

				funcstyle=funcStyle()
				funcstyle.from_dic(func)
				#funcstyle.show()
				ret,outputvalues=funcstyle.run(inputvalues,outputvalues)
				print( "after run, outputvalues=",outputvalues )
				print()
				#self._dic["output_values"]=outputvalues
				self.check_and_set_dic("output_values",outputvalues)
				#self._dic["status"]="finished"
				self.check_and_set_dic("status","finished")
				#self._dic["finished_time"]=today
				self.check_and_set_dic("finished_time",today)
				#self.update_data()

				self.save_finished_data()
				finalstate = self._dic["status"]

				oport=OutputPortOperation(self._dic["output_values"],self._dic["output_ports"])
				oport.put()

				if self._dic["creation_type"]=="static":
					pass
				elif self._dic["creation_type"]=="dynamic":
					self.reset_dic()
				self.update_data()

				iport.reset_data()
				

		return ",".join([initialstate,finalstate])


class InputPortOperation:
	""" format = 
	{"_id":ObjectId(...), "data_id": link_id, "value": somevalue } """
	def __init__(self,portlist,valuelist,iop="1"):
		self._portlist=portlist
		self._valuelist=valuelist
		self._iop=iop
		self._dataflowdb=dataFlowDB()

	def reset_data(self):
		thisfunc="InputPortOperation:reset_data"
		#print thisfunc,"self._valuelist",self._valuelist
		#print thisfunc,"self._portlist",self._portlist
		templatedb=LinkDB()
		for var in self._valuelist:
			linklist=self._portlist[var]
			key= self._dataflowdb._mainkey
			for link in linklist:
				#print
				#print thisfunc,"delete",link
				template_dic=templatedb.find_one({templatedb._mainkey:link})
				#print thisfunc," template_dic",template_dic,"find",{key:link}
				if template_dic["creation"]=="dynamic":
					#print "search",{key:link}
					result=self._dataflowdb.find_one({key:link})
					#print "find",result
					result=self._dataflowdb.delete_many({key:link})
					# error 
					if result.deleted_count>1:
						print( "reset_data, something is wrong", result.deleted_count )
						sys.exit(30000)
				
			
	def get(self):
		if self._iop=="1":
			ret=self.get1()
		elif self._iop=="N_AND":
			ret=self.getN_AND()
		elif self._iop=="N_OR":
			ret=self.getN_OR()
		else:
			print( "iop error", self._iop )
			sys.exit(1000)
		return ret
	def get1(self):
		valuelist={}
		found=1
		for var in self._valuelist:


			link=self._portlist[var][0] # assume that the number of the link to each port is one
			key= self._dataflowdb._mainkey
			value=self._dataflowdb.find_one({key:link})

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
		    #print "links=",links
		    for link in links:
                        value=self._dataflowdb.find_one({"data_id":link})
			#print "---> found , link, value=",value
                        if value:
                                valuelist.append(value["value"])
                                found = found or 1
                        else:
                                valuelist.append(None)
                                found = found or 0

                return found , {var:valuelist}

        def getN_AND(self):
		#print "getN_AND,start, valuelist",self._valuelist
                valuelist=[]
                found=1
                #for var in self._valuelist:  # assume that len(self._valuelist)==1
                if True:
                    var=self._valuelist.keys()[0]
		    #print "portlist",self._portlist,self._portlist[var]

                    links=self._portlist[var]
                    for link in links:
                        value= self._dataflowdb.find_one({"data_id":link})
			#print "found , link, value=",value
                        if value:
                                valuelist.append(value["value"])
                                found = found and 1
                        else:
                                valuelist.append(None)
                                found = found and 0

		#print "getN_AND,result",found,valuelist
                return found , {var:valuelist}



class OutputPortOperation:
	""" send data to the output port"""
	def __init__(self,valuelist,portlist):
		self._portlist=portlist
		self._valuelist=valuelist
		self._dataflowdb=dataFlowDB()
		self._templatedb= LinkDB()
	def put(self):
		thisfunc="OutputPortOperation:put"
		#print thisfunc,"put,value=",self._valuelist
		#print thisfunc,"put,port=", self._portlist
                for var in self._valuelist:
			if self._valuelist[var] is None:
				continue
                        portlist=self._portlist[var]

			for link in portlist:
				key=self._templatedb._mainkey
				template=self._templatedb.find_one({key:link})
				#print thisfunc,"template",template
				if template["treatment"]=="replace":
					dic={"data_id":link, "value": self._valuelist[var]}
					#print
					#print thisfunc,"outputport, clear_insert",dic
					#print
					#print thisfunc,"key,link=",{self._dataflowdb._mainkey:link}
					#print 
					found = self._dataflowdb.find_one({self._dataflowdb._mainkey:link})
					#print thisfunc,"found,value=",found
					if found:
						self._dataflowdb.update(dic)
					else:
						self._dataflowdb.insert_one(dic)
				elif template["treatment"]=="append":
					found = self._dataflowdb.find_one({self._dataflowdb._mainkey:link})
					if found:
						dic["value"].append( self._valuelist[var] )
						self._dataflowdb.update(dic)
					else:
						self._dataflowdb.insert_one(dic)
				else:
					print( "template keyword error" )
					sys.exit(5000)


class jobNetworkTemplate:
	""" template into DB"""
	def __init__(self,parent,child,id_,creation="static",treatment="replace"):
		""" creation = statis|dynamic
			treatment = replace|append  """

		self._accept_dic = { "creation":["dynamic","static"],
				"treatment":["replace","append"] }

		parent_node =parent[0]
		parent_key =parent[1]
		child_node =child[0]
		child_key =child[1]
		#print "network_init",parent, child
		if len(id_)==0:
			id_=hash_generator.get( "".join([parent_node,parent_key,child_node,child_key]))
		self._dic={"parent_node": parent_node,
		"parent_key": parent_key,
		"child_node": child_node,
		"child_key": child_key,
		"link_id": id_,
		"creation":creation,
		"treatment":treatment }
		# set again 
		self.check_and_set_dic("creation",creation)
		self.check_and_set_dic("treatment",treatment)

	def check_and_set_dic(self,key,value):
		found=False
		if key in self._dic:
			if key in self._accept_dic:
				if value in self._accept_dic[key]:
					found=True
			else:
				found=True
		if found:
			self._dic[key]=value
		else:
			print( "failed to find ",key,":",value,"in default list" )
			print( "programming error" )
			sys.exit(500001)
		
class JobNetwork:
        """define network. This is a helper class.
          real network in defined in node and DB """
        def __init__(self):
                self._network=()
		self._linkdb=LinkDB()

	def get_all(self):
		linklist=self._linkdb.find()
		return linklist

        def define(self,parent,child,id_="",creation="static",treatment="replace"):

		template =jobNetworkTemplate(parent,child,id_,creation,treatment)
		# register in LinkDB
		self._linkdb.insert_one(template._dic)
		key="link_id"
		link_id=template._dic[key]
		nodedb=JobnodeDB()

		# I may use array in the fugure to define them at once

		# register in the parent node
                parent_name=parent[0]
		parent_dic= nodedb.find_one({"myname":parent_name})
                parent_key=parent[1]


                if parent_key in parent_dic["output_ports"]:
                        parent_dic["output_ports"][parent_key].append( link_id )
                else:
                        print( "failed to connect",parent ,"with", link_id )
                        sys.exit(1000)
		nodedb.update(parent_dic) 

		# register in the child node
                child_name=child[0]
                child_dic=nodedb.find_one({"myname":child_name})
                child_key=child[1]
                if child_key in child_dic["input_ports"]:
                        child_dic["input_ports"][child_key].append( link_id )
                else:
                        print( "failed to connect",child ,"with", link_id )
                        sys.exit(1001)
		nodedb.update( child_dic)

		
class JobnodeList():
        """ a list of jobode"""
        def __init__(self):
                self._list=[]
		self._graphviz_id=0
        def append(self,node):
                self._list.append(node)
        def start(self):
                for node in self._list:
                        ret= node.start()
                        if ret in ["created,finished","waiting,finished"]:
                                return  True
		return False
        def show(self):
		print( "<jobnodelist.show" )
                for i,x in enumerate(self._list):
			x.show()
		print( "jobnodelist.show>" )

	def make_keylist(self,dic):
		if len(dic)==0:
			return ""
		xlist=[]
		for x in dic:
			xlist.append("<"+x+">"+ x)
		return "{"+"|".join(xlist)+"}"

	def graphviz(self):
		self._graphviz_dryrun= False
		#name = "cluster%03i.dot" % (self._graphviz_id) 
		name= "cluster.dot"
		self._graphviz_id += 1
		g = Digraph('G', filename=name)
		g.attr("node",shape="record")

		for node in self._list:
			dic=node.get_data()
			print ("dic",dic)
			iv=self.make_keylist(dic[ "input_values"])
			ov=self.make_keylist(dic["output_values"])
			input_operation_type = dic["input_operation_type"]
			print ( "input_operation_type",input_operation_type )
			myname=dic["myname"]
			if input_operation_type in [ "N_AND","N_OR" ]:
				input_operation_type=","+input_operation_type
			else:
				input_operation_type=""
			print( myname,iv,ov )
			status=dic["status"]
			if status=="created":
				fgcolor="gray"
			elif status=="waiting":
				fgcolor="blue"
			elif status=="running":
				fgcolor="red"
			elif status=="finished":
				fgcolor="green"
			g.node( myname,label="{"+"|".join([iv,myname+input_operation_type,ov])+"}", fontcolor=fgcolor,color=fgcolor )

		self.graphviz_link(g)
		#with open("graph.dot","w") as f:
		#	f.write(g.source)	
		if not self._graphviz_dryrun:
			g.view()

	def graphviz_link(self,g):
		jobnetwork = LinkDB()
		dataflow=dataFlowDB()
		for link in jobnetwork.find(""):
			parent=link["parent_node"]
			child=link["child_node"]
			p_key=link["parent_key"]
			c_key=link["child_key"]
			link_id=link["link_id"]
			print ("data_id=",{dataflow._mainkey:link_id})
			data_diclist=dataflow.find(link_id)
			print( "data_diclist",data_diclist )
			fgcolor="gray"
			if len(data_diclist)>1:
				print( "internal error, data_diclist !=1, for link_id=",link_id )
				print( data_diclist )
				sys.exit(60000)
			elif len(data_diclist)==1:
				fgcolor="red"
			
			g.edge( ":".join([parent,p_key]),":".join([child,c_key]), color=fgcolor )

		

class DBList:
	"""a list of DB"""
	def __init__(self):
        	self._db0=JobnodeFinishedDB()
		self._db1=JobnodeDB()
		self._db2=LinkDB()
		self._db3=dataFlowDB()
	def drop_all(self):
		self._db0.drop()
		self._db1.drop()
		self._db2.drop()
		self._db3.drop()

def test1(run=1):

	dblist=DBList()
	dblist.drop_all()

	if True:
		funcA= "python numeric/funcA.py"
		funcB= "python numeric/funcB.py"
		funcC= "python numeric/funcC.py"
		funcmerge= "python numeric/funcmerge.py"
		funcOR= "python numeric/funcOR.py"
	else:
		funcA= "python string/funcA.py"
		funcB= "python string/funcB.py"
		funcC= "python string/funcC.py"
		funcmerge= "python string/funcmerge.py"
		funcOR= "python string/funcOR.py"

        graph=JobNetwork()
        nodelist=JobnodeList()

        node1= JobNode("node1", [],funcStyle(funcA)._dic,["x1","y1"] )
        nodelist.append(node1)



	if run==1:
        	node3= JobNode("node3", ["a3"],funcStyle(funcB)._dic,["x3","y3"])
        	node4= JobNode("node4", ["a4","b4"],funcStyle(funcB)._dic,["x4"])
        	node5= JobNode("node5", ["a5","b5"],funcStyle(funcB)._dic,["x5"])

        	graph.define(["node1","y1"],["node3","a3"])
        	graph.define(["node3","x3"],["node4","a4"])
        	graph.define(["node3","y3"],["node4","b4"])
        	graph.define(["node1","y1"],["node5","b5"])
        	graph.define(["node4","x4"],["node5","a5"])

        	nodelist.append(node3)
         	nodelist.append(node4)
        	nodelist.append(node5)
		nodelist.show()

	if run==2:
            node2= JobNode("node2", ["a2"],funcStyle(funcB)._dic,["x2"] )
            graph.define(["node1","x1"],["node2","a2"])
            nodelist.append(node2)
	    loopmerge=JobNode("loopmerge",["m1"],funcStyle(funcmerge)._dic,["x1"],input_operation_type="N_AND")
	    for i in range(6):
		name="loop"+str(i)
		nodeloop=JobNode(name , ["i1"],funcStyle(funcB)._dic,["o1"])
		graph.define(["node2","x2"],[name,"i1"])
		graph.define([name,"o1"],["loopmerge","m1"])
		nodelist.append(nodeloop)
	    nodelist.append(loopmerge)

	if run==3:
                node2= JobNode("node2", ["a2"],funcStyle(funcB)._dic,["x2"] )
                graph.define(["node1","x1"],["node2","a2"])
                nodelist.append(node2)
		node3=JobNode("node3",["a3"],funcStyle(funcOR)._dic,["x3"],input_operation_type="N_OR",creation_type="dynamic")
		node4=JobNode("node4",["a4"],funcStyle(funcC)._dic,["x4","y4"],creation_type="dynamic")
		graph.define(["node2","x2"],["node3","a3"],creation="dynamic")
		graph.define(["node3","x3"],["node4","a4"],creation="dynamic")
		graph.define(["node4","x4"],["node3","a3"],creation="dynamic")

		node5=JobNode("node5",["a5"],funcStyle(funcB)._dic,[])
		graph.define(["node4","y4"],["node5","a5"])

		nodelist.append(node3)
		nodelist.append(node4)
		nodelist.append(node5)



	nodelist.graphviz()

	print( )
	print( "start" )
	print( )

        for i in range(11):
                print( "<-------------------------node status",i )
                r=nodelist.start()
		if not r:
			print ()
			print ("nothing left" )
			print ()
			break

		nodelist.graphviz()
		t=2.0
		print( "wait",t,"sec" )
		time.sleep(t)

	print( "------------------end-------------------------")
	nodelist.show()


test1(run=2)

