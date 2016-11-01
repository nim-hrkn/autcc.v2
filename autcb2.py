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

#sys.path.append('/home/kino/work/workflow/myxsub')

#from myxsub.remoteExec2 import qsubdic_NIMS_qsub2_kino
import myxsub.remoteExec3

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


class varType:
	def __init__(self,t,value=None):
		self._t=t
		if t in [list,str,int,float]:
		    if value is not None:
			if isinstance(t,value):
				self._value= value
		else:
			print ("error, port_type, unknown type=",t )
			sys.exit(040000)
	def type(self):
		return self._t 
	def set(self,value):
		if isinstance(self._t,value):
			self._value=value
	def get(self):
		return self._value

class NetworkCheckKeys:
	def __init__(self,dic):
		self._accept_key = {
 "creation":			["data life",			[str],		["dynamic","static"]],
 "treatment":			["action when more data come",	[str],		["replace","append"]] ,
 "parent_node":			["name of the parent node",	[str],		None],
 "parent_key":			["name of the output port of the parent node",[str],None],
 "child_node":			["name of the child node",	[str],		None],
 "child_key":			["name of the input port of the child node",[str],None],
 "link_id":			["a tag to specify the input/output ports",	[str],		None]
 }
                self.check_key_accepted(dic)

        def check_key_accepted(self, dic ):
          for key in dic.keys():
            if key in self._accept_key:
                vlist=self._accept_key[key][2]
                if vlist is not None:
                        if dic[key] in vlist:
                                pass
                        else:
                                print ("error, unknown value for key=" ,key,"value=", dic[key])
                                print ("accepted values=", vlist )
				print( "dic=", dic )
                                sys.exit(80010)
            else:
                print( "erorr, unknown key=",key )
                print( "accept_key=", accept_key )
		print( "dic=", dic )
                sys.exit(80011)


class JobnodeCheckKeys: 
        def __init__(self,dic):
                self._accept_key ={
  "func_input_type":            ["input type",                  [str],          ["json","eachfile"] ],
  "func_input_filename":        ["input filename",              [list,str],     None ], 
  "func_output_type":           ["output type",                 [str],          [ "json","eachfile" ] ],
  "func_output_templatefilename":[  "a list of output filenames for template",[list,str],None ],
  "func_output_filename":       ["a list of output filenames",  [list,str],     None ],
  "func_cmd":                   ["a list of program name" ,     [list,str],     None ],
  "myname":                     ["node name" ,                  [str],          None ],
  "input_operation_type":       ["input operation type to trigger running",[str],["1","N_OR","N_AND" ] ],
  "creation_type":              ["node life",                   [str],          ["static","dynamic"] ],
  "localworkbasedir":           ["base path name of local work directory",      [str],None ],
  "input_ports":                ["unique id for input tag" ,    [list,str],          None ],
  "output_ports" :              ["unique id for output tag",    [list,str],          None ],
  "input_values" :              ["input values for each output ports",  [list,object],None ],
  "output_values" :             ["output values for each output ports", [list,object],None ],
  "input_value_types":		["types of input values",	[dict],		None ],
  "output_value_types":		["types of output values",	[dict],		None ],
  "exec_time":                  ["starting time, string format", [str],         None ],
  "exec_id" :                   ["unique id to identify the node internally",str,None ],
  "finished_time":              ["finished time, string format", [str],         None ],
  "status":                     ["status of the node",          [str],          ["created", "running", "finished", "waiting","queued" ]  ] ,
  "jobid" : 			["jobid of batch system", 	[str],		None ] ,
  "runtype":			["type of the program",		[str],		["foreground","batch"] ],
  "hostname":			["batch queue server",		[str],		None], 
  "port":			["port of the batch queue server",[str],	None], 
  "username": 			["user id of the batch queue server",[str],	None],
  "private_key_file":		["ssh private key file to access the batch queue server",[str],None],
  "remoteworkbasedir":          ["base path name of the remote(server) work directory",      [str],None ],
  "localrunfile":		["a batch script",		[str],		None],
  "queue_cond":			["queue condition to substitute",[list,str],	None],
  "batchcmd":			["batch command for qsub/qstat/qdel",[dict],	None], 
  "_id":			["mongo DB internally used",	[None],		None]
 }
                self.check_key_accepted(dic)

        def check_key_accepted(self, dic ):
          for key in dic.keys():
            if key in self._accept_key:
		if dic[key] is None:
			continue
                vlist=self._accept_key[key][2]
                if vlist is not None:
                        if dic[key] in vlist:
                                pass
                        else:
				print ("JobnodeCheckKeys")
                                print ("error, unknown value for key=" ,key, "value=", dic[key])
                                print ("accepted values=", vlist )
				print ("dic=",dic)
                                sys.exit(80000)
            else:
                print( "erorr, unknown key=",key )
                print( "accept_key=", self._accept_key )
		print( "dic=",dic )
                sys.exit(80001)



def funcStyle_json_template( func, inputsytle, outputstyle ):
                if len(inputstyle)==0:
                        inputstyle={"func_input_type":"json", "func_input_filename":["_input.json"]}

                if len(outputstyle)==0:
                        outputstyle={"func_output_type":"json","func_output_templatefilename":["_outputtemplate.json"],"func_output_filename":["_output.json"]}
		dic={}
                dic["func_cmd"]= func 
                dic.update( inputstyle )
                dic.update( outputstyle )
		dic.update( { "jobid": None } )
                checkkeys=JobnodeCheckKeys(dic)

		return dic 

def funcStyle_eachfile_template(func, inputstyle, outputstyle, simulator=None ):
                if len(inputstyle)==0:
                        inputstyle={"func_input_type":"eachfile"}

                if len(outputstyle)==0:
                        outputstyle={"func_output_type":"eachfile"}

		dic={}
		print( "funcStyle_eachfile_template: func=",func )
                dic["func_cmd" ] = func 
                dic.update( inputstyle )
                dic.update( outputstyle )

		if simulator is not None:
			sim=simulator
			sim.update(dic)
			dic=sim 
		dic.update( { "jobid": None } )
                return dic

class funcStyle:
	def __init__(self,func="",inputstyle="",outputstyle="", iotype="eachfile", simulator=None, id_ =None):

		if iotype=="json":
			self._dic = funcStyle_json_template ( func, inputstyle,  outputstyle )
		elif iotype=="eachfile":
			self._dic = funcStyle_eachfile_template ( func,  inputstyle,  outputstyle )

		if simulator is not None:
			simulator._dic.update(self._dic)
			self._dic=simulator._dic  

		self._dic.update( {"jobid":None} )

		self._dryrun=False

	def from_dic(self,dic):
		self._dic=dic

	def make_input_output_json(self, inputvalues,outputvalues):

                inputfilename=self._dic["func_input_filename"][0]

                with open(inputfilename,"w") as f:
                        json.dump(inputvalues,f)

                outputfilename=self._dic["func_output_templatefilename"][0]

                with open(outputfilename,"w") as f:
                        json.dump(outputvalues,f)

	def make_input_output(self, inputvalues,outputvalues):
                t = self._dic["func_input_type"]
                if t =="json":
                        self.make_input_output_json( inputvalues,outputvalues)
                elif t =="eachfile":
                        self.make_input_output_eachfile( inputvalues,outputvalues) 
                else:
                        print( "make_input_output, unsupported sytle", t )
                        sys.exit(10000)


        def make_input_output_json(self, inputvalues,outputvalues):
                inputfilename=self._dic["func_input_filename"][0]
                with open(inputfilename,"w") as f:
                        json.dump(inputvalues,f)

                outputfilename=self._dic["func_output_templatefilename"][0]
                with open(outputfilename,"w") as f:
                        json.dump(outputvalues,f)

	def make_input_output_eachfile(self,inputvalues,outputvalues):
		for key in inputvalues:
			with open( key, "w" ) as f:
				for x in inputvalues[key]:
				    if x is not None:
					f.write(x+"\n")
				print( "file", key," is made at",os.getcwd() )
                for key in outputvalues:
                        with open( key, "w" ) as f:
				v= outputvalues[key]
				if v is None:
					s=""
				else:
					s=map(v,string)
                                f.write( s )
				print( "file", key," is made at", os.getcwd() )

	def make_output(self, outputvalues):
                t= self._dic["func_output_type"]
                if t =="json":
                        found, outputvalues = self.make_output_json( outputvalues )
                elif t =="eachfile":
                        found, outputvalues = self.make_output_eachfile(outputvalues )
                else:
                        print( "make_output, unsupported sytle", t )
                        sys.exit(100001)

                return found, outputvalues

	def make_output_json(self,outputvalues):

                outputfilename=self._dic["func_output_filename"][0]
                outputvalues=[]

                found=0
                with open(outputfilename,"r") as f:
                        outputvalues=json.load(f)
                        found=1

                return found, outputvalues 

	def make_output_eachfile(self, outputvalues ):
		foundlist=[]
                for key in outputvalues:
			print( "open key=", key )
                        with open( key, "r" ) as f:
                                lines=f.readlines()
				print( "make_output_eachfile", lines )
				if len(lines)>0:
					outputvalues[key] = lines[0]
					foundlist.append(1)
				else:
					outputvalues[key] = None
		
		if sum(foundlist)==len(outputvalues):
			found=1
		else:
			found=0
		return found, outputvalues

	def run(self,inputvalues,outputvalues,workdir):

		print ()
		print ( "dic= ", self._dic )
		sim=self._dic

		if sim["runtype"]=="batch":
			return self.run_remote( inputvalues,outputvalues,workdir )
		elif sim["runtype"]=="foreground":
			return self.run_local( inputvalues,outputvalues,workdir )
		else:
			print ( "unknown runtpe",sim["runtype"] )
			print ( "sim=", sim )
			sys.exit(4000)

	def run_remote(self, inputvalues,outputvalues,workdir ):

		sim= self._dic
                cwd=os.getcwd()
                os.mkdir(workdir)
                os.chdir(workdir)

		self.make_input_output( inputvalues,outputvalues)  

		print( "simulator ", sim )
		job = myxsub.remoteExec3.remoteExec(sim)
		ret,status =  job.send_and_run()
		self._dic["jobid"]= job._jobid
		print( "job._jobid", job._jobid )
		print( "job._dic=", job._dic )
		print( "ret,status=",ret , status )
		print( "self._dic=",self._dic )

		os.chdir(cwd)

		return [ ret, outputvalues, status, job._jobid[0] ]

	def qstat(self):
		sim= self._dic
		jobid=self._dic["jobid"]
		jobid=jobid[0]
		job = myxsub.remoteExec3.remoteExec(sim)
		r=job.qstat(jobid)
		self._dic["status"] = job._dic["status"]
		return r,job._dic["status"]

	def pack_and_get_dir(self):
                sim= self._dic
                jobid=self._dic["jobid"]
                job = myxsub.remoteExec3.remoteExec(sim)
                r=job.pack_and_get_dir()
                self._dic["status"] = job._dic["status"]
                return r,job._dic["status"]

	def run_local(self,inputvalues,outputvalues,workdir):

		cwd=os.getcwd()
		os.mkdir(workdir)
		os.chdir(workdir)

		print( "... >  workbasedir=",workdir  )

		self.make_input_output( inputvalues,outputvalues)  

		cmds=self._dic["func_cmd"]

		ret=-1
		if not self._dryrun:
			for cmd in cmds:
				ret=subprocess.call(cmd,shell=True)
				print( "subprocess, ret=",ret, "cmd=",cmd )
			if ret!=0:
				print( "error in subprocess call" )
				sys.exit(30000)

		found, outputvalues = self.make_output(outputvalues)

		if ret ==0 and found==0 : 
			ret = 1

		os.chdir(cwd)

		jobid=None

		return [ret,outputvalues,"finished",jobid]

	def show(self):
		print( "<funcStyle.show()" )
		print( self._dic )
		print( self._dryrun )
		print( "funcStyle.show()>" )
	

class DBbase(object):
	""" wrapper for mongoDB"""
	def __init__(self,my_database,my_collection,mainkey, mongo_server="localhost", mongo_port=27017):
		self._my_database=my_database
		self._my_collection=my_collection
		self._client = pymongo.MongoClient(mongo_server, mongo_port )
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
                self._dic={ "exec_id":None, "myname":None, 
                "input_operation_type":None ,"creation_type":None, "localworkbasedir":None}
		self._dic.update(self.keys_initialvalues(input_keys,output_keys))
		keycheck= JobnodeCheckKeys(self._dic)
		self._mainkey="myname"

	def keys_initialvalues(self,input_keys,output_keys):
		input_port={}
		for x,t in input_keys:
			input_port[x]= []
		output_port= {}
		for x,t in output_keys:
			output_port[x]=[]
		input_values={}
		input_value_types={}
		for x,t in input_keys:
			input_values[x]= None
			input_value_types[x]= t
		output_values={}
		output_value_types={}
		for x,t in output_keys:
			output_values[x]=None
			output_value_types[x]= t
		dic ={"input_ports":input_port, "output_ports":output_port, 
		"input_values":input_values, "output_values":output_values,
		"input_value_types": input_value_types, "output_value_types": output_value_types, 
		"exec_time":None , "exec_id":None,"finished_time":None,"status":"created"}
		keycheck= JobnodeCheckKeys(self._dic)
		return dic

class JobNode:
	""" job node difinition"""
	def __init__(self,myname,input_keys,func,output_keys,workbasedir, node_id="",input_operation_type="1",creation_type="static" ):  #,data_life="replace"):
		""" input_operation_type = 1
					N_AND
					N_OR"""
	
		self._jobnode_db =JobnodeDB()
		self._jobnode_finished_db =JobnodeFinishedDB()

		template=JobNodeTemplate(input_keys,output_keys)
		self._dic=template._dic
		self._mainkey=template._mainkey

		if len(node_id)==0:
			node_id=hash_generator.get(myname)

		func._dic.update( self._dic )
		dic=func._dic 
		dic.update({ "exec_id":node_id, "myname":myname,
		"input_operation_type":input_operation_type ,"creation_type":creation_type,
		"localworkbasedir": workbasedir })
		
		checkkeys=JobnodeCheckKeys(dic)

		self._dic.update(dic)
		print( "insert DB" ,self._dic)
		self._dic=self._jobnode_db.insert_and_find_one(self._dic)

	def reset_dic(self):
                input_values={}
		input_keys = self._dic["input_values"]
                for x in input_keys:
                	input_values[x]= None
                output_values={}
		output_keys = self._dic["output_values"]
                for x in output_keys:
                        output_values[x]=None
		dic ={ "input_values":input_values, "output_values": output_values,
                "exec_time":None , "exec_id":None,"finished_time":None,"status":"waiting"}
		checkkeys= JobnodeCheckKeys( dic )
		self._dic.update(dic)
		return self._dic 


	def show(self,mode="simple"):
		self.get_data()
		if mode=="simple":
			print( self._dic["myname"],self._dic["input_values"],self._dic["output_values"] )
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
			print( " in accept key?", key in self._accept_dic )
			print( "dic=", self._dic )
			sys.exit(30000)


	def start(self):
		"""check the inputport and start if the condition is fulfilled"""
		self.get_data()
		initialstate=self._dic["status"]
		finalstate=initialstate

		iop=self._dic["input_operation_type"]

		print( "initial state=",initialstate )
		if initialstate in ["queued", "running"]:
			func=self._dic["func"]
			jobid=func["jobid"]
                        funcstyle=funcStyle()
                        funcstyle.from_dic(func)
			print( "start, dic=",self._dic )
			# qstat
			r,status=funcstyle.qstat()
			self._dic["status"]=status
			print( "after funcstyle.qstat",r,status)
			if self._dic["status"]=="finished":
				funcstyle.pack_and_get_dir()
				print ( "get data ")
				sys.exit(10000)

		if initialstate in ["created","waiting"] :
			#check_all_the_port

			iport=InputPortOperation(self._dic["input_ports"],self._dic["input_values"],iop=iop)
			found,values=iport.get() 
			if found:  # now all the data are in the input ports
				# change the status
				print() 
				print( "start ",self._dic[self._mainkey] )
				print() 
				self._dic["status"] = "running"
				self._dic["input_values"] =values
				today =datetime.datetime.today().__str__()
				self._dic["exec_id"] = hash_generator.get(self._dic[self._mainkey]+today)
				self._dic["exec_time"]=today
				checkkeys= JobnodeCheckKeys(self._dic) 
				self.update_data()

				inputvalues=self._dic["input_values"]
				outputvalues=self._dic["output_values"]

				print( "set workdir, self._dic=", self._dic ) 
				workdir=os.path.join(self._dic["localworkbasedir"],self._dic["exec_id"])
				if "localworkbasedir" in self._dic:
					workdir=os.path.join(self._dic["localworkbasedir"] ,self._dic["exec_id"])

				print( "inputvalues=",inputvalues )

				func=self._dic

				funcstyle=funcStyle()
				funcstyle.from_dic(func)

				ret,outputvalues,status,jobid=funcstyle.run(inputvalues,outputvalues,workdir)

				print( "after run, ret,outputvalues,status,jobid=",ret,outputvalues,status,jobid )

				self._dic[ "output_values"] = outputvalues 
				self._dic[ "status" ] =  status 
				self._dic[ "finished_time" ] = today 

				if self._dic["runtype"]=="batch": 
					print( "not simulator in dic" )
					print( "self._dic=",self._dic )
					self._dic["jobid"]=jobid
					self._dic["status"]=status

				checkkeys=JobnodeCheckKeys(self._dic)

				if status=="finished": 
					self.save_finished_data()
				finalstate = self._dic["status"]

				oport=OutputPortOperation(self._dic["output_values"],self._dic["output_ports"])
				oport.put()

				if self._dic["creation_type"]=="static":
					pass
				elif self._dic["creation_type"]=="dynamic":
					self.reset_dic()

				print(" new status", self._dic["myname"], self._dic["status"] )
				self.update_data()

				iport.reset_data()
				

		return [initialstate,finalstate]


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
		checkkey= NetworkCheckKeys( self._dic )
		
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
		print ("parent", parent )
                parent_name=parent[0]
		parent_dic= nodedb.find_one({"myname":parent_name})
                parent_key=parent[1]

		print ( "parent_dic=", parent_dic )
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
		n= len(self._list)
		listdone=[]
                for i,node in enumerate(self._list):
                        ret= node.start()
			listdone.append(ret)
			print( "JObnodeList,node,ret=",node._dic["myname"],ret )
			if (ret[0]=="created"  and ret[1]=="finished") or (ret[0]=="waiting" and ret[1]=="finished" ):
				return False
			if (ret[0]=="finished"  and ret[1]=="finished") :
				if i==n-1:
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
			else:
				print( "unknown status=",status )
				sys.exit(1000)
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

	workbasedir = "/home/kino/kino/work/workflow/work"

	simulator_asahi = myxsub.remoteExec3.qsubdic_NIMS_qsub2_kino()
	simulator_localhost = myxsub.remoteExec3.qsubdic_localhost()

	iostyle = "numeric_file"

	prefix="/home/kino/kino/work/workflow"
	if iostyle == "numeric_json":
		funcA= ["python "+os.path.join(prefix, "numeric/funcA.py") ]
		funcB= ["python "+os.path.join(prefix, "numeric/funcB.py") ]
		funcC= ["python "+os.path.join(prefix, "numeric/funcC.py") ]
		funcmerge= ["python "+os.path.join(prefix, "numeric/funcmerge.py") ]
		funcOR= ["python "+os.path.join(prefix,"numeric/funcOR.py") ]
	elif iostyle == "numeric_file":
		funcA= ["python "+os.path.join(prefix,"numeric_file/funcA.py")]
		funcB= ["python "+os.path.join(prefix,"numeric_file/funcB.py")]
		funcC= ["python "+os.path.join(prefix,"numeric_file/funcC.py")]
		funcmerge= ["python "+os.path.join(prefix,"numeric_file/funcmerge.py") ]
		func13 = ["python "+os.path.join(prefix,"numeric_file/func13.py")]
		func14 = ["python "+os.path.join(prefix,"numeric_file/func14.py")]
		func15 = ["python "+os.path.join(prefix,"numeric_file/func15.py")]
		func33 = ["python "+os.path.join(prefix,"numeric_file/func33.py")]
		funcOR = ["python "+os.path.join(prefix,"numeric_file/funcOR.py")]
		func35 = ["python "+os.path.join(prefix,"numeric_file/func35.py")]
	elif iostyle == "string_json":
		funcA= ["python /home/kino/work/workflow/string/funcA.py" ]
		funcB= ["python /home/kino/work/workflow/string/funcB.py" ]
		funcC= ["python /home/kino/work/workflow/string/funcC.py" ]
		funcmerge= [ "python /home/kino/work/workflow/string/funcmerge.py" ]
		funcOR= ["python /home/kino/work/workflow/string/funcOR.py" ]

        graph=JobNetwork()
        nodelist=JobnodeList()

	typeint="int"

        node1= JobNode("node1", [],funcStyle(funcA, simulator=simulator_localhost),[["x1",typeint],["y1",typeint]], workbasedir )
        nodelist.append(node1)


	if run==1:
        	node3= JobNode("node3", [["a3",typeint]],funcStyle(func13, simulator=simulator_localhost),[["x3",typeint],["y3",typeint]], workbasedir)
        	node4= JobNode("node4", [["a4",typeint],["b4",typeint]],funcStyle(func14, simulator=simulator_localhost),[["x4",typeint]], workbasedir)
        	node5= JobNode("node5", [["a5",typeint],["b5",typeint]],funcStyle(func15, simulator=simulator_localhost),[["x5",typeint]], workbasedir)

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
            node2= JobNode("node2", [["a2",typeint]],funcStyle(funcB, simulator=simulator_localhost),[["x2",typeint]] , workbasedir)
            graph.define(["node1","x1"],["node2","a2"])
            nodelist.append(node2)
	    loopmerge=JobNode("loopmerge",[["m1",typeint]],funcStyle(funcmerge, simulator=simulator_localhost),[["x1",typeint]],workbasedir, input_operation_type="N_AND")
	    for i in range(3):
		name="loop"+str(i)
		nodeloop=JobNode(name , [["i1",typeint]],funcStyle(funcC,simulator=simulator_localhost),[["o1",typeint]], workbasedir)
		graph.define(["node2","x2"],[name,"i1"])
		graph.define([name,"o1"],["loopmerge","m1"])
		nodelist.append(nodeloop)
	    nodelist.append(loopmerge)

	if run==3:
                node2= JobNode("node2", [["a2",typeint]],funcStyle(funcB, simulator=simulator_localhost),[["x2",typeint]], workbasedir )
                graph.define(["node1","x1"],["node2","a2"])
                nodelist.append(node2)
		node3=JobNode("node3",[["a3",typeint]],funcStyle(func33, simulator=simulator_localhost),[["x3",typeint]], workbasedir, input_operation_type="N_OR",creation_type="dynamic")
		node4=JobNode("node4",[["a4",typeint]],funcStyle(funcOR, simulator=simulator_localhost),[["x4",typeint],["y4",typeint]],workbasedir , creation_type="dynamic")
		graph.define(["node2","x2"],["node3","a3"],creation="dynamic")
		graph.define(["node3","x3"],["node4","a4"],creation="dynamic")
		graph.define(["node4","x4"],["node3","a3"],creation="dynamic")

		node5=JobNode("node5",[["a5",typeint]],funcStyle(func35, simulator=simulator_localhost),[], workbasedir)
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
		print( "returnofnodelist.start=",r )
		if r:
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


test1(run=3)

