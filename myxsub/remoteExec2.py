#!/usr/bin/env python
import paramiko 
import os
import copy
import sys
import subprocess
import json


class runscriptTemplate:
	__cmd="""#!/bin/bash -x
#QSUB2 queue %%QUEUE%%
#QSUB2 core %%CORE%%
#QSUB2 mpi %%MPI%%
#QSUB2 smp %%SMP%%
#QSUB2 wtime %%WTIME%%
cd $PBS_O_WORKDIR
. /etc/profile.d/modules.sh
LANG=C
module list > _module.txt 2>&1
/home/kino/work/helloworld/hello
echo '{"status":"finished"}' > _status.json
"""
	def __init__(self):
		pass
	def replace(self,dic):
		y=copy.deepcopy(self.__cmd)
		for x in dic:
			y=y.replace(x,dic[x])
		return y

class remoteExec:
	#def __init__(self, hostname,port,username,private_key_file,remoteworkbasedir,localworkbasedir,localrunfile,id_,bachcmd, queue_cond ):
	def __init__(self, dic ):
		self._cwd= os.getcwd()
                self._hostname = dic["hostname"]
                self._port  =  dic["port"]
                self._username = dic["username"]
                self._private_key_file = dic["private_key_file"]
                self._remoteworkbasedir = dic["remoteworkbasedir"]
		self._id = dic["id_"]
                self._remoteworkdir = os.path.join(self._remoteworkbasedir,self._id)
		self._batchcmd = dic["batchcmd"]
		self._localrunfile= dic["localrunfile"]
		self._localworkbasedir = dic["localworkbasedir"]
                self._queue_cond= dic["queue_cond"]  # {"%%QUEUE%%":"qS", "%%CORE%%":"1", "%%MPI%%":"1", "%%SMP%%":"1", "%%WTIME%%":"0:02:00"}

		self._delete_remotefiles=False

		self.connect()

	def connect(self):
                self._ssh = paramiko.SSHClient()
                self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self._ssh.connect(self._hostname, self._port,self._username, key_filename=self._private_key_file)

	def change_workdir(self):
		print "self._localworkbasedir=",self._localworkbasedir
		os.chdir( self._localworkbasedir )
	def restore_workdir(self):
		os.chdir(self._cwd)
        def send_a_file(self,localrunfile):
		cwd=os.getcwd()
		self.change_workdir()
                sftp = self._ssh.open_sftp()
                try:
			print "sftp.put",os.getcwd(), localrunfile
                        ret =sftp.put(localrunfile,os.path.join(self._remoteworkdir,localrunfile))
                except:
			os.chdir(cwd)
                        print "error"
                        sys.exit(10000)
                sftp.close()
		os.chdir(cwd)
		return ret

	def get_a_file(self,localrunfile):
		cwd=os.getcwd()
		self.change_workdir()
                sftp = self._ssh.open_sftp()
                try:
			print "remoteworkdir=",self._remoteworkdir
			print "remotefile",os.path.join(self._remoteworkdir,localrunfile)
			print "localfile",localrunfile
                        ret =sftp.get(os.path.join(self._remoteworkdir,localrunfile),localrunfile)
                except:
			os.chdir(cwd)
                        print "error"
                        sys.exit(10000)
                sftp.close()
		os.chdir(cwd)
		return ret


        def run_raw( self,cmd ):
		cwd=os.getcwd()
		self.change_workdir()
                self._stdin, self._stdout, self._stderr = self._ssh.exec_command( cmd )
                self._status=self._stdout.channel.recv_exit_status()
		os.chdir(cwd)
                return self._status


        def run( self,localrunfile ):
		cwd=os.getcwd()
		self.change_workdir()
                cmd= [  'cd '+self._remoteworkdir+
			' && '+
			self._qsub +" "+localrunfile ]
		print "run cmd="," ".join(cmd)
                self._stdin, self._stdout, self._stderr = self._ssh.exec_command( " ".join(cmd) )
                self._status=self._stdout.channel.recv_exit_status()
		os.chdir(cwd)
                return self._status

        def qsub( self, localrunfile ):
		cwd=os.getcwd()
		self.change_workdir()

                self._qsub= self._batchcmd["qsub"]
                self._jobid=[]
                r=self.run(localrunfile)
		self._dic={}
                for n in self._stdout:
			jobid=n.rstrip("\n") 
                        self._jobid.append( jobid )
			self._dic["job_id"]= jobid 
		with open("_job_id.json","w") as f:
			json.dump(self._dic,f)
		os.chdir(cwd)
		return r

	def qstat_status_string(self,q):
		if q in ["Q"]:
			str="queued"
		elif q in ["R","T","E"]:
			str="running"
		elif q in ["C"]:
			str="finished"
		else:
			print "unknown qstat status", q
			sys.exit(040000)
		return str


        def qstat( self, jobid ):
		cwd=os.getcwd()
		self.change_workdir()
                self._qsub= self._batchcmd["qstat"]
                r =self.run(jobid)
		self._dic={}
		for i,n in enumerate(self._stdout):
			if i==2:
				s=n.split()
				jobstatus=self.qstat_status_string(s[4])
				self._dic={"status": jobstatus}
				os.chdir(cwd)
				return  r
		os.chdir(cwd)
		self._dic["status"]="no_result"
		return 0

        def qdel( self, jobid):
		cwd=os.getcwd()
		self.change_workdir()
                self._qsub= self._batchcmd[ "qdel" ]
                r= self.run(jobid)
		os.chdir(cwd)
		return r

        def show_output(self):
                print "==============================="
                for n in  self._stdout:
                        n=n.rstrip("\n")
                        print n
                print "----------------------------"
                stderr=[]
                for n in self._stderr:
                        n=n.rstrip("\n")
                        print n

        def close(self):
                self._ssh.close()

	def pack_and_get_dir(self):
		cwd=os.getcwd()
		print "pack_and_get_dir"
		self.change_workdir()
		print "cwd=",os.getcwd()
		backupdirname = self._remoteworkdir 

		self._remoteworkdir = self._remoteworkbasedir
		filename= self._id + ".tgz"
		self._qsub=""
		cmd=" ".join(["tar cvfz",filename,self._id])
		run_result= self.run( cmd )
		get_result= self.get_a_file( filename )
	
		#r=subprocess.call("pwd",shell=True)
		#r=subprocess.call("ls -l",shell=True)
		cmd=" ".join(["tar xvfz",filename])
		print "cmd=",cmd
		r=subprocess.call(cmd,shell=True)
		subprocess_result=r

		# delete remote files
		if self._delete_remotefiles:
                        self._qsub=""
                        cmd=" ".join(["rm -f", filename ])
                        run_result= self.run( cmd )
                        cmd=" ".join(["rm -rf", self._id ])
                        run_result= self.run( cmd )

		self._remoteworkdir = backupdirname
		os.chdir(cwd)


	def send_and_run(self):
                print self.run_raw("rm -rf "+self._remoteworkdir)
                print self.run_raw("mkdir "+self._remoteworkdir)

                os.chdir(self._localworkbasedir)
                template= runscriptTemplate()
                with open(self._localrunfile,"w") as f:
                        f.write(template.replace(self._queue_cond))
                self.restore_workdir()

                self.send_a_file( self._localrunfile )
                print self.qsub( self._localrunfile )
                print self._jobid
                print "--------------------------------------"
                for job in self._jobid:
                        print self.qstat( job )
                        print job, self._dic


	def qstat_by_json(self):
                self.change_workdir()
                with open("_job_id.json","r") as f:
                        jobid=json.load(f)
                print "jobid",jobid
                for job in [jobid["job_id"]]:
                        print self.qstat( job )
                        print job, self._dic
                self.restore_workdir()


	def qdel_by_json(self):
                self.change_workdir()
                with open("_job_id.json","r") as f:
                        jobid=json.load(f)
                print "jobid",jobid
                if True:
                    print "--------------------------------------"
                    for job in [jobid["job_id"]]:
                        print "delete ", job
                        print self.qdel( job )
                self.restore_workdir()


	def __del__(self):
		if self._ssh:
			self._ssh.close()
		self.restore_workdir()


if __name__ =="__main__" :

	if (len(sys.argv)!=3) :
		sys.exit(10)

	#hostname = 'asahi02'
        #port  = 22
        #username = 'kino'
        #private_key_file = '/home/kino/.ssh/id_rsa.2013'
	#remoteworkbasedir= "/home/kino/tmp"
	#id_= sys.argv[2]
	#batchcmd={ "qsub":"qsub2", "qstat":"qstat", "qdel":"qdel" }
	#localworkbasedir="/home/kino/work/workflow/myxsub/work"
	#localrunfile="run2.sh"
        #queue_cond={"%%QUEUE%%":"qS", "%%CORE%%":"1", "%%MPI%%":"1", "%%SMP%%":"1", "%%WTIME%%":"0:02:00"}
	#id_= sys.argv[2]

	qsubdic={}
	qsubdic[ "hostname" ] = 'asahi02'
        qsubdic[ "port" ]  = 22
        qsubdic[ "username" ] = 'kino'
        qsubdic[ "private_key_file" ] = '/home/kino/.ssh/id_rsa.2013'
	qsubdic[ "remoteworkbasedir" ] = "/home/kino/tmp"
	qsubdic[ "batchcmd" ] = { "qsub":"qsub2", "qstat":"qstat", "qdel":"qdel" }
	qsubdic[ "localworkbasedir" ] ="/home/kino/work/workflow/myxsub/work"
	qsubdic[ "localrunfile"] = "run2.sh"
        qsubdic[ "queue_cond" ] = {"%%QUEUE%%":"qS", "%%CORE%%":"1", "%%MPI%%":"1", "%%SMP%%":"1", "%%WTIME%%":"0:02:00"}
	qsubdic[ "id_" ] = sys.argv[2]


	#doit= remoteExec(hostname,port,username,private_key_file,remoteworkbasedir,localworkbasedir,localrunfile,id_,batchcmd,queue_cond)
	doit= remoteExec(qsubdic)

	run=int(sys.argv[1])
	print "run=",run

	if run==1:
		doit.send_and_run()

	elif run==2:
		doit.qstat_by_json()

	elif run==3:
		doit.qdel_by_json()

	elif run==4:
		doit.pack_and_get_dir()

	else:
		print "unknown run=",run


