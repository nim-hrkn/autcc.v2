#!/usr/bin/env python

import json
import copy



def get_value(file):
        with open(file,"r") as f:
		try:
			values=json.load(f)
		except:
			values={}
	return values

def put_outputvalue(values):
        file="_output.json"
	with open(file,"w") as f:
		json.dump(values,f)

inputfile="_input.json"
outputtemplatefile="_outputtemplate.json"
outputfile="_output.json"

inputvalue=  get_value(inputfile)
outputvalue= get_value(outputtemplatefile)
print "outputtemplate=",outputvalue

v=[]
for ip in  inputvalue:
	v.append(ip+"="+"+".join(inputvalue[ip]))
str1="-".join(v)
print "funcmerge,str1=",str1
print "outputvalue",outputvalue
for key in outputvalue:
	outputvalue[key]=",".join([copy.deepcopy(str1),key])

put_outputvalue(outputvalue)


