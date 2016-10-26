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

i=0
iv=[]
for ip in  inputvalue:
	for l in inputvalue[ip]:
		print "inputvalue=",inputvalue[ip]
		if isinstance(l,int):
			iv.append(l)

i=sum(iv)

for key in outputvalue:
	k=key[0:1]
	outputvalue[key]=i

put_outputvalue(outputvalue)


