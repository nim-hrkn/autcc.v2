#!/usr/bin/env python

import json
import copy


def get_value(file):
    v=None
    try:
        with open(file,"r") as f:
		lines=f.readlines()
		v=int(lines[0])
    except:
	pass 
    return v

def put_value(file,values):
	with open(file,"w") as f:
		f.write(str(values))


def doit( inputkey, outputkey ):

    thisfunc="funcB"

    inputvalue={}
    for x in  inputkey:
            inputvalue[x] = get_value(x)

    print inputvalue

    outputvalue= {}
    for x in outputkey:
            outputvalue[x] = get_value(x)

    i=0
    iv=[]
    for ip in  inputvalue:
            j=inputvalue[ip]
            if isinstance(j,int):
                    iv.append(j)

    i=sum(iv)

    for key in outputvalue:
            k=key[0:1]
            iadd=0
            if k=="x":
                    iadd=1
            elif k=="y":
                    iadd=10
            elif k=="z":
                    iadd=100
            outputvalue[key]=i+iadd

    for key in outputvalue:
            put_value(key,outputvalue[key])

    print outputvalue


doit(["a4","b4"],["x4"])
