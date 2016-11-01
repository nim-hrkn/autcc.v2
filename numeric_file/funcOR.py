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

    key=outputkey[0]
    key2=outputkey[1]

    n=8
    print "key,key2=",key,key2
    print
    print "outputvalue",outputvalue, outputvalue[key], outputvalue[key2]
    print "flag=",outputvalue[key] < n
    print

    if outputvalue[key] < n:
	print "put ", key
	put_value(key,outputvalue[key])
    else:
	print "put ", key2
	put_value(key2,outputvalue[key2])

    print outputvalue


doit(["a4"],["x4","y4"])

