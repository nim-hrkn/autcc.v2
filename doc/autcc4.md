#specification

##database
- nodedb.jobnode
- nodedb.link
- nodedb.dataflow
- nodefinisheddb.jobnode

###nodedb.jobnode
- node\_id: unique id , obsolete 
- myname:string
- func:funcStyle
	- cmd: external program
	- inputstyle: 
		- type:json|...
		- filename: string
	- outputstyle: style\_format
		- type:json|...
		- filename: string
		- templatefilename: string
- input\_operation\_type:1| N\_AND | N\_OR
	- 1:The input port will get data from a link. (default)
	- N\_AND: The input port will get data from links.  func is executed when data come from all of the input ports.
	- N\_OR: The input port will get data from links.  func is executed when data come from one of the input ports


- creation\_type:dynamic| static
- input\_ports: a list of data tag in the input\_port. Each tags may be a list.
- output\_ports:a list of data tag in output\_port. Each tags may be a list.
- input\_values:a list of input\_values
- output\_values:a list of output\_values
- exec\_time: time to start the function, assigned when this node starts
- exec\_id: unique id, assigned when this node starts
- finished\_time: time when the program finishes
- status:created|running|finished

###nodedb.link
- parent\_node: string, the same as jobnode.myname
- parent\_key: string,  a name of input port of jobnode.myname
- child\_node: string, the same as jobnode.myname
- child\_key: string,  a name of input port of jobnode.myname
- link\_id:  unique id of the link , used to define input/output tag.
- creation:dynamic| static, will change the behavior of dataflow when the data are passed to the input port of a job node. 
	-  "dynamic" will delete the dataflow after the data are passed to a node.
	-  "static" will hold the dataflow once the data are created. (default)

- treatment:replace| append, will change the behavior of dataflow when new data come.
	- "replace" will replace old data with new one when it works.
	- "append" wlll append  newdata to a lsit of old data before it is get from the input port. 


###nodedb.dataflow
- data\_id : a tag  to send/recieve data
- value  :  value to send/recieve data

###nodefinisheddb.jobnode
save jobnode when status=finished. 

data structure is the same as nodedb.jobnode.


![sample01](https://raw.githubusercontent.com/nim-hrkn/autcc.v2/dynamic/image/run01.gif/movie.gif "sample01")
![sample02](https://raw.githubusercontent.com/nim-hrkn/autcc.v2/dynamic/image/run02.gif/movie.gif "sample02")
![sample03](https://raw.githubusercontent.com/nim-hrkn/autcc.v2/dynamic/image/run03.gif/movie.gif "sample03")
