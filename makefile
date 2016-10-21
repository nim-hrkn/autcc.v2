

all:  autc2
autc1:
	python autc1.py
	dot -Tpng graph.dot > graph.png

autc2:
	python autc2.py
	dot -Tpng graph.dot > graph.png
