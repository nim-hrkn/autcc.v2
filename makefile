

all:
	python autc1.py
	dot -Tpng graph.dot > graph.png
