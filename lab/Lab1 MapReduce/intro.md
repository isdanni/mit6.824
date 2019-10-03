# 6.824 Lab 1: MapReduce

In this lab you'll build a MapReduce library as an introduction to programming in Go and to building fault tolerant distributed systems. In the first part you will write a simple MapReduce program. In the second part you will write a Master that hands out tasks to MapReduce workers, and handles failures of workers. The interface to the library and the approach to fault tolerance is similar to the one described in the original [MapReduce paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

