Instructions
=============

- Place all the documents in Books/
- To compile
	mpic++ -fopenmp tfidf.cpp -std=gnu++11 -o tfidf
- To run on single node
	mpirun -n 2 ./tfidf ./Books
- To run on cluster in 6019
	mpirun -np 6 -host node11,node12,node13,node14,node15,node16 ./tfidf ./Books
- Index.txt has the keyword file index.
- TFIDF folder has the TFIDF for all the relevant phrases.

