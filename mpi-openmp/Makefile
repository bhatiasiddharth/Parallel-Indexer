all: tfidf.cpp
	mpic++ -fopenmp tfidf.cpp -std=gnu++11 -o tfidf
	mpirun -n 2 ./tfidf ./Books

clean: 
	rm -rf tfidf *.o TFIDF/* Relevant/* Index.txt