Instructions
=============

- Load the workspace folder in eclipse as workspace
- Run from eclipse should run the program on single node, the run configurations specify the arguments required. 
- To create JAR file in Eclipse
	File > Export > JAR
- For running on cluster
	hadoop jar hadoop-tfidf.jar TFIDFDriver data/ 3 20 stopwords.txt

	data/ and stopwords.txt must be uploaded to hdfs
- sort-tfidf/ 	-  final index of phrases
  filter-tfidf/ -  tfidf of all relevant phrases
  compute-tfidf -  tfidf of all the phrases
  word-count 	-  word count of all phrases with stop words removed 
