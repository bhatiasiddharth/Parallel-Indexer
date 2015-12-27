#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <map>
#include <set>
#include <algorithm>
#include <string> 
#include <vector>  
#include <dirent.h>
#include <utility>
#include <ctype.h>
#include <mpi.h>
#include <omp.h>
using namespace MPI;
using std::map;
using std::string;
using std::vector;
string dir,output,output2;
#define ROOT 0
#define FMAX 3
#define WMAX 20
#define num_threads 4

typedef std::vector<string> list;
typedef std::map<std::string, list> Dictionary;

const std::string Symbols = "`,;.:-()\t!¡¿?\"[]{}&<>+-*/=#'";



//sorting maps
template<typename A, typename B>
std::pair<B,A> flip_pair(const std::pair<A,B> &p)
{
    return std::pair<B,A>(p.second, p.first);
}

template<typename A, typename B>
std::map<B,A> flip_map(const std::map<A,B> &src)
{
    std::map<B,A> dst;
    std::transform(src.begin(), src.end(), std::inserter(dst, dst.begin()), 
                   flip_pair<A,B>);
    return dst;
}



//serializing map
char *serialize_container(map <string, int>& hash_map, int *out_size)
{
    int str_sizes = 0;
    int buf_size = 0;

    // Iterate through map to count all string lengths
    typedef map<string, int>::iterator it_type;
    for(it_type iterator = hash_map.begin(); iterator != hash_map.end(); iterator++) {

        string word = iterator->first;
        str_sizes += word.length();
    }

    // Buffer size = all string lengths + NULLs + counts + header int
    buf_size = str_sizes + (hash_map.size() * sizeof(int)) + hash_map.size();
    buf_size += sizeof(int);
    char *buf = (char*)calloc(1, buf_size);

    // Add the elem size (header) to buf, advance ptr
    *((int*)buf) = hash_map.size();
    char *buf_ptr = buf+sizeof(int);

    // Add all strings and counts
    for(it_type iterator = hash_map.begin(); iterator != hash_map.end(); iterator++) {

        string word = iterator->first;
        int count = iterator->second;

        memcpy(buf_ptr, word.c_str(), word.length());
        buf_ptr += word.length();
        *buf_ptr = 0;
        buf_ptr++;
        *((int*)buf_ptr) = count;
        buf_ptr += sizeof(int);
    }

    // for (int i = 0; i < buf_size; i++) {
    //     printf("item %d) %x.\n", i, buf[i]);
    // }

    // Set out_ptr to size of buffer
    *out_size = buf_size;
    return buf;
}



//deserializing map
map<string, int>* deserialize_container(char *buf)
{
    // Get header size (num elements), advance ptr
    int elem_size = *((int*)buf);
    char *buf_ptr = buf+sizeof(int);

    map<string, int> *ret_map = new map<string, int>();
    for (int i = 0; i < elem_size; i++) {
        // Create string, advance ptr
        string str(buf_ptr);
        buf_ptr += str.length() + 1;

        // Get count, advance ptr
        int count = *((int*)buf_ptr);
        buf_ptr += sizeof(int);

        // Set entry in map
        (*ret_map)[str] = count;
    }

    return ret_map;
}


//split token on delimiters for stopword removal
vector<string> split(const string & str, const string & delimiters) {
    vector<string> v;
    string::size_type start = 0;
    auto pos = str.find_first_of(delimiters, start);
    while(pos != string::npos) {
        if(pos != start) // ignore empty tokens
            v.emplace_back(str, start, pos - start);
        start = pos + 1;
        pos = str.find_first_of(delimiters, start);
    }
    if(start < str.length()) // ignore trailing delimiter
        v.emplace_back(str, start, str.length() - start); // add what's left of the string
    return v;
}

struct ci_less
{
  bool operator() (const string & s1, const string & s2) const
  {
    return strcasecmp(s1.c_str(), s2.c_str()) < 0;
  }
};




//for term frequency
map<string,int,ci_less> termfreq(string file)
{
    using namespace std;
    map<string,int,ci_less> bookCount;

    string filename(dir + "/" + file );
    ifstream input(filename);
    
    std::string line;
    string word,phrase;
    int flag=0;
    int global_valid=0;
    while( std::getline( input, line ) )
    {
        // Substitute punctuation symbols with spaces
        std::string::iterator it;
       	#pragma omp parallel for
        for(it = line.begin(); it < line.end(); ++it)
        {
            if ( Symbols.find( *it ) != std::string::npos )
            {
                *it = ' ';
            }
        }

        // Let std::operator>> separate by spaces
        std::istringstream filter( line );
        while( filter >> word )
        {
            if(flag==0) //no phrase till now
            {
                if(isupper(word[0]))    //now phrase beginning
                {
                    phrase=phrase+word;
                    flag=1; //phrase continues
                }
                else if(!isupper(word[0])) 
                {
                    std::transform(word.begin(), word.end(), word.begin(), ::tolower);
                    ++bookCount[word];
                    global_valid=1;
                }
            }

            else if(flag==1)
            {
                if(isupper(word[0]))    //now phrase continues
                {
                    phrase=phrase+" "+word;
                }
                else if(!isupper(word[0])) //this condition checks that entire file not in this form
                {
                    //std::transform(phrase.begin(), phrase.end(), phrase.begin(), ::tolower);
                    ++bookCount[phrase];
                    //if(dict[phrase].size()<FMAX)    //FMAX condition

                    //bookCount[phrase].push_back(file);
                    std::transform(word.begin(), word.end(), word.begin(), ::tolower);
                    ++bookCount[word];
                    phrase.clear();
                    flag=0;
                    global_valid=1;
                }
            }
        }

        if(global_valid==1 && !phrase.empty())  //this condition is if file ends with a phrase 
            //if(dict[phrase].size()<FMAX)    //FMAX condition
                //bookCount[phrase].push_back(file);
            ++bookCount[phrase];

    }
    input.close();


    //removing stopwords from the dictionary
    input.open("StopWords.txt");
    std::vector<std::string> lines;
    while ( std::getline(input, line) )
    {
        // skip empty lines:
        
        if (line.empty())
            continue;

        vector<string> v = split(line, ";, ");
        for (auto& ent : v)
        {
            bookCount.erase(ent); //this works with case insensitive stop words
        }
    }
    input.close();


    /*
    //for printing term freq in output folder
    string filepath(output + "/" + file );
    ofstream outfile(filepath);
    for (auto& ent : bookCount)
    {
        outfile<< ent.first << "-" << ent.second << endl;
    }
    outfile.close();
    */

    return bookCount;
}



//to count the number of documents containing each word
map<string, int> countDoc(map<string, map<string, int,ci_less> >& directoryCount)
{
    using namespace std;
    map<string,int> documentCount;

    
    
    for (auto& ent : directoryCount)
    {
        for (auto& ent2 : ent.second)
            ++documentCount[ent2.first];
    }
    

    /*
    map<string, map<string, int,ci_less> >::iterator ent;
    
    for(ent = directoryCount.begin(); ent != directoryCount.end(); ++ent)
    {
        #pragma omp task
        for (auto& ent2 : ent->second)
            ++documentCount[ent2.first];
    }
    */
    



    /*
    string filepath(output + "/temptemp.txt");
    ofstream outfile(filepath);
    for (auto& ent : documentCount)
    {
        outfile<< ent.first << "-" << ent.second << endl;
    }
    outfile.close();   
    */

    return documentCount;
}


//global count of number of documents which contain the word
map<string, int> * reduceDocCount(map<string, map<string, int> >& node_map)
{
    map<string, int> *newDocCount = new map<string, int>();
    

    map<string, map<string, int> >::iterator ent;
    for(ent = node_map.begin(); ent != node_map.end(); ++ent)
    {
        for (auto& ent2 : ent->second)
            (*newDocCount)[ent2.first] +=ent2.second;
    }
    return newDocCount;
}




//TF-IDF calculation
map<string, map<double,string> > TFIDF(map<string, map<string, int,ci_less> >& directoryCount, map<string, int>& global_map_temp, int numFiles)
{

    using namespace std;
    map<string, map<double, string> > TFIDFmap;
    int count;



    map<string, map<string, int,ci_less> >::iterator ent;
    for(ent = directoryCount.begin(); ent != directoryCount.end(); ++ent)
    {
        map<string, int> global_map = global_map_temp;
    //for (auto& ent : directoryCount)
    //#pragma omp parallel task
    {
        
        count=0;
        map<string, double> temp;
        string filepath(output + "/" + ent->first );
        string filepath2(output2 + "/" + ent->first );
        ofstream output_file(filepath, std::ofstream::trunc);
        ofstream output_file2(filepath2, std::ofstream::trunc);

        for (auto& ent2 : ent->second)
        {
            string word = ent2.first;
            double tf = (ent->second)[word];
            double idf = log10(((double)numFiles) / (((double)global_map[word]) ) );
            double tfidf = tf * idf;
            //output_file << word << "," << tfidf << "\n";
            temp[word]=tfidf;
        }

        std::map<double, string> temp2 = flip_map(temp);
        std::map<double, string> temp3;

        std::map<double,string>::reverse_iterator ent2;
        for (ent2=temp2.rbegin(); ent2!=temp2.rend(); ++ent2)
        {
            if(count>WMAX)  //top WMAX words are relevant
                break;
            double freq = ent2->first;
            output_file << temp2[freq] << "," << freq << "\n";
            output_file2 << temp2[freq] << "\n";
            temp3[freq]=temp2[freq];
            count++;
            if(isupper(temp2[freq][0])) //phrases are relevant
                count--;
        }

        TFIDFmap[ent->first]=temp3;
        
        /*
        for (auto& ent : temp3)
            std::cout<<ent.second << "," << ent.first << "\n";
        */

        output_file.close();
        output_file2.close();
    }   
    }
    return TFIDFmap;
}




int main(int argc, char* argv[])
{
	double start,end;
	start =omp_get_wtime();
	Dictionary dict;
    if(argc<2)
    {
        printf("Usage: ./output directory\n");
        return 1;
    }

    int numProc;
    int self;
    int numFiles=0;
    vector<string> files;
    int i;

    
    
    //Reading directory
    struct dirent *pDirent;
    DIR *pDir;
    pDir = opendir (argv[1]);

    if (pDir == NULL)
    {
        printf ("Cannot open directory '%s'\n", argv[1]);
        return 1;
    }
    dir=argv[1];
    output = "./TFIDF/";
    output2 = "./Relevant/";
    
    while ((pDirent = readdir(pDir)) != NULL)
    {
        if((strcmp(pDirent->d_name,".")!=0)&&(strcmp(pDirent->d_name,"..")!=0))
        {
            //files[numFiles]= (char*)malloc(strlen(pDirent->d_name));
			string str(pDirent->d_name);
			files.push_back(str);
            //printf ("%s\n", files[numFiles]);
            numFiles++;
        }
    }
    

    Init(argc,argv);

    self = COMM_WORLD.Get_rank();
    numProc = COMM_WORLD.Get_size();   

    if(self==ROOT)
        printf("Number of files %d\n",numFiles );    

    map <string, map <string,int,ci_less> > directoryCount;

    int count=0;



    omp_set_dynamic(0);
    omp_set_num_threads(num_threads);
    int nthreads = omp_get_num_threads();



    //term frequency give books to each processor
    if(self!=ROOT)
    {
       	#pragma omp parallel for
        for(i=0;i<numFiles;i++)
        {
            if(i%(numProc-1)+1==self)
            {
                map <string,int,ci_less> bookCount = termfreq(files[i]);
		#pragma omp critical
                directoryCount[files[i]]=bookCount;
            }
        }

        map<string,int> documentCount = countDoc(directoryCount);

        //serial send map
        int size;
        
        char* buffer=serialize_container(documentCount,&size);
        int size_buffer[2]={size,self};
        

        COMM_WORLD.Send(size_buffer, 2, MPI_INT, ROOT, 0);
        
        COMM_WORLD.Send(buffer, size, MPI_BYTE, ROOT, 1000);
    }










    int global_size;
    char* global_buffer;

    if(self==ROOT)
    {
        map <string, map <string, int> > node_map;
        int buffer_size[2]; 

        int rank;

        //#pragma omp parallel for
        for(i=0;i<numProc-1;i++)
        {
             COMM_WORLD.Recv(buffer_size, 2, MPI_INT, MPI_ANY_SOURCE, 0);
             
             int doc_type;

             rank = buffer_size[1];
             //printf("%d\n",buffer_size[0]);

             //make rank a string
             std::stringstream temp2;
             temp2 << rank;
             std::string temp = temp2.str();
             
             char buffer[buffer_size[0]];         
             

             COMM_WORLD.Recv(buffer, buffer_size[0], MPI_BYTE, rank, 1000);
             
             
             //deserialize
             map <string, int> *deser_node_map = deserialize_container(buffer);
             

             //add to vector

             node_map[temp] = *deser_node_map;
        }


        map <string, int> *global_map = reduceDocCount(node_map);
        global_buffer = serialize_container(*global_map, &global_size);

    }

    
   

    COMM_WORLD.Bcast(&global_size, 1, MPI_INT, ROOT);



     if (self != ROOT) {
      global_buffer = new char[global_size];
   }

   //broadcast(global_doc_buf);
   COMM_WORLD.Bcast(global_buffer, global_size, MPI_BYTE, ROOT);

    

    if(self!=ROOT)
    {
        map <string, int> *global_map = deserialize_container(global_buffer);
        map<string, map<double,string> > TFIDFmap = TFIDF(directoryCount, *global_map, numFiles);   //CHECK need to remove the return type
    }




    
    Finalize();
    if(self==ROOT)
    {
        string line;
        
        for(i=0;i<numFiles;i++) //cannot do openmp for this as push_back reallocates so not thread safe operation
        {
            string tempfile(output2 + "/" + files[i] );
            std::ifstream input(tempfile);    
            while(std::getline(input, line)) 
            {
                if (line.empty())
                    continue;

                vector<string> v = split(line, "\n");
                for (auto& ent : v)
                {
                    if(dict[ent].size()<FMAX-1)    //FMAX condition
                    dict[ent].push_back(files[i]);   
                }

            }
            input.close();
        }
        

        std::ofstream outfile;//,std::ofstream::trunc);
	outfile.open("Index.txt");
        for (auto& ent : dict)
        {
            int flag=0;
            outfile<<ent.first << " : ";
            for (auto& ent2 : ent.second)
                if(flag==1)
                    outfile<<", "<<ent2;   

                else
                    {
                        outfile<<ent2;
                        flag=1;
                    }
            outfile<<"\n";
        }

        outfile.close();


    }
    

    closedir (pDir);
	end =omp_get_wtime();
	if(self==ROOT)
	printf("Time %lf\n",end-start);

    return 0;


}
