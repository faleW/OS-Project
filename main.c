/*
* NOTE TO STUDENTS: Before you do anything else, please
* provide your group information here.
*
* Group No.: 25 (Join a project group in Canvas)
* First member's full name: Tang Wing Yi William
* First member's email address: wwytang2-c@my.cityu.edu.hk
* Second member's full name: Ng Ho Man
* Second member's email address: homanng7-c@my.cityu.edu.hk
* Third member's full name: Chan Tsz Hin
* Third member's email address: thchan384-c@my.cityu.edu.hk
*/
#include <stdio.h>
#include <stdint.h>   // portable int types
#include <fcntl.h>    // open
#include <unistd.h>   // close
#include <assert.h>   // assert
#include <stdlib.h>   // exit
#include <sys/stat.h> // file stat
#include <sys/mman.h> // mmap
#include <string.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <dirent.h>

///////////////////////////////////////////////////////////////
//////////// Buffer to save the data allocated by producer ///////////

int front = 0, rare = 0;
struct buffer
{
    char *address;
    int file_number; //File Number
    int size;
} * buf;

struct output
{
    char *data;
    int *count;
    int size;
} * out;

/* simply add item to the buffer.*/
void put(struct buffer b)
{
    buf[front] = b; //Enqueue the buffer
    front++;
}


/* get the buffer item and return the last index of the queue.*/
struct buffer get()
{
    struct buffer b = buf[rare];
    rare++;   //in order to get the next buffer
    return b;
}


///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////



int num_files;
char **allPaths;
int isComplete = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER, fill = PTHREAD_COND_INITIALIZER;


///////////////////////////////////////////////////////////////
/////////////////////////Find all files Paths//////////////////////

//A function to mix two string
/*Copy character up to a given position, which is the length of a pair of character and then concatenate the two parameters. This will be used to find path of directories or files.*/
char *sumString(char *str1, char *str2)
{
    int len = strlen(str1) + strlen(str2) + 1;
    char *concated = malloc(sizeof(char) * len);

    memset(concated, '\0', len);


    strcat(concated, str1);
    strcat(concated, str2);
    return concated;
}




/* Get the path to all content using recursion, depends on whether the parameter is directory or file.*/
void findAllPath(char *directory)
{
    DIR *d;
    struct dirent *dir;
    d = opendir(directory);

//If the path is directory, open it and run findAllPath() in order to find all the file
    if (d)
    {
        (dir = readdir(d));
        (dir = readdir(d));
        while ((dir = readdir(d)) != NULL)
        {

            findAllPath(sumString(sumString(directory, "/"), dir->d_name));
        }
        closedir(d);
    }
    else //If it is a file, add the file to allPaths array
    {


        allPaths[num_files++] = directory;
    }
}
///////////////////////////////////////////////////////////////
/////////////////////////Print the output//////////////////////
/* For loop the output data and then print it out.
*/
void write_out()
{
    for (int i = 0; i < num_files; i++)
    {


        for (int j = 0; j < out[i].size + 1; j++)
        {
            // printf("%d\n", out[i].size);
            int num = out[i].count[j];
            char character = out[i].data[j];


            fwrite(&num, sizeof(int), 1, stdout);
            fwrite(&character, sizeof(char), 1, stdout);
        }
    }
}

///////////////////////////////////////////////////////////////



///////////////////////////////////////////////////////////////
///////////////////////////Producer////////////////////////////
/*
Producer will allocate all files to buffer array and wake up consumer.
How to allocate:
1: open file
2: use MMap to get the address
3: Summarize the file size, address and index to buffer
4: put to buffer into buffer array
*/

void *producer(void *arg)
// void compress(FILE* infile)
{
    struct stat stat_buf;

//Loop all files
    for (int i = 0; i < num_files; i++)
    {

//Open file
        int fd = open(allPaths[i], O_RDONLY);



        fstat(fd, &stat_buf);
        int size = stat_buf.st_size;
        //printf("%d",);
        if (size == 0) // If the file is empty, skip it;
        {
            num_files = num_files - 1;
            continue;
        }


        struct buffer temp;

//mmap file
        char *start_addr = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        assert(start_addr != MAP_FAILED);
//Generate buffer data
        temp.address = start_addr; // Allocate address
        temp.file_number = i;    //Position of printing output
        temp.size = size;        //Size of file

        pthread_mutex_lock(&lock); // Product buffer array
        put(temp); // Put the buffer data to buffer
        pthread_mutex_unlock(&lock); // Unlock buffer array
        pthread_cond_signal(&fill);
    }
    isComplete = 1; // notice consumer what producer finish mapping
    pthread_cond_broadcast(&fill); //Wake-up all the sleeping consumer threads.
    return 0;
}

///////////////////////////////////////////////////////////////
///////////////////////////Consumer////////////////////////////

// create a number of threads that start to compress the input data.
// If the queue size is 0 and it is not complete. And then the program will wake up the producer thread to fill the queue.
// Next if is complete then unlock the thread. Since it is not allowed that the consumer get the data from queue at the same time.
// So, there will have a mutex lock in the getting buffer, unlock after this. Finally put the compress result in a new array according to the number of files.
// Therefore, preventing race condition.

void *consumer()
{
    // avoid the consumer get the file at the same time
    pthread_mutex_lock(&lock);

    while (front == rare && isComplete == 0) //nothing in the queue
    {

        pthread_cond_signal(&empty);
        pthread_cond_wait(&fill, &lock); //call the producer to start filling.

    }


    if (isComplete == 1 && front == rare)
    { //If producer is done and the queue reach the end
        pthread_mutex_unlock(&lock);
        return NULL;
    }


//get file data
    struct buffer temp = get();

    // mutex becomes available, the scheduling policy shall determine which thread shall acquire the mutex.
    pthread_mutex_unlock(&lock);


    struct output compressed;

    compressed.count = malloc(temp.size * sizeof(int));
    compressed.data = malloc(temp.size * sizeof(char));

    int rc = -1;
    char prev_ch = -1;
    char ch;
    int count = 0;
    char *ptr = temp.address;
    int countIndex = 0;


    while (ptr < temp.address + temp.size)
    {
        // read char
        ch = *ptr;
        ptr++;


        // compress
        if (count == 0) // initialize
        {
            prev_ch = ch;
            count = 1;
            compressed.count[countIndex] = 1;
        }
        else if (ch == prev_ch) // if it same word then count+1
        {


            count += 1;
        }
        else // new char
        {

// save the compress into out for printing
            compressed.data[countIndex] = prev_ch;


            compressed.count[countIndex] = count;


            prev_ch = ch;
            count = 1;

//save compress data in next index 
            countIndex++;
        }
    }


    if (count > 0) // last compressed unit hasn't been written out
    {


        compressed.data[countIndex] = prev_ch;
        compressed.count[countIndex] = count;
    }

    compressed.size = countIndex;


    rc = munmap(temp.address, temp.size); // un-mmap the file
    assert(rc == 0);


    out[temp.file_number] = compressed;


    return NULL;
}

///////////////////////////////////////////////////////////////
////////////////////////////Main///////////////////////////////

int main(int argc, char *argv[])
{

//num of cosumer threads
    int total_threads = get_nprocs();

//malloc memory to save the output data
    out = malloc(sizeof(struct output) * 512000 * 4);


    pthread_t pid, cid[total_threads];


    // Check if it do not more than two arguments
    if (argc < 2)
    {
        printf("pzip: file1 [file2 ...]\n");
        exit(1);
    }

//Initialize allPaths array
    allPaths = malloc(sizeof(char **) * 100);

//record how many files
    num_files = 0;

//use to find all the paths and then convert to string and save it
//we will use to open the file 
    for (int i = 1; i < argc; i++)
    {
        findAllPath(argv[i]);
    }

//malloc the memory in order to allocate the input file data 
    buf = malloc(sizeof(struct buffer *) * num_files);

//create a thread to allocate the file data in buffer
    pthread_create(&pid, NULL, producer, argv + 1);



//create consumer threads to compress all the file and save into out
    for (int j = 0; j < total_threads; j++)
    {
        pthread_create(&cid[i], NULL, consumer, NULL);
    }

//Wait for producer-consumers to finish.
    for (int k = 0; k < total_threads; k++)
    {
        pthread_join(cid[i], NULL);
    }
    pthread_join(pid, NULL);

//print output
    write_out();
}



