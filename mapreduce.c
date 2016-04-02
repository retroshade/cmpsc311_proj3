/*
 * Implementation file for simple MapReduce framework.  Fill in the functions
 * and definitions in this file to complete the assignment.
 *
 * Place all of your implementation code in this file.  You are encouraged to
 * create helper functions wherever it is necessary or will make your code
 * clearer.  For these functions, you should follow the practice of declaring
 * them "static" and not including them in the header file (which should only be
 * used for the *public-facing* API.
 */


/* Header includes */
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>

#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024


/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int threads)
{
	int i;//, success;
	const int nThreads = threads;
	pthread_t child_threads[nThreads] = NULL;

	struct map_reduce * mp = (struct map_reduce *)malloc(sizeof(struct map_reduce));	//allocates the memory for map reduce pointer
	
	if (mp != NULL) {	//if memory is allocated correctly
		int i;
		for (i = 0; i < threads; i++) {		//loop through the number of threads requested
			if (pthread_create(&child_threads[i], NULL, int mr_start(mr, NULL, NULL), NULL) != 0) {		//create a thread that starts
				printf("Error creating thread %d", i);
				return NULL;			
			}
			
		}

		return mp;	//return the pointer
	}
	else {			//otherwise, if the memory allocation fails
		return NULL;	//return NULL
	}
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{
	free(mr);	//releases the memory
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	return 0;
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
	return 0;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	return 0;
}
