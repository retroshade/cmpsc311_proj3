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
#include <signal.h>

#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

struct arguments {
  struct map_reduce *mr;
  char *inpath;
  int mapper_id;
};

static void * mr_child_func(void *);
static void * mr_reduce_child_func(void *);

static void * mr_child_func(void *arg)
{
  printf("child function\n");
  // make shared memory buffers
  struct arguments *argument;
  argument = (struct arguments*)arg;

  //struct map_reduce *mp;
  // mp = argument->mr;
  int j;
  map_fn map_func = argument->mr->mf;
  printf("Mapper id: %d\n", argument->mapper_id);
  // int i;
  // for (i = 0; i < argument->mr->nthreads; i++) {
  // printf("thread %d id in child func: %d\n", i, (int)argument->mr->child_threads[i]);
  // }

  j = map_func(argument->mr, 0, argument->mapper_id, argument->mr->nthreads);
  //printf("%d\n", j);

  //mr_finish(argument->mr);

  return 0;
}

// return 0 upon success and return anything else indicates failure
// function to create a new thread and call reduce function
static void * mr_reduce_child_func(void *arg) {
  printf("Reduce child function\n");

  struct arguments *argument;
  argument = (struct arguments*)arg;
  reduce_fn reduce_func = argument->mr->rf;
  
  printf("mapper id: %d\n", argument->mapper_id);
  printf("thread id: %d\n", (int)argument->mr->child_threads[argument->mr->nthreads]);

  int j;
  j = reduce_func(argument->mr, 1, argument->mr->nthreads);
  printf("exiting reduce child function\n");
  return 0;
}

/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int threads)
{
  printf("create function\n");
  // use malloc() to allocate memory for an instance of the struct map_reduce
  // if mp is not empty, that means it worked correctly
  struct  map_reduce * mp =  malloc(sizeof(struct map_reduce));
  if (mp != NULL) {
     mp->nthreads = threads;
     mp->mf = map;
     mp->rf = reduce;
     // printf("returning mp\n");
    return mp;
  }
  else
    return NULL;
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{
  printf("destroy function\n");
  // use free to deallocate memory for the map_reduce struct
  free(mr);

}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
   printf("start function\n");
  // open the files in here
  if (fopen(inpath, "r"))
    printf("Inpath open\n");
  if(fopen(outpath, "w"))
    printf("Outpath open\n");

  // pthread_t child_array[mr->nthreads];
  // pthread_t *child_array = NULL;

  // create the threads
  int i, success;
  for (i = 0; i < mr->nthreads; i++)
    {
      struct arguments * arg = (struct arguments *)malloc(sizeof(struct arguments));
      arg->mr = mr;
      arg->mapper_id = i;
      // arg->inpath = inpath;
      
      printf("new thread\n");
      // success = pthread_create(&child_array[i], NULL, mr_child_func, (void*) arg);
      success = pthread_create(&arg->mr->child_threads[i], NULL, mr_child_func, (void*) arg);
      // this line is broken vvvvvvvv
      // mr->child_threads[i] = child_array[i];
      printf("thread id: %d\n", (int)mr->child_threads[i]);
      if(success != 0) {
	printf("Error creating thread\n");
	return 1;
      }
      // arg->mr = mr;
      int h;
      for (h = 0; h <= i; h++) {
	printf("thread id for %d is %d\n", h, (int)arg->mr->child_threads[h]);
      }

    }

  // create another thread for the reduce function
  struct arguments * arg = (struct arguments *) malloc(sizeof(struct arguments));
  arg->mr = mr;
  arg->mapper_id = i;
  printf("Creating thread for reduce function\n");
  success = pthread_create(&arg->mr->child_threads[i], NULL, mr_reduce_child_func, (void*) arg);
  printf("thread id: %d\n", (int)arg->mr->child_threads[i]);
  if(success != 0) {
    printf("Error creating thread\n");
    return 1;
 }
  // arg->mr->child_threads[i] = child_array[i];

  int j;
  for (j = 0; j <= mr->nthreads; j++) {
    printf("thread %d id: %d\n", j, (int)mr->child_threads[j]);
  }

  return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
  // returns 0 if every Map and Reduce function returned 0 and nonzero if any of the Map or reduce functions failed
  int returnValue = 0;
  int i;

  for (i = 0; i <= mr->nthreads; i++) {
    printf("thread %d id in finish: %d\n", i, (int)mr->child_threads[i]);
  }

  for (i = 0; i <= mr->nthreads; i++) {
    printf("wait for %d\n", (int)mr->child_threads[i]);
    returnValue += pthread_join(mr->child_threads[i], 0);
    printf("%d finished\n", (int)mr->child_threads[i]);
  }
  return returnValue;
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
