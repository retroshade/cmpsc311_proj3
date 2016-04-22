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
#include <sys/stat.h>
#include <fcntl.h>
#include "mapreduce.h"
#include <unistd.h>
#include <string.h>

/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

struct arguments {
  struct map_reduce *mr;
  int mapper_id;
};

static void * mr_child_func(void *);
static void * mr_reduce_child_func(void *);

static void * mr_child_func(void *arg)
{
  printf("child function\n");
  // make shared memory buffers
  struct arguments *argument = (struct arguments*)arg;
  int j;
  map_fn map_func = argument->mr->mf;
  printf("argument->mapperid: %d\n", argument->mapper_id);
  printf("file desc: %d\n", argument->mr->infd[argument->mapper_id]);

  j = map_func(argument->mr, argument->mr->infd[argument->mapper_id], argument->mapper_id, argument->mr->nthreads);

  if (j != 0) {
    printf("map function failure\n");
    printf("j = %d\n", j);
    argument->mr->fail = j;
  }
  free(arg);
  return 0;
}

// return 0 upon success and return anything else indicates failure
// function to create a new thread and call reduce function
static void * mr_reduce_child_func(void *arg) {
  printf("Reduce child function\n");

  struct arguments *argument;
  argument = (struct arguments*)arg;
  reduce_fn reduce_func = argument->mr->rf;

  int j;
  j = reduce_func(argument->mr, argument->mr->outfd, argument->mr->nthreads);

  if (j != 0) {
    printf("reduce function failure\n");
    // printf("j = %d\n", j);
    argument->mr->fail = j;
  }

  free(arg);
  return 0;
}

/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int threads)
{
  printf("create function\n");
  // use malloc() to allocate memory for an instance of the struct map_reduce
  // if mp is not empty, that means it worked correctly

  struct map_reduce * mp = malloc(sizeof(struct map_reduce));

  if (mp != NULL) {
     mp->nthreads = threads;
     mp->mf = map;
     mp->rf = reduce;
     mp->fail = 0;
     pthread_mutex_init(&mp->lock, NULL);
     mp->infd = malloc(sizeof(int) * threads);
     mp->buffer = malloc(MR_BUFFER_SIZE);
     //mp->ids = malloc(sizeof(int) * (threads + 1));
     int m;
     for (m = 0; m < threads; m++) {
     	 *(mp->buffer + m) = malloc(sizeof(int));
     }
     mp->bufferValproduce = malloc(sizeof(int) * threads);
     mp->bufferValconsume = malloc(sizeof(int) * threads);
     int n;
     for (n = 0; n < threads; n++) {
   	mp->bufferValproduce[n] = 0;
        mp->bufferValconsume[n] = 0;
     }
     mp->child_threads = malloc (sizeof(pthread_t) * (threads + 1));
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
  if (mr->child_threads != NULL) {
    free(mr->child_threads);
  }
  int m;
  for (m = 0; m < mr->nthreads; m++) {
  	free(mr->buffer[m]);
	//free(&mr->bufferValproduce[m]);
	//free(&mr->bufferValconsume[m]);
  }
  free(mr->buffer);
  //free(mr->bufferValproduce);
  //free(mr->bufferValconsume);
  free(mr->infd);
  pthread_mutex_destroy(&mr->lock);
  free(mr);

}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
   printf("start function\n");
  // open the files in here
   FILE *out;
   int h;
   for (h = 0; h <= mr->nthreads; h++) {
     if ((mr->infd[h] = open(inpath, O_RDONLY)) < 0) {
       return 1;
     }
   }
   if ((mr->outfd = open(outpath, O_WRONLY | O_TRUNC)) < 0) {
     out = fopen(outpath, "w");
     mr->outfd = fileno(out);
   }

  // create the threads
  int i, success;

  pthread_t childthreads[mr->nthreads + 1];
  for (i = 0; i < mr->nthreads; i++)
  {
    struct arguments * arg;
    if ((arg = malloc(sizeof(struct arguments))) == NULL) {
      return 1;
    }
    arg->mr = mr;
    arg->mapper_id = i;
    mr->id = arg->mapper_id;
    

     //mr->bufferValproduce[i] = 0;
     //mr->bufferValconsume[i] = 0; 
      success = pthread_create(&childthreads[i], NULL, mr_child_func, (void*) arg);
      
      if(success != 0) {
	printf("Error creating thread\n");
	return 1;
      }

      mr->child_threads[i] = childthreads[i];
    }

  // create another thread for the reduce function
  struct arguments * arg = malloc(sizeof(struct arguments));
  arg->mr = mr;
  arg->mapper_id = i;
  printf("Creating thread for reduce function\n");
  success = pthread_create(&childthreads[i], NULL, mr_reduce_child_func, (void*) arg);
  if(success != 0) {
    printf("Error creating thread\n");
    return 1;
 }
  mr->child_threads[i] = childthreads[i];

  return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
  // returns 0 if every Map and Reduce function returned 0 and nonzero if any of the Map or reduce functions failed

  int i;
  for (i = 0; i <= mr->nthreads; i++) {
    pthread_join(mr->child_threads[i], 0);
  }
  
   int t;
  for (t = 0; t <= mr->nthreads; t++) {
      close(mr->infd[t]);
  }
  close(mr->outfd);

  if (mr->fail != 0)
    return 1;
  return 0;
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
  printf("producer function\n");

  pthread_mutex_lock(&mr->lock);
  printf("producer lock\n");
  
  printf("%d\n", mr->bufferValproduce[id]);

  memcpy(&mr->buffer[id][mr->bufferValproduce[id]], &kv->keysz, sizeof(kv->keysz));
  mr->bufferValproduce[id] += (int)sizeof(kv->keysz);
  printf("%d\n", mr->bufferValproduce[id]);
  if ((int)mr->bufferValproduce[id] >= 1024) {
    mr->bufferValproduce[id] = 0;
    memcpy(&mr->buffer[id][mr->bufferValproduce[id]], &kv->keysz, sizeof(kv->keysz));
    mr->bufferValproduce[id] += (int)sizeof(kv->keysz);
  }

  memcpy(&mr->buffer[id][mr->bufferValproduce[id]], kv->key, kv->keysz);
  printf("%s\n", &mr->buffer[id][mr->bufferValproduce[id]]);
  mr->bufferValproduce[id] += (int)kv->keysz;
  if ((int)mr->bufferValproduce[id] >= 1024) {
    mr->bufferValproduce[id] = 0;
    memcpy(&mr->buffer[id][mr->bufferValproduce[id]], kv->key, kv->keysz);
    mr->bufferValproduce[id] += (int)(kv->keysz);
  }
  printf("%d\n", mr->bufferValproduce[id]);
  printf("key is in\n");
  
  memcpy(&mr->buffer[id][mr->bufferValproduce[id]], &kv->valuesz, sizeof(kv->valuesz));
  mr->bufferValproduce[id] += (int)sizeof(kv->valuesz);
  if ((int)mr->bufferValproduce[id] >= 1024) {
    mr->bufferValproduce[id] = 0;
    memcpy(&mr->buffer[id][mr->bufferValproduce[id]], &kv->valuesz, sizeof(kv->valuesz));
    mr->bufferValproduce[id] += (int)sizeof(kv->valuesz);
  }
  printf("%d\n", mr->bufferValproduce[id]);  

  memcpy(&mr->buffer[id][mr->bufferValproduce[id]], kv->value, kv->valuesz);
  mr->bufferValproduce[id] += (int)kv->valuesz;
  if ((int)mr->bufferValproduce[id] >= 1024) {
    mr->bufferValproduce[id] = 0;
    memcpy(&mr->buffer[id][mr->bufferValproduce[id]], kv->value, kv->valuesz);
    mr->bufferValproduce[id] += (int)kv->valuesz;
  }
  printf("val is in\n");
  printf("%d\n", mr->bufferValproduce[id]);  

  pthread_mutex_unlock(&mr->lock);
  
  // called by a map thread each time it produces a kv pair to be consumed by the reducer thread
  // returns 1 upon success (one key-value pair is successfully produced)
  // returns -1 upon failure
  return 1;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
  printf("consumer function\n");
  // returns 1 if one pair is successfully consumed
  // returns 0 if the map thread returns-- no more values left in that particular mapper
  // returns -1 on error

  printf("%d\n", mr->bufferValconsume[id]);

  pthread_mutex_lock(&mr->lock);
  printf("consumer lock\n");
 
  memcpy(&kv->keysz, &mr->buffer[id][mr->bufferValconsume[id]], sizeof(kv->keysz));
  mr->bufferValconsume[id] += (int)(sizeof(kv->keysz));
  if ((int)mr->bufferValconsume[id] >= 1024) {
    mr->bufferValconsume[id] = 0;
    memcpy(&kv->keysz, &mr->buffer[id][mr->bufferValconsume[id]], sizeof(kv->keysz));
    mr->bufferValconsume[id] += (int)(sizeof(kv->keysz));
  }
  printf("%d\n", mr->bufferValconsume[id]);

  memcpy(kv->key, &mr->buffer[id][mr->bufferValconsume[id]], (int)kv->keysz);
  mr->bufferValconsume[id] += (int)kv->keysz;
  if ((int)mr->bufferValconsume[id] >= 1024) {
    mr->bufferValconsume[id] = 0;
    memcpy(kv->key, &mr->buffer[id][mr->bufferValconsume[id]], (int)kv->keysz);
    mr->bufferValconsume[id] += (int)kv->keysz;
  }
  printf("%d\n", mr->bufferValconsume[id]);

  memcpy(&kv->valuesz, &mr->buffer[id][mr->bufferValconsume[id]], sizeof(kv->valuesz));
  mr->bufferValconsume[id] += (int)(sizeof(kv->valuesz));
  if ((int)mr->bufferValconsume[id] >= 1024) {
    mr->bufferValconsume[id] = 0;
    memcpy(&kv->valuesz, &mr->buffer[id][mr->bufferValconsume[id]], sizeof(kv->valuesz));
    mr->bufferValconsume[id] += (int)(sizeof(kv->valuesz));
  }

  memcpy(kv->value, &mr->buffer[id][mr->bufferValconsume[id]], kv->valuesz);
  mr->bufferValconsume[id] += (int)kv->valuesz;
  if ((int)mr->bufferValconsume[id] >= 1024) {
    mr->bufferValconsume[id] = 0;
    memcpy(kv->value, &mr->buffer[id][mr->bufferValconsume[id]], kv->valuesz);
    mr->bufferValconsume[id] += (int)kv->valuesz;
  }
  printf("%d\n", mr->bufferValconsume[id]);

  pthread_mutex_unlock(&mr->lock);

  

  return 1;
}
