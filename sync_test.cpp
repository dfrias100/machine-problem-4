/* 
    File: sync_test.cpp
    Author: U. Chanchlani / R. Bettati
            Department of Computer Science
            Texas A&M University
    Date  : 2019/10/04
            2020/09/26
    Testing Suite for MP2/MP3 in CSCE313
    Make sure to compile with -lpthread option.
*/

/*--------------------------------------------------------------------------*/
/* DEFINES */
/*--------------------------------------------------------------------------*/

    
/* 
 * One of the three test macro needs to be defined.
 * You can define it either here or in compiler directives.
 * For the latter, compile the program with option 
 *    -DMUTEX_TEST to test class Mutex, 
 *    -DMUTEX_GUARD_TEST to test MutexGuard,
 *    -DSEMAPHORE_TEST to test Semaphore,
 *    -DPCBUFFER_TEST to test PCBuffer.
 */

// #define MUTEX_TEST
// #define MUTEX_GUARD_TEST
// #define SEMAPHORE_TEST
// #define PCBUFFER_TEST

/*--------------------------------------------------------------------------*/
/* INCLUDES */
/*--------------------------------------------------------------------------*/

#include <iostream>
#include <unistd.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include <assert.h>


using namespace std;

/*--------------------------------------------------------------------------*/
/* ADDITIONAL INCLUDES AND GLOBAL VARIABLES */
/*--------------------------------------------------------------------------*/

#ifdef MUTEX_TEST
#include "mutex.hpp"
Mutex m;
#elif MUTEX_GUARD_TEST
#include "mutex_guard.hpp"
Mutex m;
#elif SEMAPHORE_TEST
#include "semaphore.hpp"
Semaphore s(5);
#elif PCBUFFER_TEST
#include "pcbuffer.hpp"
#endif

/*--------------------------------------------------------------------------*/
/* DATA STRUCTURES */ 
/*--------------------------------------------------------------------------*/

struct ThreadData {
  long threadId;
  unsigned long iters;
  unsigned long sleepTime;
};

/*--------------------------------------------------------------------------*/
/* SUPPORT FUNCTIONS TO TEST MUTEX AND SEMAPHORE */
/*--------------------------------------------------------------------------*/

#if defined(MUTEX_TEST) || defined(MUTEX_GUARD_TEST) || defined(SEMAPHORE_TEST)

string generateThreadStats(ThreadData *tData, int iter) {
  stringstream ss;
  ss << "Thread " << tData->threadId << ", iter: " << iter << endl;
  return ss.str();
}

void* thread_function(void *threadData) {
  ThreadData* tData = (ThreadData*) threadData;
  for(int i = 0; i < tData->iters; i++) {
#ifdef MUTEX_TEST
    m.Lock();
#elif MUTEX_GUARD_TEST
    { auto mg = MutexGuard(m);
#elif SEMAPHORE_TEST 
    s.P();
#endif
    
    cout << generateThreadStats(tData, i);
    usleep(tData->sleepTime); // takes some time in critical section

#ifdef MUTEX_TEST
    m.Unlock();
#elif MUTEX_GUARD_TEST
    }
#elif SEMAPHORE_TEST
    s.V();
#endif
    
    usleep(tData->sleepTime); // takes some time in non critical section
  }
  return NULL;
}

void run_test_threads(int numThreads, int numIters, int sleepTime) {
  pthread_t* threads = new pthread_t[numThreads];
  ThreadData* threadData = new ThreadData[numThreads];
  for(int i = 0; i < numThreads; i++) {
    threadData[i].threadId = i;
    threadData[i].iters = numIters;
    threadData[i].sleepTime = sleepTime;
    int rc = pthread_create(threads + i, NULL, thread_function, (void *)(threadData + i));
    if (rc < 0) {
      cerr << "Thread unable to start... exitting" << endl;
      exit(1);
    }
  }
  for(int i = 0; i < numThreads; i++) {
    pthread_join(threads[i], NULL);
  }
  cout << "All test threads terminated." << endl;
  delete[] threads;
  delete[] threadData;
}

#endif

/*--------------------------------------------------------------------------*/
/* SUPPORT FUNCTIONS TO TEST MUTEX AND SEMAPHORE */
/*--------------------------------------------------------------------------*/

#ifdef PCBUFFER_TEST

PCBuffer buffer(100);

void * produce_items(void *) {
  for (int i = 0; i < 100000; i++) {
    cout << "P: depositing " << i << endl;
    buffer.Deposit(to_string(i));
  }
  buffer.Deposit(to_string(-1)); // We deposit a '-1' to stop the consumer.

  cout << "Producer: done" << endl;

  return nullptr; // to keep compiler happy
}

void * consume_items(void *) {
  string val;
  do {
    val = buffer.Retrieve();
    if (stoi(val) >= 0) {
      cout << "C: Removed " << val << endl;
    }
  } while (stoi(val) >= 0);

  // We read a negative number,
  // which means that the producer wants us to stop
  
  cout << "Consumer: done" << endl;
  
  return nullptr; // to make compiler happy
}

void run_pcbuffer_test() {
  pthread_t producer;
  pthread_t consumer;

  assert(pthread_create(&producer, NULL, produce_items, NULL) == 0);
  assert(pthread_create(&consumer, NULL, consume_items, NULL) == 0);

  assert(pthread_join(producer, NULL) == 0);
  assert(pthread_join(consumer, NULL) == 0);
}

#endif

/*--------------------------------------------------------------------------*/
/* MAIN FUNCTION */
/*--------------------------------------------------------------------------*/

int main(int argc, char* argv[]) {

#ifdef PCBUFFER_TEST
  run_pcbuffer_test();
#else
  run_test_threads(10, // Number of threads to spin off
      10, // Number of iterations each thread will take
      500000 // Wait time in usecs. This value is 0.5 sec
    );
#endif

  cout << "Done with the test." << endl
       << "Check output to determine if synchronization worked correctly." << endl;

  return 0;
}