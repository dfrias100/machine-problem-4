/* 
    File: semaphore.cpp

    Author: Daniel Frias
            Department of Computer Science
            Texas A&M University
    Date  : 2020/09/30

    Source code for the Semaphore class.
*/

/*--------------------------------------------------------------------------*/
/* DEFINES */
/*--------------------------------------------------------------------------*/

/* -- (none) -- */

/*--------------------------------------------------------------------------*/
/* INCLUDES */
/*--------------------------------------------------------------------------*/

#include "semaphore.hpp"

/*--------------------------------------------------------------------------*/
/* NAME SPACES */ 
/*--------------------------------------------------------------------------*/

/* -- (none) -- */

/*--------------------------------------------------------------------------*/
/* DATA STRUCTURES */ 
/*--------------------------------------------------------------------------*/

/* -- (none) -- */

/*--------------------------------------------------------------------------*/
/* CONSTANTS */
/*--------------------------------------------------------------------------*/

/* -- (none) -- */

/*--------------------------------------------------------------------------*/
/* FORWARDS */
/*--------------------------------------------------------------------------*/

/* -- (none) -- */

/*--------------------------------------------------------------------------*/
/* FUNCTIONS FOR CLASS S e m a p h o r e */
/*--------------------------------------------------------------------------*/

Semaphore::Semaphore(int _val) : value(_val) {
    m = PTHREAD_MUTEX_INITIALIZER;
    c = PTHREAD_COND_INITIALIZER;
}

Semaphore::~Semaphore() {
    pthread_cond_destroy(&c);
    pthread_mutex_destroy(&m);
}

int Semaphore::P() {
    pthread_mutex_lock(&m);
    /* If the semaphore value is 0, it should not let any threads in. As the value of the semaphore
       indicates the number of threads that can be there at once. */
    while (value == 0)
        pthread_cond_wait(&c, &m);
    /* Once the value is greater than zero, then we can decrease the value once more. */
    value--;
    pthread_mutex_unlock(&m);
    return value;
}

int Semaphore::V() {
    pthread_mutex_lock(&m);
    value++;
    /* A thread waiting on a semaphore will automatically be signalled once the value has been incremented. */
    pthread_cond_signal(&c);
    pthread_mutex_unlock(&m);
    return value;
}
