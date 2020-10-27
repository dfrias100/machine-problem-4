/* 
    File: pcbuffer.cpp

    Author: Daniel Frias
            Department of Computer Science
            Texas A&M University
    Date  : 2020/09/30

    Source code for the PCBuffer class.
*/

/*--------------------------------------------------------------------------*/
/* DEFINES */
/*--------------------------------------------------------------------------*/

/* -- (none) -- */

/*--------------------------------------------------------------------------*/
/* INCLUDES */
/*--------------------------------------------------------------------------*/

#include "pcbuffer.hpp"

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
/* FUNCTIONS FOR CLASS P C B u f f e r */
/*--------------------------------------------------------------------------*/

PCBuffer::PCBuffer(int _size) : size(_size), empty(_size), full(0), mutex(1) {
    buffer = new std::string[size];
    nextin = 0;
    nextout = 0;
    count = 0;
}

PCBuffer::~PCBuffer() {
    delete[] buffer;
}

int PCBuffer::Deposit(std::string _item) {
    empty.P();
    mutex.P(); // If empty came after the mutex, there is a potential deadlock if the PCBuffer is truly empty.
    buffer[nextin] = _item;
    nextin = (nextin + 1) % size;
    mutex.V();
    full.V();
    return 0;
}


std::string PCBuffer::Retrieve() {
    full.P();
    mutex.P();
    std::string ret = buffer[nextout];
    nextout = (nextout + 1) % size;
    mutex.V();
    empty.V();
    return ret;
}
