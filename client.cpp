/* 
    File: client.cpp

    Author: Daniel Frias
            Department of Computer Science
            Texas A&M University
    Date  : 

    Client main program for MP3
*/

/*--------------------------------------------------------------------------*/
/* DEFINES */
/*--------------------------------------------------------------------------*/

    /* -- (none) -- */

/*--------------------------------------------------------------------------*/
/* INCLUDES */
/*--------------------------------------------------------------------------*/

#include <cassert>
#include <cstring>
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/select.h>

#include <errno.h>
#include <unistd.h>
#include <pthread.h>

#include "reqchannel.hpp"
#include "pcbuffer.hpp"

/*--------------------------------------------------------------------------*/
/* DATA STRUCTURES */ 
/*--------------------------------------------------------------------------*/

typedef struct {
    size_t* n_req_threads;
    size_t n_req;
    PCBuffer* PCB;
    std::string patient_name;
} RTFargs;

typedef struct {
    PCBuffer* PCB;
    std::unordered_map<int, RequestChannel*>* rq_chans;
    fd_set* readset;
    int maxfd;
} EHargs;

Semaphore n_req_thread_count_mutex(1);

/*--------------------------------------------------------------------------*/
/* CONSTANTS */
/*--------------------------------------------------------------------------*/

const size_t NUM_PATIENTS = 5;

/*--------------------------------------------------------------------------*/
/* FORWARDS */
/*--------------------------------------------------------------------------*/

    /* -- (none) -- */

/*--------------------------------------------------------------------------*/
/* LOCAL FUNCTIONS -- SUPPORT FUNCTIONS */
/*--------------------------------------------------------------------------*/

void* request_thread_func(void* rtfargs) {
    RTFargs* args = (RTFargs*) rtfargs;
    size_t* n_req_threads = args->n_req_threads;
    for (size_t i = 0; i < args->n_req; i++) {
        std::string req = "data " + args->patient_name;
        std::cout << "Depositing request..." << std::endl;
	    args->PCB->Deposit(req);
    }

    // The number of threads active is a shared variable, so we need to synchronize the access to the variable.
    n_req_thread_count_mutex.P();
    if (*n_req_threads != 1)
        *n_req_threads = *n_req_threads - 1;
    else {
        args->PCB->Deposit("done");
        *n_req_threads = *n_req_threads - 1;
    }
    std::cout << "Request thread finished." << std::endl;
    n_req_thread_count_mutex.V();

    return nullptr;
}

void* event_handler_func(void* ehargs) {
    EHargs* args = (EHargs *) ehargs;
    fd_set* readset = args->readset;
    int maxfd = args->maxfd;
    // We need this to break out of the nested for loop
    bool done = false;
    std::unordered_map<int, RequestChannel*>* rq_chans = args->rq_chans;
    for(;;) {
        // We still need the original fd_set to determine if any new files are ready
        // so we copy it into this fd_set every loop
        fd_set active_readset = *readset;
        if (select(maxfd + 1, &active_readset, NULL, NULL, NULL) > 0 && !done) {
            for (int fd = 0; fd <= maxfd; fd++) {
                // If the file decriptor(s) is/are not "set" we wait until it is
                if(!FD_ISSET(fd, &active_readset))
                    continue;
                else {
                    RequestChannel* rq_chan = rq_chans->find(fd)->second;
                    std::string reply = rq_chan->cread();

                    std::string req = args->PCB->Retrieve();
                    // "done" is not a valid request to the server, we have to break and teardown all of the request
                    // channels using "quit"
                    if (req.compare("done") == 0) {
                        done = true;
                        break;
                    }
                    rq_chan->cwrite(req);
                }
            }
        } else {
            break;
        }
    }
    for (auto i : *rq_chans) {
        i.second->send_request("quit");
        // Giving it a little bit of time to close and delete the named pipes
        usleep(1000);
    }
    return nullptr;
}

/* These functions prepare the arguments and then creates the thread; The thread is linked to their arguments with the
   the thread index that it takes in, along with the other required objects needed to create the thread */

void create_requester(int _thread_num, int _num_requests, std::string _patient_name, PCBuffer* _PCB, 
                        pthread_t* rq_threads, size_t* _n_req_threads, RTFargs* args) {
    args[_thread_num].n_req = _num_requests;
    args[_thread_num].patient_name = _patient_name;
    args[_thread_num].PCB = _PCB;
    args[_thread_num].n_req_threads = _n_req_threads;
    pthread_create(&rq_threads[_thread_num], NULL, request_thread_func, (void*) &args[_thread_num]);
}

void create_event_handler(std::unordered_map<int, RequestChannel*>* _rq_chans, fd_set* _readset, int _maxfd, PCBuffer* _PCB, 
                            pthread_t* eh_thread, EHargs* args) {
    args->rq_chans = _rq_chans;
    args->readset = _readset;
    args->maxfd = _maxfd;
    args->PCB = _PCB;
    pthread_create(eh_thread, NULL, event_handler_func, (void *) args);
}

/*--------------------------------------------------------------------------*/
/* MAIN FUNCTION */
/*--------------------------------------------------------------------------*/

int main(int argc, char * argv[]) {
    size_t num_requests = 0;
    size_t pcb_size = 0;
    size_t num_chan = 0;
        
    int opt;

    while((opt = getopt(argc, argv, ":n:b:w:")) != -1) {
        switch (opt) {
            case 'n':
                sscanf(optarg, "%zu", &num_requests);
                break;
            case 'b':
                sscanf(optarg, "%zu", &pcb_size);
                break;
            case 'w':
                sscanf(optarg, "%zu", &num_chan);
                break;
            case ':':
                std::cout << "Invalid parameters or no parameters passed. Check your input and start again." << std::endl;
                return 1;
            case '?':
                std::cout << "Unknown argument" << std::endl;
                return 1;
        }
    }

    if (num_requests == 0 || pcb_size == 0 || num_chan == 0) {
        std::cout << "Invalid parameters or no parameters passed. Check your input and start again." << std::endl;
        exit(1);
    } 

    /* We have valid parameters so we can begin the client & server */
    if (fork() == 0){ 
        execve("dataserver", NULL, NULL);
    } else {
        std::cout << "CLIENT STARTED:" << std::endl;
        std::cout << "Establishing control channel... " << std::flush;
        RequestChannel chan("control", RequestChannel::Side::CLIENT);
        std::cout << "done." << std::endl;

        std::cout << "Creating PCBuffer..." << std::endl;
        PCBuffer PCB(pcb_size);
        std::cout << "done." << std::endl;

        /* We create an array of threads to keep track of their thread ids, the statistics threads in particular. */
        pthread_t* rq_threads = new pthread_t[NUM_PATIENTS];
        pthread_t* event_thrd = new pthread_t();
        
        /* We allocate an array of arguments for the threads here so that we can delete them later when the program is finishing up. */
        RTFargs* rtfargs = new RTFargs[NUM_PATIENTS];
        EHargs* ehargs = new EHargs();

        std::unordered_map<int, RequestChannel*> rq_chans;

        /* We will pass the memory addresses of these size_t's into the arguments; they will be shared across their respective threads */
        size_t n_req_threads = NUM_PATIENTS;

        std::cout << "Creating request threads..." << std::endl;
        for (size_t i = 0; i < NUM_PATIENTS; i++) {
            // This is just to give them some sort of name.
            std::string patient_name = "Patient " + std::to_string(i + 1);

            create_requester(i, num_requests, patient_name, &PCB, rq_threads, &n_req_threads, rtfargs);
        }
        std::cout << "done." << std::endl;

        // Creating the readset of FDs
        fd_set readset;

        // Zeroing the fdset
        FD_ZERO(&readset);

        int maxfd = 0;

        // Here we "prime the pump"
        for (size_t i = 0; i < num_chan; i++) {
            std::string reply = chan.send_request("newthread");
            std::cout << "Reply to request 'newthread' is " << reply << std::endl;
            std::cout << "Establishing new control channel... " << std::flush;
            RequestChannel* rc = new RequestChannel(reply, RequestChannel::Side::CLIENT);
            // Getting the read file decriptor to determine the max
            int rfd = rc->read_fd();
            if (rfd >= maxfd)
                maxfd = rfd;
            // Adding this fd to the fdset
            FD_SET(rfd, &readset);
            rq_chans[rfd] = rc;
            std::cout << "done." << std::endl;
            std::cout << "Writing first request to pipe..." << std::endl;
            rq_chans[rfd]->cwrite(PCB.Retrieve());
        }

        // Creating the event handler
        create_event_handler(&rq_chans, &readset, maxfd, &PCB, event_thrd, ehargs);
        pthread_join(*event_thrd, NULL);


        std::cout << "Closing control request channel..." << std::endl;
        std::string fin_reply = chan.send_request("quit");
        std::cout << "Reply from control channel read: " << fin_reply << std::endl;
        std::cout << "done." << std::endl;

        std::cout << "Clearing the heap..." << std::endl;

        /* We call detach on the other threads so that memory can be freed, and there will be no memory leaks */
        for (size_t i = 0; i < NUM_PATIENTS; i++)
            pthread_detach(rq_threads[i]);

        for (auto i : rq_chans)
            delete i.second;
            
        delete[] rq_threads;
        delete event_thrd;
        delete ehargs;
        delete[] rtfargs;
        std::cout << "Client stopped successfully." << std::endl;
    }

    usleep(1000000);
    exit(0);
}
