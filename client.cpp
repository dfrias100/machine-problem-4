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

#include <errno.h>
#include <unistd.h>
#include <pthread.h>

#include "reqchannel.hpp"
#include "pcbuffer.hpp"

/*--------------------------------------------------------------------------*/
/* DATA STRUCTURES */ 
/*--------------------------------------------------------------------------*/

typedef struct {
    /* Histogram bins
        0 - 9
        10 - 19
        20 - 29
        30 - 39
        40 - 49
        50 - 59
        60 - 69
        70 - 79
        80 - 89
        90 - 99 */
    PCBuffer* PatientDataBuffer;
    std::vector<int> histogram; 
} PatientHistogram;

typedef struct {
    size_t* n_req_threads;
    size_t n_req;
    PCBuffer* PCB;
    std::string patient_name;
} RTFargs;

typedef struct {
    size_t* n_wkr_threads;
    PCBuffer* PCB;
    RequestChannel* rc;
    std::unordered_map<std::string, PatientHistogram>* PatientData;
} WTFargs;

typedef struct {
    std::unordered_map<std::string, PatientHistogram>* PatientData;
    std::string patient_name;
} STFargs;

Semaphore n_req_thread_count_mutex(1);
Semaphore n_wkr_thread_count_mutex(1);
Semaphore histogram_print_sync(1);

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

void print_histogram(std::vector<int> histogram) {
    size_t total_data_points = 0;
    size_t scale = 1;

    /* This is needed so that we can see if we need to scale the data down or not */
    for (size_t i = 0; i < 10; i++)
        total_data_points += histogram[i];

    std::cout << "Size of data: " << total_data_points << std::endl;

    for(int i = 0; i < 10; i++) {
        std::cout << std::setw(2) << std::setfill('0') << i * 10 << "-" << std::setw(2) << std::setfill('0') << i * 10 + 9 << ": ";
        std::cout << histogram[i] << std::endl;
    }
}

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

void* worker_thread_func(void* wtfargs) {
    WTFargs* args = (WTFargs*) wtfargs;
    size_t* n_wkr_threads = args->n_wkr_threads;
    for(;;) {
        std::string req = args->PCB->Retrieve();
        
        // The number of threads active is a shared variable, so we need to synchronize the access to the variable.
        if (req.compare("done") == 0) {
            std::cout << "Worker thread read 'done' from PCBuffer" << std::endl;
            args->rc->send_request("quit");
            args->PCB->Deposit("done");
            n_wkr_thread_count_mutex.P();
            if (*n_wkr_threads != 1) {
                *n_wkr_threads = *n_wkr_threads - 1;
            } else {
                /* We have to iterate this way because it is an unordered (hash) map. 
                   Only the last worker thread can send the 'done' message to the statistics
                   threads. */
                for (auto i : *args->PatientData) {
                    i.second.PatientDataBuffer->Deposit("done");
                }
                *n_wkr_threads = *n_wkr_threads - 1;
            }
            std::cout << "Worker thread finished." << std::endl;
            n_wkr_thread_count_mutex.V();
            break;
        }

        std::cout << "New request: " << req << std::endl;
        std::string reply = args->rc->send_request(req);
        std::cout << "Out from PCBuffer: " << req << std::endl;
        std::cout << "Reply to request '" << req << "': " << reply << std::endl;

        std::string name = req.substr(5, req.length() - 1);
        PCBuffer* patient_buff = (*args->PatientData)[name].PatientDataBuffer;
        patient_buff->Deposit(reply);
    }

    return nullptr;
}

void* stats_thread_func(void* args) {
    STFargs* stfargs = (STFargs *) args;
    std::unordered_map<std::string, PatientHistogram>* PatientData = stfargs->PatientData;
    std::string patient_name = stfargs->patient_name;
    /* This makes it so that we only have to access the hashmap once, and it makes the rest of the code look nicer. */
    PatientHistogram* patient_histogram = &PatientData->find(patient_name)->second;
    for (;;) {
        std::string req = patient_histogram->PatientDataBuffer->Retrieve();
        if (req.compare("done") == 0) {
            /* We synchronize the output of the histogram so that one statistic thread doesn't print over the other threads */
            histogram_print_sync.P();
            std::cout << "Statistic thread read 'done' from PCBuffer" << std::endl;
            std::string to_print = "--------HISTOGRAM FOR " + patient_name + "--------";
            std::cout << to_print << std::endl;
            print_histogram(patient_histogram->histogram);
            /* We delete the PCBuffer since it is allocated on the heap. */
            delete patient_histogram->PatientDataBuffer;
            to_print = std::string(to_print.size(), '-');
            std::cout << to_print << std::endl;
            histogram_print_sync.V();
            break;
        } else {
            size_t data = stoi(req);
            if (data <= 9) {
                patient_histogram->histogram[0]++;
            } else if (data <= 19) {
                patient_histogram->histogram[1]++;
            } else if (data <= 29) {
                patient_histogram->histogram[2]++;
            } else if (data <= 39) {
                patient_histogram->histogram[3]++;
            } else if (data <= 49) {
                patient_histogram->histogram[4]++;
            } else if (data <= 59) {
                patient_histogram->histogram[5]++;
            } else if (data <= 69) {
                patient_histogram->histogram[6]++;
            } else if (data <= 79) {
                patient_histogram->histogram[7]++;
            } else if (data <= 89) {
                patient_histogram->histogram[8]++;
            } else {
                patient_histogram->histogram[9]++;
            }
        }
    }

    return nullptr;
}

/* These functions prepare the arguments and then creates the thread; The thread is linked to their arguments with the
   the thread index that it takes in, along with the other required objects needed to create the thread */

void create_worker(int _thread_num, RequestChannel* _rc, PCBuffer* _PCB, size_t* n_wkr_thread_count, 
                    std::unordered_map<std::string, PatientHistogram>* patient_data,
                    pthread_t* wk_threads, WTFargs* args) {
    args[_thread_num].PCB = _PCB;
    args[_thread_num].rc = _rc;
    args[_thread_num].PatientData = patient_data;
    args[_thread_num].n_wkr_threads = n_wkr_thread_count;
    pthread_create(&wk_threads[_thread_num], NULL, worker_thread_func, (void*) &args[_thread_num]);
}

void create_requester(int _thread_num, int _num_requests, std::string _patient_name, PCBuffer* _PCB, 
                        pthread_t* rq_threads, size_t* _n_req_threads, RTFargs* args) {
    args[_thread_num].n_req = _num_requests;
    args[_thread_num].patient_name = _patient_name;
    args[_thread_num].PCB = _PCB;
    args[_thread_num].n_req_threads = _n_req_threads;
    pthread_create(&rq_threads[_thread_num], NULL, request_thread_func, (void*) &args[_thread_num]);
}

void create_stats(int _thread_num, std::string _patient_name, std::unordered_map<std::string, PatientHistogram>* patient_data, 
                    pthread_t* st_threads, STFargs* args) {
    args[_thread_num].PatientData = patient_data;
    args[_thread_num].patient_name = _patient_name;
    pthread_create(&st_threads[_thread_num], NULL, stats_thread_func, (void*) &args[_thread_num]);
}

/*--------------------------------------------------------------------------*/
/* MAIN FUNCTION */
/*--------------------------------------------------------------------------*/

int main(int argc, char * argv[]) {
    size_t num_requests = 0;
    size_t pcb_size = 0;
    size_t num_threads = 0;
        
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
                sscanf(optarg, "%zu", &num_threads);
                break;
            case ':':
                std::cout << "Invalid parameters or no parameters passed. Check your input and start again." << std::endl;
                return 1;
            case '?':
                std::cout << "Unknown argument" << std::endl;
                return 1;
        }
    }

    if (num_requests == 0 || pcb_size == 0 || num_threads == 0) {
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

        /* We use a hashmap so we can use the patient's name as a key to access the relevant data */
        std::cout << "Creating hash map..." << std::endl;
        std::unordered_map<std::string, PatientHistogram> patient_data;
        std::cout << "done." << std::endl;

        std::cout << "Creating PCBuffer..." << std::endl;
        PCBuffer PCB(pcb_size);
        std::cout << "done." << std::endl;

        /* We create an array of threads to keep track of their thread ids, the statistics threads in particular. */
        pthread_t* rq_threads = new pthread_t[NUM_PATIENTS];
        pthread_t* st_threads = new pthread_t[NUM_PATIENTS];
        pthread_t* wk_threads = new pthread_t[num_threads];
        
        /* We allocate an array of arguments for the threads here so that we can delete them later when the program is finishing up. */
        WTFargs* wtfargs = new WTFargs[num_threads];
        RTFargs* rtfargs = new RTFargs[NUM_PATIENTS];
        STFargs* stfargs = new STFargs[NUM_PATIENTS];

        /* We will pass the memory addresses of these size_t's into the arguments; they will be shared across their respective threads */
        size_t n_req_threads = NUM_PATIENTS;
        size_t n_wkr_threads = num_threads;

        std::cout << "Creating request threads..." << std::endl;
        for (size_t i = 0; i < NUM_PATIENTS; i++) {
            // This is just to give them some sort of name.
            std::string patient_name = "Patient " + std::to_string(i + 1);

            create_requester(i, num_requests, patient_name, &PCB, rq_threads, &n_req_threads, rtfargs);

            // These PCBuffers needs to be allocated on the heap, otherwise it will be destroyed once it leaves scope, even if we pass a reference.
            PCBuffer* stats_buff = new PCBuffer(pcb_size);
            patient_data[patient_name].PatientDataBuffer = stats_buff;
            patient_data[patient_name].histogram = std::vector<int>(10, 0);
            create_stats(i, patient_name, &patient_data, st_threads, stfargs);
        }
        std::cout << "done." << std::endl;

        for (size_t i = 0; i < num_threads; i++) {
            std::string reply = chan.send_request("newthread");
            std::cout << "Reply to request 'newthread' is " << reply << std::endl;
            std::cout << "Establishing new control channel... " << std::flush;
            // These channels need to be allocated on the heap for the same reason as the stats buffers.
            RequestChannel* new_chan = new RequestChannel(reply, RequestChannel::Side::CLIENT);
            std::cout << "done." << std::endl;
            create_worker(i, new_chan, &PCB, &n_wkr_threads, &patient_data, wk_threads, wtfargs);
        }

        /* We need to wait for the termination of the statistics thread AND the last worker thread (see worker_thread_func), otherwise the program
           will terminate prematurely */
        for (size_t i = 0; i < NUM_PATIENTS; i++)
            pthread_join(st_threads[i], NULL);

        std::cout << "Closing control request channel..." << std::endl;
        std::string fin_reply = chan.send_request("quit");
        std::cout << "Reply from control channel read: " << fin_reply << std::endl;
        std::cout << "done." << std::endl;

        std::cout << "Clearing the heap..." << std::endl;

        /* We call detach on the other threads so that memory can be freed, and there will be no memory leaks, additionally we delete the request
           channels here so that we don't have to loop twice. */
        for (size_t i = 0; i < NUM_PATIENTS; i++)
            pthread_detach(rq_threads[i]);
        for (size_t i = 0; i < num_threads; i++) {
            pthread_detach(wk_threads[i]);    
            delete wtfargs[i].rc; // The reason they are deleted here is so that the server threads have a chance to read the quit message, or else it 
                                  // can leave behind some fifo_* files.
        }

        delete[] rq_threads;
        delete[] wk_threads;
        delete[] st_threads;
        delete[] wtfargs;
        delete[] stfargs;
        delete[] rtfargs;
        std::cout << "Client stopped successfully." << std::endl;
    }

    usleep(1000000);
    exit(0);
}
