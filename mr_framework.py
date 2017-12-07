#!/usr/bin/python
#  
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Course: CS4287-5287 Principles of Cloud Computing
# Created: Fall 2016
#
# Purpose: MapReduce Framework for Wordcount (not a generic framework)
#

# system and time
import os
import sys
import time
import re                    # regular expression
import csv                  # deal with CSV files
import operator     # used in itertools
import itertools    # nice iterators

import zmq               # ZeroMQ library
import json               # json
import pickle           # serialization

import subprocess as sp

from mr_thread import MR_Thread  # our Map Reduce threading class

#--------------------------------------------------------------------------------------------------------
# barrier thread for map and reduce phases
def barrier_sink (args):
    "map or reduce phase barrier"

    try:
        # here we start the ZeroMQ sink task. When the required number of
        # map or reduce responses are received, the thread terminates.
        print "Barrier sink thread starting with args: ", args

        context = zmq.Context ().instance ()

        # Socket to receive messages on. Note that the sink uses the PULL pattern
        receiver = context.socket (zmq.PULL)
        bind_addr = "tcp://*:" + str (args['port']) 
        print "Using PULL, barrier is binding to ", bind_addr
        receiver.bind (bind_addr)   # use the port from the arg

        # The logic here is that we wait until the required number of
        # ACKs are received 
        i = 0
        while (i < args['cond']):
            # wait until all responses received from workers
            s = receiver.recv ()
            print "Barrier received an ACK"
            i = i + 1

        print "Barrier received required number of ACKS = ", args['cond']
        return
    
    except:
        print "Unexpected error in barrier thread: ", sys.exc_info()[0]

# ------------------------------------------------
# Main map reduce class        
class MR_Framework ():
    """ The map reduce orchestrator class """
    def __init__ (self, args):
        self.M = args.map                     # num of map jobs
        self.R = args.reduce                        # num of reduce jobs
        self.masterport = args.masterport  # port on which we push to workers
        self.datafile = args.datafile # name of the big data file
        self.uniquekeys = []       # num of unique keys
        self.groups = []              # num of groups per unique key

        self.map_sender = None   # used to send push messages
        self.reduce_sender = None   # used to send push messages
        self.threads = {'map': None, 'reduce': None}  # for the map and reduce barrier threads
        
    # ------------------------------------------------------------------------------------------------------
    # Initialize the network connections and the barriers
    def init_server (self):
        """Initialize the networking part of the server"""

        try:
            # obtain the ZeroMQ context
            context = zmq.Context().instance ()

            # Socket to send messages on. Note, we use the PUSH pattern
            # Note that we create two such PUSH-PULL workflows:
            # one for master to map, and other for master to reduce
            #
            # It had to be done this way due to the choice of comm pattern we
            # made in Zero MQ.
            self.map_sender = context.socket (zmq.PUSH)
            bind_addr = "tcp://*:" + str (self.masterport)
            print "For master->map PUSH, bind addr is: ", bind_addr
            self.map_sender.bind  (bind_addr)

            self.reduce_sender = context.socket (zmq.PUSH)
            bind_addr = "tcp://*:" + str (self.masterport+1)
            print "For master->reduce PUSH, bind addr is: ", bind_addr
            self.reduce_sender.bind  (bind_addr)

        except:
            print "Unexpected error in init_server:", sys.exc_info()[0]
            raise

    # ------------------------------------------------------------------------------------------------------
    # Create the barrier sink as a separate process (either for map or reduce)
    def start_barrier_sink (self, op):
        """Initialize the networking part of the server including sinks"""

        try:
            # The process-based approach of creating a child barrier process
            # is working fine. But we will use thread-based as it is easier

            #--------------- this part works fine ----------------------
            # create the apparatus to start a child process
            #operation = os.getcwd () + "/mr_barriersink.py"
            #args = ['/usr/bin/python', operation, '-p', str(self.masterport+1), barrier_cond]
            #print "MR::start_barrier_sink invoking child with args: ", args
            # start the process
            #p = sp.Popen (args)
            #-------------------------------------------------------------------

            # Note that barriers start at base port of master + 2 and +3
            # for map and reduce barriers, respectively.
            if (op == "map"):
                barrier_cond = self.M
                port = self.masterport + 2
            elif (op == "reduce"):
                barrier_cond = self.R
                port = self.masterport + 3
            else:
                print "bad op in starting barrier process"
                return None

            # create the args to send to the thread
            args = {'port': port, 'cond': barrier_cond}
            
            # instantiate a thread obj and start the thread
            print "MR::solve - start_barrier_sink with args: ", args
            self.threads[op]  = MR_Thread (barrier_sink, args)
            self.threads[op].start ()

        except:
            print "Unexpected error in init_server:", sys.exc_info()[0]
            raise

    # --------------------------------------------------------------------------------------------------------
    #
    # @NOTE@: changes will be needed here for assignment #4
    #
    def distribute_map_tasks (self):
        "create the json object to be sent to all map tasks"

        # find the file size and break it into (almost) equal sized chunks
        #
        # The way we are going to do this is that we will let the workers know
        # the start byte and the num of bytes each map task must read from
        # the original file.  To that end we first create a json obj that we are going to
        # send to the workers so that each worker can process it

        try:
            # initialize the argument data structure that we plan to send to our workers.
            # The data structure we are passing to the workers comprises the name of the
            # datafile, the starting byte from the file to read and the number of bytes to read.

            # compute the size of the datafile and create (almost) equal sized chunks
            doc_size = os.path.getsize (self.datafile)
            chunk_size = int (round (doc_size/self.M))  # integer division
            print "doc size = ", doc_size, ", chunk size = ", chunk_size

            locn2read = 0  # the starting location of the next chunk to read, initialized to 0 (start)
            
            # Note that we are splitting the file along bytes so it is
            # very much possible that a valid word may get split into
            # nonsensical two words but we don't care here and will treat
            # these two split parts of a word as separate unique words
            for i in range (self.M):
                # get the starting locn and size of the next chunk
                if (i == self.M-1): # if this is the last chunk
                    chunk_size = doc_size - locn2read

                # create the argument to send to the task
                map_arg = {'datafile': self.datafile, 'start': locn2read, 'size': chunk_size}

                # now send this and one of the map tasks will receive it
                # according to the PUSH-PULL pattern we are using
                print "MR::solve - master sending args to map: ", map_arg
                self.map_sender.send_json (map_arg)

                # move the starting position that many bytes ahead
                locn2read += chunk_size

        except:
            print "Unexpected error in distribute_map_tasks:", sys.exc_info()[0]
            raise

    # --------------------------------------------------------------------------------------------------------
    #
    # @NOTE@: changes will be needed here for assignment #4
    #
    def distribute_reduce_tasks (self):
        "create the json object to be sent to all reduce tasks"

        # here we use the Shuffle*.csv file from the Shuffle phase and 
        # let each reduce worker pick their corresponding files.
        # In our case we do not send any specific message to the reduce
        # workers but just a short nudge so they can go ahead and
        # pick their respective shuffle files.
        try:
            for i in range (self.R):
                # send a short nudge to the reducer task for it to start
                self.reduce_sender.send (b'0')

        except:
            print "Unexpected error in distribute_reduce_tasks:", sys.exc_info()[0]
            raise

    # -------------------------------------------------------------------------------------------------------
    #
    # @NOTE@: changes will be needed here for assignment #4
    #
    # shuffle function. We are assured that there are no
    # stragglers because we use the barrier synchronization and hence
    # only after all maps are done, we start shuffle.
    def shuffle_func (self):
        """ Word count shuffle function """

        print "MR::solve - Shuffle phase"

        # Potentially we could have done all the reduction here itself
        # given all files are in the same directory (though on diff hosts).
        # But we will not do it that way so as to remain true to the spirit of
        # MapReduce (as much as we can)
        
        # Now create a temp file with combiner optimizations
        tempfile = open ("temp.csv", "w")
        
        # for each csv file generated in the map phase, we sort the keys.
        # Note that the map csv files are numbered Map0.csv, Map1.csv, ...
        for i in range (self.M):
            # open the CVS file created by map job
            csvfile = csv.reader (open ("Map"+str(i)+".csv", "r"), delimiter=",")
            
            # get the sorted list of entries from our csv file using
            # column 1 (0-based indexing) as the key to sort on
            # and we use traditional alphabetic order
            wordlist = sorted (csvfile, key=operator.itemgetter (0))

            # Now group all entries by the unique identified words and perform
            # local combiner optimization (because it is addition, which is commutative
            # and associative). Note, again that we do not do all the reduction here
            # itself but leave it to the reduce tasks.

            groups = []
            uniquekeys = []
            for k, g in itertools.groupby (wordlist, key=operator.itemgetter (0)):
                groups.append (list (g))
                uniquekeys.append (k)

            for j in range (len (uniquekeys)):
                tempfile.write (uniquekeys[j] + ", ")
                # note that for each unique word, we have a grouped
                # list, and each value is 1. So the length of the
                # sublist is the sum, which represents the combiner
                # optimization reduction for that unique key.
                # We save that info
                tempfile.write (str (len (groups[j])) + "\n")

            # Delete the intermediate Map file (no need for it anymore)
            os.remove ("Map"+str(i)+".csv")

        # close the temp file
        tempfile.close ()

        # Now group all the entries in sorted order so that we can hand them
        # out to reduce tasks. For that, open the temp CSV file we just
        # created
        csvfile = csv.reader (open ("temp.csv", "rb"), delimiter=",")

        # We need this because otherwise the csv treats everything
        # as text and so our numbers are getting converted to strings
        rows = [[row[0], int(row[1])] for row in csvfile]
        
        # get the sorted list of entries from our csv file using
        # column 1 (0-based indexing used here) as the key to sort on
        # and we use traditional alphabetic order.  
        wordlist = sorted (rows, key=operator.itemgetter (0))
        
        # Identify the total number of unique keys we have in our data
        # We need this info to make a split in the Shuffle file to hand out
        # to reduce tasks. We create as many intermediate shuffle files
        # as the number of readers we have
        for k, g in itertools.groupby (wordlist, key=operator.itemgetter (0)):
            self.groups.append (list (g))
            self.uniquekeys.append (k)

        print "MR::shuffle - Total unique keys = ", len (self.uniquekeys)

        # now save the shuffle info into as many files as the number
        # of reduce jobs
        
        keysPerReduce = int (round (len(self.uniquekeys)/self.R))  # integer division
        keysConsumed = 0  # initial condition

        for i in range (self.R):
            # we open the file with binary write property since
            # we are going to write an in-memory representation
            # of the lists of lists we have stored in self.groups
            shufflefile = open ("Shuffle" + str (i) + ".dat", "wb")

            # get the starting locn and num of entries to be stored
            # in this shuffle file.
            if (i == self.R-1): # if this is the last reduce task
                    keysPerReduce = len(self.uniquekeys) - keysConsumed

            print "keysPerReduce this iteration = ", keysPerReduce
            # use pickle to store binary representation.
            pickle.dump (self.groups[keysConsumed:keysConsumed+keysPerReduce], shufflefile, pickle.HIGHEST_PROTOCOL)
            # update the starting locn for the next shuffle file.
            keysConsumed = keysConsumed + keysPerReduce
            print "keys consumed end of this iteration = ", keysConsumed
            # close this shuffle file
            shufflefile.close ()

        # cleanup the temp file
        os.remove ("temp.csv")
        
    # ------------------------------------------------
    #
    # @NOTE@: changes will be needed here for assignment #4
    #
    # finalize function. We are assured that there are no
    # remaining reduce jobs because we use the barrier synchronization
    def finalize_func (self):
        """ Word count finalize function """
        print "Finalize phase: Aggregate all the results"

        # effectively, we go thru all the reduce results files and
        # get the results
        results = open ("results.csv", "w")
        for i in range (self.R):
            reduce_file = open ("Reduce"+str(i)+".csv", "rb")
            data = reduce_file.read ()
            reduce_file.close ()
            results.write (data)
            
        results.close ()

        # cleanup. Delete all reducer related files
        for i in range (self.R):
            os.remove ("Shuffle"+str(i)+".dat")
            os.remove ("Reduce"+str(i)+".csv")

    # ------------------------------------------------------------------------------------------------------
    #
    # @NOTE@: I do not think any change is needed here for assignment #4
    #
    
    # The method that solves the problem using the map reduce approach
    def solve (self):
        """Solve the problem using map reduce"""

        try:

            total_running_time = 0
            
            ############ initialization ##################

            start_time = time.time ()

            # here we start our end of the PUSH based workflow
            print "MR::solve - initialize server"
            self.init_server ()
            
            # start a barrier to make sure the required number of map workers
            # are up and running
            print "MR::solve - start map barrier"
            #handle = self.start_barrier_sink ("map")
            self.start_barrier_sink ("map")

            # Do the same for reduce workers
            print "MR::solve - start reduce barrier"
            self.start_barrier_sink ("reduce")
            
            # the join results in a barrier synchronization. At this point
            # make sure all map and reduce workers are up and running
            print "MR::solve - wait for workers to start"
            for thr in self.threads.values ():
                thr.join ()

            print "MR::solve - All map and reduce workers are up"
            
            end_time = time.time ()

            print "***** Initialization phase required: ", (end_time-start_time), " seconds"
            total_running_time = total_running_time + (end_time-start_time)

            ########### Phase 1: Map ###################
            start_time = time.time ()

            # start the map barrier sink again to receive the ACKs after
            # each map worker has completed its work.
            print "MR::solve - start map barrier"
            self.start_barrier_sink ("map")
            
            ### distribute tasks to all map workers ###
            print "MR::solve - send message to workers"
            self.distribute_map_tasks ()
            
            ### barrier ###
            print "MR::solve - wait for map sink to return"
            self.threads["map"].join ()

            # this part works if we use the process approach
            #retcode = handle.wait ()
            #print "MR::solve - barrier returned with status: ", retcode
            
            end_time = time.time ()

            print "***** Map phase required: ", (end_time-start_time), " seconds"
            total_running_time = total_running_time + (end_time-start_time)

            ########### Phase 2: Shuffle ###################
            # this is not done in parallel for our case. In reality shuffle will do the necessary
            # things in parallel and move things around so that the reduce job can fetch the
             # right things from the right place. We do not have any such elaborate
            # mechanism.
            start_time = time.time ()

            self.shuffle_func ()
            
            end_time = time.time ()

            print "***** Shuffle phase required: ", (end_time-start_time), " seconds"
            total_running_time = total_running_time + (end_time-start_time)

            ########### Phase 3: Reduce ###################
            start_time = time.time ()

            # start the reduce barrier sink again to receive the ACKs after
            # each reduce worker has completed its work.
            print "MR::solve - start reduce barrier"
            self.start_barrier_sink ("reduce")
            
            ### distribute tasks to all workers ###
            print "MR::solve - send message to reduce workers"
            self.distribute_reduce_tasks ()
            
            ### barrier ###
            print "MR::solve - wait for reduce sink to return"
            self.threads["reduce"].join ()

            # this part works if we use the process approach
            #retcode = handle.wait ()
            #print "MR::solve - barrier returned with status: ", retcode
            
            end_time = time.time ()

            print "***** Reduce phase required: ", (end_time-start_time), " seconds"
            total_running_time = total_running_time + (end_time-start_time)

            ########### Phase 4: Finalize ###################
            start_time = time.time ()
            self.finalize_func ()
            end_time = time.time ()

            print "***** Finalize phase required: ", (end_time-start_time), " seconds"
            total_running_time = total_running_time + (end_time-start_time)

            print "*** Total Running Time ***  = ",  total_running_time
        except:
            print "Unexpected error in solve method:", sys.exc_info()[0]
            raise

