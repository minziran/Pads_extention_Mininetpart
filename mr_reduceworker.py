#!/usr/bin/python
#
# Vanderbilt University, Computer Science
# CS4287-5287: Principles of Cloud Computing
# Author: Aniruddha Gokhale
# Created: Nov 2016
# 
#
# Purpose:
# This code runs the wordcount map task. It runs inside the worker process. Since the
# worker gets commands from a master and sends result back to the master, we use
# ZeroMQ as a way to get this communication part done.

import os
import sys
import time
import re
import zmq
import pickle

import argparse   # argument parser

# @NOTE@: You will need to make appropriate changes
#                          to this logic.  You can maintain the overall structure
#                          but the logic of the reduce function has to change to
#                           suit the needs of the assignment
#
#                          I do not think you need to change the class variables
#                          but you may need additional ones. The key change
#                          will be in do_work

# ------------------------------------------------
# Main reduce worker class        
class MR_Reduce ():
    """ The map worker class """
    def __init__ (self, args):
        """ constructor """
        self.id = args.id
        self.master_ip = args.masterip
        self.master_port = args.masterport
        self.receiver = None  # connection to master
        self.sender = None    # connection to reduce barrier

    #------------------------------------------
    def init_worker (self):
        """ Word count reduce worker initialization """
        print "initializing reduce worker with id: ", self.id

        context = zmq.Context()

        # Socket to receive messages on. Worker uses PULL from the master
        # Note that the reducer uses 1 more than the baseline port of master
        self.receiver = context.socket (zmq.PULL)
        connect_addr = "tcp://"+ self.master_ip + ":" + str (self.master_port+1)
        print "Using PULL, reduce worker connecting to ", connect_addr
        self.receiver.connect (connect_addr)
        
        # Socket to send messages to. In our case, the reduce worker will push an event
        # to the reduce barrier indicating one of two things.
        # First, it tells that it is up and running.
        # Second, it tells it has completed the reduce task.

        # Note that the port number of the reduce barrier is 3 more than the
        # port of the master. This is our internal 
        self.sender = context.socket (zmq.PUSH)
        connect_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+3)

        print "Using PUSH, reduce worker connecting to barrier at ", connect_addr
        self.sender.connect (connect_addr)

        # now send an ACK to the barrier to let it know that we are up
        self.sender.send (b'0')

    #------------------------------------------
    def do_work (self):
        """ Word count reduce function """
        print "starting work: reduce worker with id: ", self.id,  " in directory: ", os.getcwd()

        # In our case, we do only one task and our job in life is done :-)
        # Wait for that nudge
        s = self.receiver.recv ()
        print "reducer received a nudge to start"
        
        # Open the corresponding shuffle file for reading
        shufflefile = open ("Shuffle"+str(self.id)+".dat", "rb")

        # retrieve the contents of the shuffle file using pickle
        contents = pickle.load (shufflefile)

        # close the shuffle file
        shufflefile.close ()
        
        # Each reduce task saves its results in a file
        reduce_file = open ("Reduce"+str(self.id)+".csv", "wb")

        # our contents will be a list of list. Each internal list could have
        # one or more entries for a given unique word
        for items in contents:
            sum = 0
            for i in range(len(items)):
                sum = sum + items[i][1]

            # The [0]'th entry of each of the entries of the second level
            # list is the unique word. We just use the first one and dump it
            # into our results file.
            reduce_file.write (items[0][0] + ", " + str(sum) + "\n")

        reduce_file.close ()

        # trigger the reduce barrier by sending a dummy byte
        self.sender.send (b'0')

        print "reduce worker with name: ", self.id, " exiting"

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add positional arguments in that order
    parser.add_argument ("id", type=int, help="worker number")
    parser.add_argument ("masterip", help="IP addr of master")
    parser.add_argument ("masterport", type=int, help="Port number of master")

    # parse the args
    args = parser.parse_args ()

    return args
    
#---------------------------------------------------------------------------------------------------------------
# main function
def main ():
    """ Main program for Reduce worker """

    print "MapReduce Reduce Worker program"
    parsed_args = parseCmdLineArgs ()
    
    # invoke the reducer logic
    reduceobj = MR_Reduce (parsed_args)

    # initialize the reduce worker network connections
    reduceobj.init_worker ()
    
    # invoke the reduce process
    reduceobj.do_work ()

#----------------------------------------------
if __name__ == '__main__':
    main ()
