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
import json

import argparse   # argument parser

# @NOTE@: You will need to make appropriate changes
#                          to this logic.  You can maintain the overall structure
#                          but the logic of the map function has to change to
#                           suit the needs of the assignment
#
#                          I do not think you need to change the class variables
#                          but you may need additional ones. The key change
#                          will be in do_work

# ------------------------------------------------
# Main map worker class        
class MR_Map ():
    """ The map worker class """
    def __init__ (self, args):
        """ constructor """
        self.id = args.id
        self.master_ip = args.masterip
        self.master_port = args.masterport
        self.receiver = None  # connection to master
        self.sender = None    # connection to map barrier

    #------------------------------------------
    def init_worker (self):
        """ Word count map worker initialization """
        print "initializing map worker with id: ", self.id, " in directory: ", os.getcwd ()

        context = zmq.Context()

        # Socket to receive messages on. Worker uses PULL from the master
        self.receiver = context.socket (zmq.PULL)
        connect_addr = "tcp://"+ self.master_ip + ":" + str (self.master_port)
        print "Using PULL, map worker connecting to ", connect_addr
        self.receiver.connect (connect_addr)
        
        # Socket to send messages to. In our case, the map worker will push an event
        # to the map barrier indicating two things.
        # First, it tells that it is up and running.
        # Second, it tells it has completed the map task.

        # Note that the port number of the map barrier is 2 more than the
        # port of the master
        self.sender = context.socket (zmq.PUSH)
        connect_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+2)

        print "Using PUSH, map worker connecting to barrier at ", connect_addr
        self.sender.connect (connect_addr)

        # now send an ACK to the barrier to let it know that we are up
        self.sender.send (b'0')

    #------------------------------------------
    def do_work (self):
        """ Word count map function """
        print "starting work: map worker with id: ", self.id

        # recall that the master broadcasts the map or reduce message via the PUSH.
        # It can be received by both the map and reduce workers. So it is the job of
        # the map and reduce worker to make sure the message was meant for it.
        # Else ignore it.
        
        # In our case, we do only one task and our job in life is done :-)
        json_obj = self.receiver.recv_json()
        print "received message = ", json.dumps(json_obj)
        
        # now parse the json object and do the work
        # We use our id to index into the array of workers in the received message to
        # find the position in the file we want to read from
        datafile = open (json_obj['datafile'],'r')
        datafile.seek (json_obj['start'], 0)
        content = datafile.read (json_obj['size'])
        datafile.close ()
        
        # Each map task saves its intermediate results in a file
        map_file = open ("Map"+str(self.id)+".csv", "w")

        # split the incoming chunk, which is a string. We want the
        # list to be only words and nothing else. So rather than the simple
        # split method of the string class, we use regexp's split

        # We allow apostrophe
        pattern = "(\s|~|`|\!|@|\#|\$|%|\^|&|\*|\(|\)|-|_|\+|=|\{|\}|\[|\]|\||\||:|;|\"|<|>|\,|\.|\?|\/)+"
        split_arg = re.split (pattern, content)

        # create a reg expression pattern against which we are going
        # to match against. We allow a word with apostrophe
        pattern = re.compile ("([A-Za-z]+)('[A-Za-z])?")

        # For every element in the split, if it belongs to a sensical
        # word, emit it as an intermediate key with its count
        for token in split_arg:
            # now check if it is a valid word
            if pattern.match (token):
                map_file.write (token + ", 1\n")

        # close the file
        map_file.close ()

        # trigger the map barrier by sending a dummy byte
        self.sender.send (b'0')

        print "map worker with ID: ", self.id, " exiting"

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
    """ Main program for Map worker """

    print "MapReduce Map Worker program"
    parsed_args = parseCmdLineArgs ()
    
    # now invoke the mapreduce framework. Notice we have slightly changed the way the
    # constructor works and the arguments it takes. 
    mapobj = MR_Map (parsed_args)

    # this is a hack for the purposes of coordination. We need to have the servers 
    # ready for us to connect.  So sleep for a few secs to make sure the push and sink
    # servers are up.
    time.sleep (2)
    
    # initialize the map worker network connections
    mapobj.init_worker ()
    
    # invoke the map process
    mapobj.do_work ()

#----------------------------------------------
if __name__ == '__main__':
    main ()
