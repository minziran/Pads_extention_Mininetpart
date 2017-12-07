#!/usr/bin/python
#  
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Course: CS4287-5287 Principles of Cloud Computing
# Created: Fall 2016
#
# Purpose: Simple MapReduce Wordcount
#

# Note that this is standalone main program. Although we can invoke it via mininet,
# you can start it manually from an xterm that you can start on the master node
# from the mininet CLI

# system and time
import os
import sys
import time
import argparse   # argument parser
import re          # regular expression

from mr_framework import MR_Framework # our wordcount MR framework

# @NOTE@:  Clearly, this file is specifically for wordcount and so you will
#                          need to do things differently for Assignment #4 in the
#                          mr_framework.py file. However, this file may not need any
#                          change other than just renaming it (since it is not wordcount
#                          for your assignment)


##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-m", "--map", type=int, default=10, help="Number of Map jobs, default 10")
    parser.add_argument ("-r", "--reduce", type=int, default=3, help="Number of Reduce jobs, default 3")
    parser.add_argument ("-p", "--masterport", type=int, default=5557, help="Master node port, default 5557")
    
    # add positional arguments in that order
    # parser.add_argument ("addrfile", help="File of host ip addresses")
    parser.add_argument ("datafile", help="Big data file")

    # parse the args
    args = parser.parse_args ()

    return args
    
#------------------------------------------
# main function
def main ():
    """ Main program """

    print "MapReduce Wordcount Main program"
    parsed_args = parseCmdLineArgs ()
    
    # now invoke the mapreduce framework. Notice we have slightly changed the way the
    # constructor works and the arguments it takes. 
    mrf = MR_Framework (parsed_args)

    # invoke the process
    mrf.solve ()

#----------------------------------------------
if __name__ == '__main__':
    main ()
