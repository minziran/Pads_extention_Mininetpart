from topology import runMultiLink, runApps
from mininet.log import setLogLevel
from mininet.net import CLI
from mininet.net import Mininet
from mininet.topo import Topo
from mininet.link import TCLink, TCIntf, Link
from time import sleep
import json
import argparse
import os
import sys


def parseCmdLineArgs():

    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--masterport", type=int, default=5557, help="Wordcount master port number, default 5557")
    parser.add_argument("-r", "--racks", type=int, choices=[1, 2, 3], default=1,
                        help="Number of racks, choices 1, 2 or 3")
    parser.add_argument("-M", "--map", type=int, default=1, help="Number of Map jobs, default 10")
    parser.add_argument("-R", "--reduce", type=int, default=1, help="Number of Reduce jobs, default 3")
    parser.add_argument("-j","--json",type=str, default="FSM.json", help="Json file of FSM" )

    parser.add_argument("datafile", help="Big data file")

    args = parser.parse_args()

    return args


def json_parser(model,data):
    # with open('FSM (13).json') as f:
    #     data = json.load(f)
    #     linkcoll = []
    linkcoll =[]
    for object in data['Guards']:
        if object['src'] == data['InitialState']['name']:
            key = object['timer']['Value']
            for i in range(0, len(data['InitialState']['link'])):
                slink = data['InitialState']['link'][i]
                linkcoll.append(
                    [str(slink['name']), str(slink['Src_node']), str(slink['Dst_node']), int(slink['Bandwidth_Mbps']), int(slink['Latency_ms']),
                     int(slink['Loss'])])
                model.append([key, linkcoll])
            linkcoll = []
            next_state = object['dst']

    for state in data['Intermediate_States']:
        if state['name'] == next_state:
            for object in data['Guards']:
                if object['src'] == state['name']:
                    key = object['timer']['Value']
                    for i in range(0, len(state['link'])):
                        slink = state['link'][i]
                        linkcoll.append(
                            [str(slink['name']), str(slink['Src_node']), str(slink['Dst_node']),
                             int(slink['Bandwidth_Mbps']), int(slink['Latency_ms']),
                             int(slink['Loss'])])
                    model.append([key, linkcoll])
                    linkcoll = []
                    next_state = object['dst']


    for i in range(0, len(data['FinalState']['link'])):
        slink = data['FinalState']['link'][i]
        linkcoll.append(
            [str(slink['name']), str(slink['Src_node']), str(slink['Dst_node']),
             int(slink['Bandwidth_Mbps']), int(slink['Latency_ms']),
             int(slink['Loss'])])
    model.append([key, linkcoll])


def link_update(example,model,parsed_args):
    time = 0
    print "Activate network"
    example.net.start()
    print "Testing network connectivity"
    example.net.pingAll()

    # example.link_dic[example.h1,example.MRSwitch1].intf1.config(bw=0)
    # example.link_dic[example.h2, example.MRSwitch1].intf1.config(bw=0)
    # example.link_dic[example.h3, example.MRSwitch1].intf1.config(bw=0)
    # genCommandsFile(example.net.hosts, parsed_args)
    # CLI(example.net)


    for key in model:
        print "Generating commands file to be sourced"
        #example.net.iperf()
        # example.net.iperf((example.h1,example.MRSwitch1),l4Type='TCP')
        # example.net.ping(example.h1,50)
        example.net.iperf((example.h1,example.h2),l4Type='TCP')
        example.net.iperf((example.h1,example.h3),l4Type='TCP')
        example.net.iperf((example.h2,example.h3),l4Type='TCP')
        genCommandsFile(example.net.hosts, parsed_args)
        CLI(example.net)

        time = key[0]
        s = key[1].__len__()
        for index, k in enumerate(key[1]):
            update(example,k)


        sleep(time)
    example.net.pingPairFull()


def update(example,link_list):
    src = link_list[1]
    dst = link_list[2]
    bw = link_list[3]
    lan = link_list[4]
    loss = link_list[5]

    if bw == 0 and lan == 0 and loss == 0:
        print 'Link is not updated'
    elif bw == 0 and lan == 0 and loss != 0:
        print 'Update link between ' + src + ' ' + dst + ' loss=' + str(loss)
        example.link_dic[src, dst].intf1.config(loss=loss)
    elif bw == 0 and lan != 0 and loss == 0:
        print 'Update link between ' + src + ' ' + dst + ' delay=' + str(lan)
        example.link_dic[src, dst].intf1.config(delay=lan)
    elif bw != 0 and lan == 0 and loss == 0:
        print 'Update link between ' + src + ' ' + dst + ' bw=' + str(bw)
        example.link_dic[src, dst].intf1.config(bw=bw)
    elif bw != 0 and lan != 0 and loss == 0:
        print 'Update link between ' + src + ' ' + dst + ' bw=' + str(bw) + ' delay=' + str(lan)
        example.link_dic[src, dst].intf1.config(bw=bw, delay=lan)
    elif bw != 0 and lan == 0 and loss != 0:
        print 'Update link between ' + src + ' ' + dst + ' bw=' + str(bw) + ' loss=' + str(loss)
        example.link_dic[src, dst].intf1.config(bw=bw, loss=loss)
    elif bw == 0 and lan != 0 and loss != 0:
        print 'Update link between ' + src + ' ' + dst + ' delay=' + str(lan) + ' loss=' + str(loss)
        example.link_dic[src, dst].intf1.config(delay=lan, loss=loss)
    else:
        print 'Update link between '+ src + ' ' +  dst + ' bw='+ str(bw) +' delay='+str(lan)+' loss='+str(loss)
        example.link_dic[src,dst].intf1.config(bw = bw, delay = lan, loss = loss )


def genCommandsFile(hosts, args):
    try:
        for i in range(len(hosts)):
            if (os.path.isfile(hosts[i].name + ".out")):
                os.remove(hosts[i].name + ".out")

        cmds = open("command.txt", "w")

        for i in range(args.map):
            cmd_str = hosts[i + 1].name + " python mr_mapworker.py " + str(i) + " " + hosts[0].IP() + " " + str(
                args.masterport) + " &> " + hosts[i + 1].name + ".out &\n"
            cmds.write(cmd_str)


        k = 1 + args.map  # starting index for reducer hosts (master + maps)
        for i in range(args.reduce):
            cmd_str = hosts[k + i].name + " python mr_reduceworker.py " + str(i) + " " + hosts[0].IP() + " " + str(
                args.masterport) + " &> " + hosts[k + i].name + ".out &\n"
            cmds.write(cmd_str)


        cmds.close()

    except:
        print "Unexpected error in run mininet:", sys.exc_info()[0]
        raise


def main():

    parsed_args = parseCmdLineArgs()

    model = []
    with open(parsed_args.json) as f:
        data = json.load(f)
        json_parser(model,data)

    setLogLevel('info')

    print "Instantiate network"
    example = runMultiLink()

    link_update(example, model,parsed_args)



    print "Generating commands file to be sourced"

    example.net.stop()




main()



