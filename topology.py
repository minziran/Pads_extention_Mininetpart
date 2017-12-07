#!/usr/bin/python

"""
This is a simple example that demonstrates multiple links
between nodes.
"""

from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.topo import Topo
from mininet.link import TCLink, TCIntf, Link
from time import sleep


class runMultiLink():
    link_dic = {}
    def __init__(self):
        self.net = Mininet(autoStaticArp=True)
        self.net.addController('c0')
        self.h1 = self.net.addHost('h1')
        self.h2 = self.net.addHost('h2')
        self.h3 = self.net.addHost('h3')
        self.MRSwitch1 = self.net.addSwitch('MRSwitch1')
        self.link1 = self.net.addLink(self.h1, self.MRSwitch1, cls=TCLink)
        self.link2 = self.net.addLink(self.h2, self.MRSwitch1, cls=TCLink)
        self.link3 = self.net.addLink(self.h3, self.MRSwitch1, cls=TCLink)
        self.link_dic['h1','MRSwitch1'] = self.link1
        self.link_dic['h2','MRSwitch1'] = self.link2
        self.link_dic['h3','MRSwitch1'] = self.link3


def runApps(net):
    "Run cmds in command.txt"
    print "Running applications"
    with open("command.txt") as f:
        for line in f:
            splitedLine = line.split(' ', 1)
            print splitedLine[0]
            host = net.get(splitedLine[0])
            print splitedLine[1]
            host.cmd(splitedLine[1])
            sleep(1)

if __name__ == '__main__':
    setLogLevel( 'info' )
    example = runMultiLink()
    example.net.start()
    example.net.pingAll()
    example.link1.intf1.config(bw=5)
    example.net.iperf()
    example.link1.intf1.config(bw=50)
    example.net.iperf()
    example.net.pingPairFull()
    example.net.stop()

