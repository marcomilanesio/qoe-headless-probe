#!/usr/bin/python
#
# mPlane QoE Probe
#
# (c) 2013-2014 mPlane Consortium (http://www.ict-mplane.eu)
#               Author: Marco Milanesio <milanesio.marco@gmail.com>
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
#
import re

class TracerouteParser():
    def __init__(self, target):
        self.target = target
        self.nodes = {}
        self.target_hop = 0

    def add_to_dictionary(self, hop, addr, rtts):
        if str(hop) not in self.nodes.keys():
            self.nodes[str(hop)] = [(addr, rtts)]
        else:
            if addr == self.nodes[str(hop)][0][0]:
                self.nodes[str(hop)][0][1].extend(rtts)
            else:
                if self.nodes[str(hop)][0][0] == "???":
                    self.nodes[str(hop)] = [(addr, rtts)]
                else:
                    self.nodes[str(hop)].append((addr, rtts))

    def parse_traceroutefile(self, trfile):
        f = open(trfile, 'r')
        arr = f.readlines()
        f.close()
        slices_starts = [i for i, x in enumerate(arr) if re.match("traceroute", x) or re.match("\n", x)]
        all_traces = [[slices_starts[x], slices_starts[x+1]] for x in range(0, len(slices_starts)-1)]
        
        for interval in all_traces:
            trace = arr[interval[0] + 1: interval[1]]
            for step in trace:
                tmp = step.strip().split("  ")
                hop = int(tmp[0])
                if len(tmp) > 2:
                    print str(tmp)
                    addr = [k for k in tmp[1].split(" ") if k != "*"][0]  # remove possible \* in traceroute
                    rtts = [float(tmp[i].split(" ")[0]) for i in range(2, len(tmp)) if tmp[i] != '*']
                else:
                    addr = '???'
                    rtts = []
                self.add_to_dictionary(hop, addr, rtts)

    def parse_mtrfile(self, mtrfile):
        f = open(mtrfile, 'r')
        arr = f.readlines()
        f.close()
        rows = arr[1:]
        if re.match('HOST',rows[0]): #quick & dirty for mtr 0.85 vs 0.84
            rows = rows[1:]
        last_inserted = 0
        for step in rows:
            try:
                tmp = step.strip().split(".|--")
                hop = int(tmp[0])
                last_inserted = hop
                values = [x for x in tmp[1].split(" ") if x != '']
                #remove loss rate, packet sent, last sent
                addr = values[0]
                rtts = map(float, values[5:-1]) # only get best and worst
                #print hop, addr, rtts
                self.add_to_dictionary(hop, addr, rtts)
            except ValueError: # alternative found
                alt = step.strip().split("|-- ")[1] # for the last hop inserted
                self.nodes[str(last_inserted)].append((alt, []))
                continue

    def get_results(self, print_=False):
        ordered_list_of_steps = []
        numerical_keys = sorted(map(int, self.nodes.keys()))
        for k in numerical_keys:
            ordered_list_of_steps.append(self.nodes[str(k)])
            if print_:
                print k, self.nodes[str(k)]
            if self.nodes[str(k)][0][0] == self.target:
                self.target_hop = k
                break
        
        for k in [x for x in numerical_keys if x > self.target_hop]:
            del self.nodes[str( k )]

        #return self.nodes
        return ordered_list_of_steps


if __name__ == "__main__":
    mtrfile = "./173.194.35.31.mtr"
    t = TracerouteParser("173.194.35.31")    
    t.parse_mtrfile(mtrfile)
    res = t.get_results()
    #print res
    for i in range(len(res) - 1):
        print i+1, len(res[i]), res[i]

