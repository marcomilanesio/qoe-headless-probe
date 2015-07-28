#!/usr/bin/python
#
# mPlane QoE Server
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
import math


class Cusum():
    def __init__(self, cusum_name, th, i=0, alpha=0.875, c=0.5):
        self.name = cusum_name
        self.th = th
        self.alpha = alpha
        self.c = c
        self.mean = 0.0
        self.var = 0.0
        self.cusum = 0.0
        self.i = i  # keep track of count, locally
        self.items = []

    def compute(self, item):
        self.items.append(item)
        self.i += 1
        if self.i == 1:
            mean = self.mean = item
            var = self.var = 0
            cusum = self.cusum = item
            self.adjust_th(cusum)
        else:
            mean = self.alpha * self.mean + (1 - self.alpha) * item
            var = self.alpha * self.var + (1 - self.alpha) * pow((item - mean), 2)
            cusum = self.cusum + item - (self.mean + self.c * self.var)

        if cusum < 0:
            cusum = 0.0

        if self.i < 50 and cusum > 0:
            self.adjust_th(cusum)

        if cusum > self.th:
            print("{0} = {1}".format(self.i, cusum), " > ", self.th)
            return cusum
        else:
            self.mean = mean
            self.var = var
            self.cusum = cusum
        return None

    '''
    def compute_old(self, list_):
        print("[iter.{0}] sample = {1}, cusum = {2}, th = {3}".format(self.i, list_[0], self.cusum, self.th))
        for sample in list_:
            self.i += 1
            if self.i == 1:
                self.m = sample
                self.cusum = sample
                cusum_p = self.cusum
                self.adjust_th(cusum_p)
            else:
                m_p = self.alpha * self.m + (1 - self.alpha) * sample   #EWMA
                var_p = self.alpha * self.var + (1 - self.alpha) * pow((sample - m_p), 2)
                L = sample - (m_p + self.c * math.sqrt(var_p))  # incremento cusum
                cusum_p = self.cusum + L

                if cusum_p < 0:
                    cusum_p = 0.0

                if cusum_p > self.th:
                    print("anomaly detected: ", cusum_p, " > ", self.th)
                    if self.i < 100:  # FIXME end of training
                        self.adjust_th(self.cusum)
                    return cusum_p
                else:
                    self.m = m_p
                    self.var = var_p
                    self.cusum = cusum_p
        return None
    '''

    def get_mean_var(self):
        return self.m, self.var

    def get_count(self):
        return self.i

    def adjust_th(self, computed_cusum):
        if self.i == 1:
            self.th = computed_cusum
        self.th = (1 - self.alpha) * computed_cusum + self.alpha * self.th

    def get_cusum_value(self):
        return self.cusum

    def get_th(self):
        return self.th

    def compute_new_threshold(self, cusum_name):
        new_th = self.m + 3 * self.var
        return new_th


if __name__ == '__main__':
    a = [1, 1, 1, 1, 1, 2]
    c = Cusum('test', 1)

    for el in a:
        c.compute(el)
    print("Finale. cusum: ", c.get_cusum_value(), "; th: ", c.get_th())