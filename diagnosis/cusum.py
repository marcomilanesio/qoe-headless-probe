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
    def __init__(self, name, th, value=0.0, mean=None, var=None, count=1, alpha=0.875, c=0.5):
        self.name = name
        self.th = th
        self.alpha = alpha
        self.c = c
        if not mean:
            self.mean = value
            self.var = 0.0
        else:
            self.mean = mean
            self.var = var
        self.cusum = value
        self.count = count  # keep track of count, locally

    def compute(self, item):
        self.count += 1
        if self.count == 1:
            mean = self.mean = item
            var = self.var = 0
            cusum = self.cusum = item
            self.adjust_th(cusum)
        else:
            mean = self.alpha * self.mean + (1 - self.alpha) * item
            var = self.alpha * self.var + (1 - self.alpha) * pow((item - mean), 2)
            cusum = self.cusum + item - (self.mean + self.c * self.var)
            # Old update
            # L = sample - (m_p + self.c * math.sqrt(var_p))  # incremento cusum
            # cusum_p = self.cusum + L

        if cusum < 0:
            cusum = 0.0

        if self.count < 50 and cusum > 0:
            self.adjust_th(cusum)

        if cusum > self.th:
            print("{0} = {1}".format(self.count, cusum), " > ", self.th)
            if self.count > 50:
                return cusum

        self.mean = mean
        self.var = var
        self.cusum = cusum
        return None

    def adjust_th(self, computed_cusum):
        if self.count == 1:
            self.th = computed_cusum
        else:
            self.th = (1 - self.alpha) * computed_cusum + self.alpha * self.th

    def get_th(self):
        return self.th

    def get_count(self):
        return self.count