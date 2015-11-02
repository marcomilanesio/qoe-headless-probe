# qoe-headless-probe

This tool performs root cause analysis to identify the causes of a high page load time in a web browsing session.

### Requirements
All the additional software can be retrieved from http://firelog.eurecom.fr/mplane/software:
- phantomJS headless browser toolkit
- apache flume > 1.5.2
- custom Tstat 2.4

### Setup

* Configure and compile Tstat on your machine, following the instruction at http://tstat.tlc.polito.it/index.shtml:

```bash
  $ cd eur-tstat-2.4
  $ ./autogen.sh
  $ ./configure.sh
  $ make
  $ cd ..
```
<b>DO NOT</b> run "make install".

* You need to compile with sudo privileges the C programs in the script/ folder:

```bash
  $ cd script
  $ sudo gcc -o start.out start.c
  $ sudo gcc -o stop.out stop.c
  $ sudo chmod 4755 *.out
```
* You need a set up a configuration file for Tstat, specifying the interface to sniff. e.g.,:

```
192.168.1.0/255.255.255.0
```
* If you want to use Flume as source for storing data on HDFS, configure Flume as detailed in https://flume.apache.org/FlumeUserGuide.html#configuring-individual-components

* Modify accordingly the parameters in the file conf/firelog.conf.

* Run 
```
$ ./phantomprobe.py -h
```
