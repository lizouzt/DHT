# -*- coding: UTF8 -*-
import os
import pdb
import socket
from datetime import date
from socket import inet_aton, inet_ntoa
from struct import unpack, pack
from hashlib import sha1
from random import randint
from threading import Timer

KEEP_RUNNING = True

#生成数字串
#param [{init}] bytes 长度
#return [{str}] 字符串
def entropy(bytes):
    s = ""  
    for i in range(bytes):  
        s += chr(randint(0, 255))
    return s
#生成hash字串
#return [{hash}] hash值
def random_id():
    hash = sha1()
    hash.update(entropy(20))
    return hash.digest()

def decode_nodes(nodes):
    n = []
    length = len(nodes)
    if (length % 26) != 0:
        return n
    for i in range(0, length, 26):
        nid = nodes[i:i+20]
        ip = inet_ntoa(nodes[i+20:i+24])
        port = unpack("!H", nodes[i+24:i+26])[0]
        n.append( (nid, ip, port) )
    return n
def encode_nodes(nodes):
    strings = []
    for node in nodes:
        s = "%s%s%s" % (node.nid, inet_aton(node.ip), pack("!H", node.port))
        strings.append(s)
    return "".join(strings)
def intify(hstr):
    return long(hstr.encode('hex'), 16)
def timer(t, f):
    if KEEP_RUNNING:
        t['timer'] = Timer(t['time'], f)
        t['timer'].start()
    else:
        print 'TIMER ENDED!'
def get_time_formatter(interval):
    day = interval / (60*60*24)
    interval = interval % (60*60*24)
    hour = interval / (60*60)
    interval = interval % (60*60)
    minute = interval / 60
    interval = interval % 60
    second = interval
    return 'day: %d, hour: %d, minute: %d, second: %d' % \
           (day, hour, minute, second)

def unpack_host(host):
    if len(host) == 4:
        return socket.inet_ntop(socket.AF_INET, host)
    elif len(host) == 16: 
        return socket.inet_ntop(socket.AF_INET6, host)

def unpack_port(port):
    return (ord(port[0]) << 8) + ord(port[1])

def unpack_hostport(addr):
    if len(addr) == 6:
        host = addr[:4]
        port = addr[4:6]
    if len(addr) == 18:
        host = addr[:16]
        port = addr[16:18]
    return (unpack_host(host), unpack_port(port))