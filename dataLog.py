# -*- coding: UTF8 -*-
'''
Created on 2014-11-30

@author: tao.z
'''
import pdb
import socket
from bencode import bencode, bdecode
from settings import *

class DataLog(object):
    """docstring for DataLog"""
    def __init__(self, port=9998):
        super(DataLog, self).__init__()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("0.0.0.0", port))
	self.socket.setblocking(0)

    def send_log(self, msg, address=(DLHOST,DLPORT)):
        msg['t'] = TOKEN
        try:
            self.socket.sendto(bencode(msg), address)
        except Exception,e:
            pass
