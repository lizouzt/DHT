# -*- coding: UTF8 -*-
import re

TOKEN = '¢¢¢.elfer.¢¢¢'

#statistic server 
DLHOST = ''
DLPORT = 60006

'''
MQUSER = ''
MQPWD = ''
MQSERVER = ''
MQDB = ''
MQPORT = 
'''
#Mongo db server
MHOST = ''
MPORT = '27017'
MUSER = 'dht'
MPWD = 'dhtpwd2015'

RVIDEO = re.compile(r"mkv$|mp4$|avi$|rmvb$|rm$|asf$|mpg$|wmv$|vob$")
RAUDIO = re.compile(r"mp3$|ogg$|asf$|wma$|wav$|acc$|flac$|ape$|lpac$")

BTSTORAGESERVERS = [
	'http://thetorrent.org/${info_hash}.torrent',
	'https://zoink.ch/torrent/${info_hash}.torrent',
]

DHT_ROUTER_NODES = [
    ('router.bittorrent.com', 6881),
    ('router.utorrent.com', 6881),
    ('router.bitcomet.com', 6881),
    ('dht.transmissionbt.com', 6881)
]
