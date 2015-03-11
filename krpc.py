# -*- coding: UTF8 -*-
import pdb
import socket
import MySQLdb
import dbManage
import hashlib
import logging
import os, sys, time, json, re
from datetime import date
from bisect import bisect_left
from bencode import bencode, bdecode
from time import sleep
from utils import *
from getTorrent import Peer
from settings import *

logger = logging.getLogger('dht')
fh = logging.FileHandler('%s.log' % date.today(), 'wb')
sh = logging.StreamHandler()

fhFmt = logging.Formatter('%(asctime)s [line: %(lineno)d] %(levelname)s %(message)s')
shFmt = logging.Formatter('%(levelname)s %(message)s')

fh.setFormatter(fhFmt)
sh.setFormatter(shFmt)

logger.setLevel(logging.INFO)
logger.addHandler(fh)
logger.addHandler(sh)

BOOTSTRAP_NODES = [
    ('router.bittorrent.com', 6881),
    ('router.utorrent.com', 6881),
    ('dht.transmissionbt.com', 6881)
]

PORT = 8006
BTDPORT = 8001
K = 8
TID_LENGTH = 4
KRPC_TIMEOUT = {'time': 20, 'timer': None}
REBORN_TIME = {'time': 10 * 60, 'timer': None}

#######################
try_get_peers_infohash_list = {}
#######################

peer = Peer(BTDPORT)

class BucketFull(Exception):  
    pass
class KRPC(object):  
    def __init__(self):  
        self.types = {
            "r": self.response_received,
            "q": self.query_received
        }
        self.actions = {
            "ping": self.ping_received,
            "find_node": self.find_node_received,
            "get_peers": self.get_peers_received,
            "announce_peer": self.announce_peer_received,
        }
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("0.0.0.0", self.port))

    def response_received(self, msg, address):
        if 'r' in msg:
            if 'q' in msg and msg['q'] == 'find_node':
		print 'find_response'
                self.find_node_handler(msg)
	    elif 'values' in msg['r'] or 'nodes' in msg['r']:
            	self.peers_response_handler(msg, address)
            elif 'id' in msg['r']:
            	self.announce_response_handler(msg, address)
	    else:
                print 'response error: ',msg

    def query_received(self, msg, address):
        self.actions[msg["q"]](msg, address)

    def send_krpc(self, msg, address):
        try:
            self.socket.sendto(bencode(msg), address)
        except Exception,e:
            pass

class Client(KRPC):
    def __init__(self):
        timer(KRPC_TIMEOUT, self.fill_the_buckets)
        timer(REBORN_TIME, self.reborn)
        KRPC.__init__(self)
    def find_node(self, address, nid=None):
        '''
        say hi
        '''
        target = self.get_neighbor(nid) if nid else self.table.nid
        tid = entropy(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {"id": self.table.nid, "target": target}
        }
        self.send_krpc(msg, address)
    def find_node_handler(self, msg):
        try:
            nodes = decode_nodes(msg["r"]["nodes"])
            for node in nodes:
                (nid, ip, port) = node  
                if len(nid) != 20: continue
                if nid == self.table.nid: continue  
                self.find_node( (ip, port), nid )
        except KeyError:
            pass
    def peers_response_handler(self, msg, address):
        data = msg["r"]
        nid = data['id']
        if nid in try_get_peers_infohash_list:
            info_hash = try_get_peers_infohash_list[nid]
        else:
            return
	
        if 'nodes' in data:
            self.send_get_peers(info_hash, decode_nodes(data["nodes"]))
        elif 'values' in data:
            try_get_peers_infohash_list.pop(nid)
            values = data['values']
            for addr in values:
                ipaddr = unpack_hostport(addr)
                self.send_announce_peer(info_hash,ipaddr,msg["t"],data["token"])
        else:
            print 'useless peers_response'

    def announce_response_handler(self, msg, address):
        logger.info('announce_response_handler: %s' % msg)
        info_hash = msg['r']['id']
        remsg = {
            "t": TOKEN,
            "i": info_hash
        }
        self.socket.send_krpc(remsg, ('127.0.0.1', DLPORT))

    def joinDHT(self):
        for address in BOOTSTRAP_NODES:
            self.find_node(address)
    def fill_the_buckets(self):
        if len( self.table.buckets ) < 2:
            print 'fill_the_buckets','*****nodes nums: ',self.table.get_nodes_count()
            self.joinDHT()

        timer(KRPC_TIMEOUT, self.fill_the_buckets)
    def get_neighbor(self, target):
        return target[:10] + random_id()[10:]
    def reborn(self):
        self.master.output_stat(self.table.get_nodes_count(), self.infohash_from_getpeers_count, self.infohash_from_announcepeers_count)
        self.table.nid = random_id()
        self.table.buckets = [ KBucket(0, 2**160) ]
        print 'REBORN!'
        timer(REBORN_TIME, self.reborn)
    def send_get_peers(self, info_hash, neighbors=None):
        nodes = neighbors or self.table.buckets[randint(0, len( self.table.buckets )-1)].nodes
        msg = {
            "t": entropy(TID_LENGTH),
            "y": "q",
            "q": "get_peers",
            "a": {
                "id": self.table.nid,
                "info_hash": info_hash
            }
        }
    
        def send(node):
            if not hasattr(node, 'nid'):
                return
            self.send_krpc(msg, (node.ip, node.port))
            try_get_peers_infohash_list[node.nid] = info_hash
        try:
            if isinstance(nodes, KNode):
                send(nodes)
            else:
                for node in nodes:
                    send(node)

        except Exception as e:
            print type(nodes),isinstance(nodes,KNode)
            print 'send_get_peers_error:', e
	
    def send_announce_peer(self, info_hash, address, t=None, token='', port=BTDPORT):
        t = t or entropy(TID_LENGTH)
	msg = {
            "t": t,
            "y": "q",
            "q": "announce_peer",
            "a": {
                "id": self.table.nid,
                "info_hash": info_hash,
		"implied_port": 1,
                "port": port,
                "token": token
            }
        }
	try:
            self.send_krpc(msg, address)
            #logger.info('send announce_peer to %s for %s' % (address, info_hash.encode('hex')))
	except Exception as e:
	    print 'send_announce_peer error: ',e

    def start(self):
        self.joinDHT()
        print 'Crawler Start.'
        while True:
            try:
                (data, address) = self.socket.recvfrom(512)
                msg = bdecode(data)
                self.types[msg["y"]](msg, address)
            except Exception,e:
                pass
    def stop(self):
        KRPC_TIMEOUT['timer'] and KRPC_TIMEOUT['timer'].cancel()
        REBORN_TIME['timer'] and REBORN_TIME['timer'].cancel()
        s.socket.close()
        print 'Crawler closed.'

class Server(Client):
    def __init__(self, master, table, port=8006):
        self.infohash_from_getpeers_count = 0
        self.infohash_from_announcepeers_count = 0
        self.table = table
        self.master = master
        self.port = port
        self._client = Client.__init__(self)
    def ping_received(self, msg, address):
        try:
            nid = msg["a"]["id"]
            msg = {
                "t": msg["t"],
                "y": "r",
                "r": {"id": self.get_neighbor(nid)}
            }
            self.send_krpc(msg, address)
            self.find_node(address, nid)
        except KeyError:
            pass
    def find_node_received(self, msg, address):
        try:
            target = msg["a"]["target"]
            neighbors = self.table.get_neighbors(target)
            nid = msg["a"]["id"]
            msg = {
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": self.get_neighbor(target),
                    "nodes": encode_nodes(neighbors)
                }
            }
            self.table.append(KNode(nid, *address))
            self.send_krpc(msg, address)
            self.find_node(address, nid)
        except KeyError,e:
            print 'find_node_received error: ',e
    def get_peers_received(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            neighbors = self.table.get_neighbors(infohash)
            nid = msg["a"]["id"]
            msg = {
                "t": msg["t"],
                "y": "r",
                "r": {
                    "id": self.get_neighbor(infohash),
                    "nodes": encode_nodes(neighbors)
                }
            }
            self.table.append(KNode(nid, *address))
            self.send_krpc(msg, address)
            self.infohash_from_getpeers_count += 1
            self.find_node(address, nid)

            self.send_get_peers(infohash, decode_nodes(neighbors))
        except KeyError:
            pass
    def announce_peer_received(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            nid = msg["a"]["id"]
            remsg = {
                "t": msg["t"],
                "y": "r",
                "r": {"id": self.get_neighbor(infohash)}
            }
            self.table.append(KNode(nid, *address))
            self.send_krpc(remsg, address)
            self.infohash_from_announcepeers_count += 1
            self.find_node(address, nid)

            extra = msg['a']
            if 'info_hash' and 'token' in extra:
            	self.send_get_peers(extra['info_hash'])
	except KeyError, e:
            print 'announce_peer_received with error: ', str(e)
	except Exception as e:
	   print 'announce_extra_error:',str(e)

class KTable(object):
    def __init__(self, nid):
        self.nid = nid
        self.buckets = [ KBucket(0, 2**160) ]
    def append(self, node):
        index = self.bucket_index(node.nid)
        try:
            bucket = self.buckets[index]
            bucket.append(node)
        except IndexError,e:
            print 'append: ',e
            return
        except BucketFull:
            '''
            桶满了之后
            1、自己节点如果不在桶中则不添加
            2、在桶中则把该桶两分，再添加
            '''
            if not bucket.in_range(self.nid): return
            self.split_bucket(index)
            self.append(node)
    def get_neighbors(self, target):
        nodes = []
        if len(self.buckets) == 0: return nodes
        if len(target) != 20 : return nodes
        index = self.bucket_index(target)
        try:
            nodes = self.buckets[index].nodes
            min = index - 1
            max = index + 1
            while len(nodes) < K and ((min >= 0) or (max < len(self.buckets))):
                if min >= 0:
                    nodes.extend(self.buckets[min].nodes)
                if max < len(self.buckets):
                    nodes.extend(self.buckets[max].nodes)
                min -= 1
                max += 1
            num = intify(target)  
            nodes.sort(lambda a, b, num=num: cmp(num^intify(a.nid), num^intify(b.nid)))
            return nodes[:K]
        except IndexError:
            return nodes
    def bucket_index(self, target):  
        return bisect_left(self.buckets, intify(target))
    def split_bucket(self, index):  
        old = self.buckets[index]
        point = old.max - (old.max - old.min)/2  
        new = KBucket(point, old.max)
        old.max = point
        self.buckets.insert(index + 1, new)  
        for node in old.nodes[:]:
            if new.in_range(node.nid):
                new.append(node)
                old.remove(node)
    def get_nodes_count(self):
        c = 0
        for bucket in self.buckets:
            c += len(bucket.nodes)
        return c
    def __iter__(self):
        for bucket in self.buckets:  
            yield bucket

class KBucket(object):
    __slots__ = ("min", "max", "nodes")
    def __init__(self, min, max):
        self.min = min
        self.max = max
        self.nodes = []
    def append(self, node):
        if self.__contains__(node):
            return

        if node in self:
            self.remove(node)
            self.nodes.append(node)
        else:
            if len(self) < K:
                self.nodes.append(node)
            else:
                raise BucketFull
    def remove(self, node):  
        self.nodes.remove(node)
    def in_range(self, target):
        return self.min <= intify(target) < self.max
    def __len__(self):
        return len(self.nodes)
    def __contains__(self, node):
        return node in self.nodes
    def __iter__(self):
        for node in self.nodes:
            yield node
    def __lt__(self, target):
        return self.max <= target
class KNode(object):  
    __slots__ = ("nid", "ip", "port")  
    def __init__(self, nid, ip, port):
        self.nid = nid  
        self.ip = ip  
        self.port = port
    def __eq__(self, other):  
        return self.nid == other.nid

class Master(object):
    def __init__(self, m, stat_file):
        self._stat_file = stat_file
        self.begin_time = time.time()
        self.dbm = m
    def log(self, infohash):
        pass

    def output_stat(self, nodes_nums=0, get_peer_nums=0, announce_peer_nums=0):
        show_content = ['torrents:']
        interval = time.time() - self.begin_time
        show_content.append('  pid: %s' % os.getpid())
        show_content.append('  time: %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
        show_content.append('  run time: %s' % get_time_formatter(interval))
        show_content.append('  start port: %d' % PORT)
        show_content.append('  krpc table nodes nums: %d' % nodes_nums)
        show_content.append('  get peers nums: %d' % get_peer_nums)
        show_content.append('  announce peer nums: %d' % announce_peer_nums)
        show_content.append('\n')

        try:
            with open(self._stat_file, 'wb') as f:
                f.write('\n'.join(show_content))
        except Exception as err:
            logger.info('output_stat error: ' + str(err))

if __name__ == '__main__':
    try:
	stat_file = sys.argv[1] if len(sys.argv) >= 2 else 'info.stat'
    	# m = dbManage.DBManage()
    	m = object
    	s = Server(Master(m,stat_file), KTable(random_id()), PORT)
    	#tp = threading.Thread(target = peer.start)
    	#ts = threading.Thread(target = s.start)
    	#tp.start()
    	s.start()
    except KeyboardInterrupt:
        KEEP_RUNNING = False
        s.stop()
	#ts.join()
        logger.info('STOPED!')
        exit()
    except Exception, e:
        print 'Service Error: ',e
    #     KEEP_RUNNING = False
    #     s.stop()
    #     peer.stop()
    #     tp.join()
    #     ts.join()
    #     logger.info('STOPED!')
    #     exit()
