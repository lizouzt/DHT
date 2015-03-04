# -*- coding: UTF8 -*-
import os
import pdb
import socket
import logging
import os, sys, time, json, re
from datetime import date

logger = logging.getLogger()
fh = logging.FileHandler('tracker-%s.log' % date.today(), 'wb')
sh = logging.StreamHandler()

fhFmt = logging.Formatter('%(asctime)s [line: %(lineno)d] %(levelname)s %(message)s')
shFmt = logging.Formatter('%(levelname)s %(message)s')

fh.setFormatter(fhFmt)
sh.setFormatter(shFmt)

logger.setLevel(logging.INFO)
logger.addHandler(fh)
logger.addHandler(sh)

class Peer_Connection(object):
    def __init__(self, sock, addr):
        self._buffer = ""
        self.sock = sock
        self.addr = addr
        self.am_interested = False
        self.am_blocking = True
        self.peer_interested = False
        self.peer_blocking = True
        self.honeypot_name = ""
        pass

    def data_come_in(self, length):
        while True:
            try:
                #self.sock.settimeout(HTTP_TO)
                if length <= len(self._buffer):
                    message = self._buffer[:length]
                    self._buffer = self._buffer[length:]
                    return message
                else:
                    # Be careful of the zero-length message
                    b = self.sock.recv(2*20)
                    if not len(b):
                        raise Exception("Zero length message!")
                    else:
                        self._buffer += b
            except Exception, err:
                print "Exception:Peer_Connection.data_come_in():", err
                if self.sock:
                    self.sock.close()
                self.sock = None
                break  # It is better to disconnect
        pass

class Peer(object):
    """docstring for Peer"""
    def __init__(self, port):
        self.peerlist = dict()
        self.port = port
        self.nid = ''
        self.socket = None

    def peer(self, host, port, peerid=""):
        d = {
            'conn': Peer_Connection(host, port),
            'azureus': False,
            'utorrent': False,
            'DHT': False,
            'DHT_PORT': None,
            'FAST_EXTENSION': False,
            'NAT_TRAVERSAL': False,
            'peerid': peerid
            }
        return d

    def register_peer(self, host, port, info_hash):
        """Set up the application layer communication channel"""
        if (host, port) not in self.peerlist:
            self.peerlist[(host, port)] = self._peer_dict(host, port)
        peer = self.peerlist[(host, port)]
        conn = peer['conn']
        sock = conn.connect()
        if not sock:
            return False
        # Send handshake
        self.send_handshake(sock)
        protocol = conn.data_come_in(1 + len(protocol_name))
        # Get peer's config
        reserved = conn.data_come_in(8)
        if ord(reserved[0]) & AZUREUS:
            peer['azureus'] = True
        if ord(reserved[5]) & UTORRENT:
            peer['utorrent'] = True
        if ord(reserved[7]) & DHT:
            peer['DHT'] = True
        if ord(reserved[7]) & FAST_EXTENSION:
            peer['FAST_EXTENSION'] = True
        if ord(reserved[7]) & NAT_TRAVERSAL:
            peer['NAT_TRAVERSAL'] = True
        # Get infohash
        infohash = conn.data_come_in(20)
        # Get peer id
        peer['peerid'] = conn.data_come_in(20)
        # Handshake finish!
        print peer
        # Send peer id, initiator has different behaviors from receiver, refer
        try:
            sock.settimeout(HTTP_TO)
            sock.sendall(self.id)
        except Exception, err:
            if DEBUG:
                print "Exception:Peer.connect():send_id:", err
        return True

    def send_handshake(self, sock):
        try:
            sock.settimeout(HTTP_TO)
            sock.sendall(''.join((chr(len(protocol_name)),
                                  protocol_name,
                                  FLAGS,
                                  self.metainfo.infohash)))
        except Exception, err:
            if DEBUG:
                print "Exception:Peer.send_handshake():", err
        pass

    def calc_bitfield(self, bitfield):
        p = 0
        for x in bitfield:
            for i in range(8):
                if (ord(x)>>i)&0x1:
                    p += 1
        print "I have %i pieces" % p
        pass

    def got_message(self, msg, conn):
        t = msg[0]
        r = None
        if t == UTORRENT_MSG:
            print "UTORRENT_MSG"
        if t == CHOKE:
            print "CHOKE"
        elif t == UNCHOKE:
            print "UNCHOKE"
        elif t == INTERESTED:
            print "INTERESTED"
        elif t == NOT_INTERESTED:
            print "NOT_INTERESTED"
        elif t == HAVE:
            i = struct.unpack("!i", msg[1:])[0]
            print "HAVE", i
        elif t == BITFIELD:
            print "BITFIELD"
            self.calc_bitfield(msg[1:])
        elif t == REQUEST:
            print "REQUEST"
        elif t == CANCEL:
            print "CANCEL"
        elif t == PIECE:
            print "PIECE"
        elif t == PORT:
            print "PORT", struct.unpack("!H", msg[1:])[0]
            #self.peerlist[conn.addr]["DHT"] = True
            #self.peerlist[conn.addr]["DHT_PORT"] = struct.unpack("!H", msg[1:])[0]
        #elif t == SUGGEST_PORT:
        #    print "SUGGEST_PORT"
        elif t == HAVE_ALL:
            print "HAVE_ALL"
        elif t == HAVE_NONE:
            print "HAVE_NONE"
        elif t == REJECT_REQUEST:
            print "REJECT_REQUEST"
        elif t == ALLOWED_FAST:
            print "ALLOWED FAST"
        elif t == UTORRENT_MSG:
            ext_type = ord(msg[1])
            d = bdecode(msg[2:])
            print "?"*10, d
            infodict = bencode(self.honey[self.honey.keys()[0]])
            response = {"msg_type": ord(chr(1)), "piece":d["piece"], "total_size":16*2**10}
            response = chr(20) + chr(conn.ut_metadata) + bencode(response) + infodict[d["piece"]*2**14: (d["piece"]+1)*2**14]
            response = struct.pack("!i", len(response)) + response
            print response[:300]
            conn.sock.sendall(response)
        else:
            print "got unknown message", repr(msg)
        # Continue
        return r

    def incoming_peer(self, sock, addr):
        try:
            ipeer = {}
            conn = Peer_Connection(sock, addr)
            sock.settimeout(HTTP_TO)
            # Get peer's BT protocol
            pstrlen = ord(conn.data_come_in(1))
            ipeer['protocol'] = conn.data_come_in(pstrlen)
            print pstrlen, ipeer['protocol']
            # Get peer's config
            reserved = conn.data_come_in(8)
            if ord(reserved[0]) & AZUREUS:
                ipeer['azureus'] = True
            if ord(reserved[5]) & UTORRENT:
                ipeer['utorrent'] = True
            if ord(reserved[7]) & DHT:
                ipeer['DHT'] = True
            if ord(reserved[7]) & FAST_EXTENSION:
                ipeer['FAST_EXTENSION'] = True
            if ord(reserved[7]) & NAT_TRAVERSAL:
                ipeer['NAT_TRAVERSAL'] = True
            # Get peer's infohash
            ipeer['infohash'] = conn.data_come_in(20)
            # Send my handshake
            sock.sendall(''.join((chr(len(protocol_name)),
                                  protocol_name,
                                  FLAGS,
                                  ipeer['infohash'],
                                  self.id)))
            # Get peer id
            ipeer['peerid'] = conn.data_come_in(20)
            print "1$"*50
            if ipeer['infohash'] in self.honey.keys():
                print "<>" * 50, repr(conn._buffer)
            print "2$"*50, ipeer
            # Record the infohash
            logger.info( "%s\t%s\t%s\n" % (str(addr),time.ctime(),intify(ipeer["infohash"])) )

            # Get extension message
            if ipeer['utorrent']:
                response = {'m': {'ut_pex': ord(UTORRENT_MSG_PEX), "ut_metadata": ord(chr(3)), "metadata_size": 49152},
                            'v': ('%s %s' % ("utorrent", "3.01")).encode('utf8'),
                            'e': 0,
                            'p': self.port
                            }
                response = chr(20) + chr(0) + bencode(response)
                response = struct.pack("!i", len(response)) + response
                sock.sendall(response)
                print "3$"*50
                pstrlen = struct.unpack("!i", conn.data_come_in(4))[0]
                msg = conn.data_come_in(pstrlen)
                print "&"*10, pstrlen, len(msg)
                #msg = bdecode(msg[1:])
                assert(ord(msg[0])==20)
                assert(ord(msg[1])==0)
                msg = bdecode(msg[2:])
                conn.ut_metadata = msg['m']['ut_metadata']
                print msg
                logger.info( "%s\t%s\t%s\n" % (str(addr),time.ctime(),str(msg)) )
                print "4$"*50
            # Handshake finish!
            print ipeer
            logger.info( "%s\t%s\t%s\n" % (str(addr),time.ctime(),str(ipeer)) )

            while True:
                l = conn.data_come_in(4)
                l = struct.unpack("!i", l)[0]
                msg = conn.data_come_in(l)
                print l, repr(msg[0])
                logger.info( "%s\t%s\t%s\t%s\n" % (str(addr),time.ctime(),repr(msg[0]),repr(msg[1])) )
                self.got_message(msg, conn)
        except Exception, err:
            if DEBUG:
                print "Exception:Peer.probe_incoming_peer():", err
        pass

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind( ("", self.port) )
        self.socket.listen(5)
        print 'TCP Start.'
        while True:
            try:
                conn, addr = self.socket.accept()
                logger.info( "Peer send connect: %s\t%s\t \n" % (str(addr),time.ctime()) )
                self.probe_incoming_peer(conn, addr)
            except Exception,e:
                pass

    def stop(self):
        self.socket.close()
        print 'TCP closed.'

# if __name__ == '__main__':
#     try:
#         p = Peer(BTDPORT)
#         p.start()
#     except KeyboardInterrupt, e:
#         p.stop()
#         exit()
#     except Exception, e:
#         print 'Service Error: ',e