# coding: utf-8

import os, sys
import json, time, re, logging
import datetime
import libtorrent as lt
from string import Template
from threading import Timer,Thread
from bencode import bdecode
from urllib2 import HTTPError
import dbManage
from settings import *

def getLogger(_namespace, _filestr):
	_logger = logging.getLogger(_namespace)
	_fh = logging.FileHandler(_filestr)
	_sh = logging.StreamHandler()
	_fhFmt = logging.Formatter('%(asctime)s [line: %(lineno)d] %(levelname)s %(message)s')
	_shFmt = logging.Formatter('%(levelname)s %(message)s')
	_fh.setFormatter(_fhFmt)
	_sh.setFormatter(_shFmt)
	_logger.setLevel(logging.INFO)
	_logger.addHandler(_fh)
	_logger.addHandler(_sh)
	return _logger

logger = getLogger('libt','./%s-collector.log'% datetime.date.today().day)

manage = dbManage.DBManage(logger)

THRESHOLD = 0
_upload_rate_limit = 200000
_download_rate_limit = 200000
_alert_queue_size = 1000
_max_connections = 50
class DHTCollector():
    _ALERT_TYPE_SESSION = None
    _sleep_time = 0.7
    _sessions = []
    _meta_list = []
    _end = False
    _priv_th_queue = {}
    info_hash_queue = []

    def __init__(self,port,session_num):
        self._session_num = session_num
        self._port = port
        self.begin_time = time.time()

    def _get_file_info_from_torrent(self, handle):
        try:
            meta = {}
            torrent_info = handle.get_torrent_info()

            meta['info_hash'] = str(torrent_info.info_hash())
            meta['name'] = torrent_info.name()
            meta['num_files'] = torrent_info.num_files()
            meta['total_size'] = torrent_info.total_size()
            meta['creation_date'] = time.mktime(torrent_info.creation_date().timetuple())
            meta['valid'] = len(handle.get_peer_info())

            meta['media_type'] = None
            meta['files'] = []
            _count = 60
            for _fd in torrent_info.files():
                if _count == 0:
                    break
                _count -= 1
                if not hasattr(_fd, 'path') or not hasattr(_fd,'size'):
                    continue
                meta['files'].append({
                    'path': _fd.path,
                    'size': _fd.size
                })
                
                if meta['media_type'] is None and re.search(RVIDEO, _fd.path):
                    meta['media_type'] = 'video'
                elif meta['media_type'] is None and re.search(RAUDIO, _fd.path):
                    meta['media_type'] = 'audio'
            
            # meta['files'] = json.dumps(meta['files'], ensure_ascii=False)
            Thread(target=manage.saveTorrent, args=[meta]).start()
            #manage.saveTorrent(meta)

        except Exception, e:
            logger.error('torrent_info_error: '+str(e))

    '''
    alert logic doc:
    1:get a info_hash from DHT peer
    2:async_add_torrent start check this info_hash on DHT net
    3:when a torrent completes checking post torrent_checked_alert[status_notification[64]], 
    then looking for peers.
    5:connected to peers post peer_connect_alert[debug_notification[32]], 
    then check trackers for the torrent.
    6:torrent actived after found trackers for the torrent and post the stats_alert[stats_notification:[2048]], 
    then request trackers connect.
    7:tracker events after torrent actived. 
    Includes announcing to trackers, receiving responses, warnings and errors [tracker_notification:[16]].
    8:Success connected to trackers succeed request trackers for pieces of the torrent.
    9:add torrent to session and post add_torrent_alert.
    After add torrent to session succeed post torrent_added_alert.
    10:Get metadata
    11:When pieces download completed.[progress_notification].
    13:download ended post torrent_finished_alert.
    '''
    def _remove_torrent(self, session, alert):
        try:
            session.remove_torrent(alert.handle,1)
        except Exception,e:
            pass

    def _handle_alerts(self, session, alerts):
        while len(alerts):
            alert = alerts.pop()
            if isinstance(alert, lt.piece_finished_alert):
                self._remove_torrent(session, alert)
            elif isinstance(alert, lt.read_piece_alert):
                self._remove_torrent(session, alert)
            elif isinstance(alert, lt.torrent_finished_alert):
                print 'finished'

            elif isinstance(alert, lt.metadata_received_alert):
                handle = alert.handle
                if handle and handle.is_valid():
                    self._get_file_info_from_torrent(handle)
                try:
                    session.remove_torrent(handle, 1)
                except Exception,e:
                    pass

            elif isinstance(alert, lt.metadata_failed_alert):
                self._remove_torrent(session, alert)

            elif isinstance(alert, lt.dht_announce_alert):
                info_hash = str(alert.info_hash)
                #if info_hash not in self._meta_list:
                #self._meta_list.append(info_hash)
                self.add_magnet(session, alert.info_hash)

            elif isinstance(alert, lt.dht_get_peers_alert):
                info_hash = str(alert.info_hash)
                #if info_hash not in self._meta_list:
                #self._meta_list.append(info_hash)
                self.add_magnet(session, alert.info_hash)

            elif isinstance(alert, lt.torrent_alert):
                pass
            else:
                pass
            #################################
            # elif self._ALERT_TYPE_SESSION is not None and self._ALERT_TYPE_SESSION == session:
            #    logger.info('********Alert message: '+ alert.message() + '    Alert category: ' + str(alert.category()))
            #################################

    def add_magnet(self, session, info_hash):
        if info_hash in self.info_hash_queue:
            return
        else:
            self.info_hash_queue.append(str(info_hash))

        params = {'save_path': os.path.join(os.curdir,'collections'),
                  'storage_mode': lt.storage_mode_t.storage_mode_sparse,
                  'paused': False,
                  'auto_managed': True,
                  'duplicate_is_error': False,
                  'info_hash': info_hash}
        try:
            session.add_torrent(params)
        except Exception:
            pass

    def create_session(self):
        begin_port = self._port
        for port in range(begin_port, begin_port + self._session_num):
            session = lt.session()
            #session.set_alert_mask(lt.alert.category_t.status_notification | lt.alert.category_t.stats_notification | lt.alert.category_t.progress_notification | lt.alert.category_t.tracker_notification | lt.alert.category_t.dht_notification | lt.alert.category_t.progress_notification | lt.alert.category_t.error_notification)
            session.set_alert_mask(lt.alert.category_t.all_categories)
            session.listen_on(port, port+10)
            for router in DHT_ROUTER_NODES:
                session.add_dht_router(router[0],router[1])
            session.set_download_rate_limit(_download_rate_limit)
            session.set_upload_rate_limit(_upload_rate_limit)
            session.set_alert_queue_size_limit(_alert_queue_size)
            #session.set_max_connections(_max_connections)
            #session.set_max_half_open_connections(_max_half_open_connections)
            session.start_dht()
            session.start_natpmp()
            self._sessions.append(session)
        return self._sessions

    def clean_passive_torrent(self):
        _del_queue = []
        ###################
        for _ti, _conn in self._priv_th_queue.iteritems():
            if _conn['p'] > 450:
                _del_queue.append(_ti)
                try:
                    _conn['_session'].remove_torrent(_conn['_th'],1)
                except Exception:
                    pass
        ###################
        for _ti in _del_queue:
            del self._priv_th_queue[_ti]
            self.info_hash_queue.remove(_ti)
        #print '>'*20,'Cleaned: %s. Remained: %s'% (len(_del_queue), len(self._priv_th_queue))

    def stop_work(self):
        self._end = True
        for session in self._sessions:
            session.stop_dht()
            torrents = session.get_torrents()
            ###################
            for torrent in torrents:
                session.remove_torrent(torrent,1)

    def start_work(self):
        while True and not self._end:
            ###################
            _length = 0
            for session in self._sessions:
                '''
                request this session to POST state_update_alert
                '''
                #session.post_torrent_updates()
                _alerts = []
                _alert = True
                ###################
                while _alert:
                    _alerts.append(_alert)
                    _alert = session.pop_alert()
                ###################
                _alerts.remove(True)
                self._handle_alerts(session, _alerts)
                _ths = session.get_torrents()
                ###################
                for th in _ths:
                    status = th.status()
                    if str(status.state) == 'downloading':
                        if status.progress > 1e-10:
                            self._the_deleted_count += 1
                            logger.info('\n delete %.10f \n'%status.progress)
                            session.remove_torrent(th,1)
                    _length += 1
                    _ti = str(th.info_hash())
                    if _ti in self._priv_th_queue:
                        self._priv_th_queue[_ti]['p'] += 1
                    else:
                        self._priv_th_queue[_ti] = {'_session': session, '_th': th, 'p': 1}
            ###################
            if _length > THRESHOLD:
                Thread(target=self.clean_passive_torrent,args=()).start()
            #else:
            #    print '>'*20,_length
            time.sleep(self._sleep_time)

def main(opt, args):
    if not os.path.isdir('collections'):
        os.mkdir('collections')
    
    sd = DHTCollector(port=opt.listen_port, session_num=opt.session_num)
    try:
        logger.info('Start.')
        sd.create_session()
        sd.start_work()
    except KeyboardInterrupt:
        print 'Interrupted!'
        sd.stop_work()
        sys.exit()
    except Exception, e:
        logger.info('*'*40,'\n','Service Error: ',e)

if __name__ == '__main__':
    from optparse import OptionParser

    usage = 'usage: %prog [options]'
    parser = OptionParser(usage=usage)

    parser.add_option('-p', '--port', action='store', type='int',
                     dest='listen_port', default=8001, metavar='LISTEN-PORT',
                     help='the listen port.')
    
    parser.add_option('-n', '--snum', action='store', type='int',
                     dest='session_num', default=50, metavar='SESSION-NUM',
                     help='the dht sessions num.')

    parser.add_option('-t', '--threshold', action='store', type='int',
                     dest='threshold', default=1000, metavar='THRESHOLD-VAL',
                     help='the clean threshold value.')

    options, args = parser.parse_args()

    THRESHOLD = options.threshold
    logger.info('Port: %s, Threshold: %s, Session num: %s'%(options.listen_port, options.session_num, options.threshold))
    main(options, args)
