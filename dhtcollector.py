# coding: utf-8

import pdb
import os, sys
import json, time, re, logging, urllib2
import traceback as tb
import libtorrent as lt
from string import Template
from bencode import bdecode
from urllib2 import HTTPError
import dbManage

manage = dbManage.DBManage()

logging.basicConfig(level=logging.INFO,
                   # format='%(asctime)s [line: %(lineno)d] %(levelname)s %(message)s',
                   format='%(levelname)s %(message)s',
                   datefnt='%d %b %H:%M%S',
                   filename='./processing.log',
                   filemode='wb')

DHT_ROUTER_NODES = [
    ('router.bittorrent.com', 6881),
    ('router.utorrent.com', 6881),
    ('router.bitcomet.com', 6881),
    ('dht.transmissionbt.com', 6881)
]

BTSTORAGESERVERS = [
    'http://thetorrent.org/${info_hash}.torrent',
    'https://zoink.it/torrent/${info_hash}.torrent',
    'https://torcache.net/torrent/${info_hash}.torrent'
]

RVIDEO = re.compile(r"\.mkv$|\.mp4$|\.avi$|\.rmvb$|\.rm$|\.asf$|\.mpg$|\.wmv$|\.vob$")
RAUDIO = re.compile(r"\.mp3$|\.ogg$|\.asf$|\.wma$|\.wav$|\.acc$|\.flac$|\.ape$|\.lpac$")

class DHTCollector(object):
    '''
    一个简单的 bt 下载工具，依赖开源库 libtorrent.
    '''
    # libtorrent下载配置
    _upload_rate_limit = 200000
    _download_rate_limit = 200000
    _active_downloads = 30
    _alert_queue_size = 4000
    _dht_announce_interval = 60
    _torrent_upload_limit = 10000
    _torrent_download_limit = 20000
    _auto_manage_startup = 30
    _auto_manage_interval = 15
    _ALERT_TYPE_SESSION = None
    # 主循环 sleep 时间
    _sleep_time = 0.5
    _start_port = 32800
    _sessions = []
    _infohash_queue_from_getpeers = []
    _info_hash_set = {}
    _current_meta_count = 0
    _meta_list = {}

    def __init__(self,
                 session_nums=50,
                 delay_interval=40,
                 exit_time=4*60*60,
                 result_file=None,
                 stat_file=None):
        self._session_nums = session_nums
        self._delay_interval = delay_interval
        self._exit_time = exit_time
        self._result_file = result_file
        self._stat_file = stat_file
        self._backup_result()

        try:
            with open(self._result_file, 'rb') as f:
                self._meta_list = json.load(f)
        except Exception as err:
            pass

    def _backup_result(self):
        back_file = '%s_%s' % (time.strftime('%Y%m%d%H'), self._result_file)
        if not os.path.isfile(back_file):
            os.system('cp %s %s' %
                      (self._result_file,
                       back_file))

    def _get_file_info_from_torrent(self, handle):
        file_info = {}

        try:
            torrent_info_obj = handle.get_torrent_info()
            
            file_info['name'] = torrent_info_obj.name()
            file_info['creator'] = torrent_info_obj.creator()
            file_info['comment'] = torrent_info_obj.comment()
            file_info['num_files'] = torrent_info_obj.num_files()
            file_info['total_size'] = torrent_info_obj.total_size()
            file_info['is_valid'] = torrent_info_obj.is_valid()
            file_info['priv'] = torrent_info_obj.priv()
            # file_info['is_i2p'] = torrent_info_obj.is_i2p()
            # file_info['creation_date'] = torrent_info_obj.creation_date()
            file_info['info_hash'] = torrent_info_obj.info_hash().to_string().encode('hex')

            file_info['media_type'] = None
            file_info['files'] = []

            for file in torrent_info_obj.files():
                file_info['files'].append({
                    'path': file.path,
                    'size': file.size
                })
                
                if file_info['media_type'] is None and re.search(RVIDEO, file.path):
                    file_info['media_type'] = 'video'
                elif file_info['media_type'] is None and re.search(RAUDIO, file.path):
                    file_info['media_type'] = 'audio'
        
        except Exception, e:
            logging.error('torrent_info_error: '+str(e))


        print json.dumps(file_info, ensure_ascii=False)

        manage.saveTorrent(file_info)

    def _get_runtime(self, interval):
        day = interval / (60*60*24)
        interval = interval % (60*60*24)
        hour = interval / (60*60)
        interval = interval % (60*60)
        minute = interval / 60
        interval = interval % 60
        second = interval
        return 'day: %d, hour: %d, minute: %d, second: %d' % \
               (day, hour, minute, second)

    # 辅助函数
    # 事件通知处理函数
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
    8:Do 9,10 steps and after connected to trackers succeed request trackers for pieces of the torrent.
    (9):Post alerts when blocks are requested and completed. 
    Also when pieces are completed.[progress_notification].
    (10):add torrent to session and post add_torrent_alert.
    After add torrent to session succeed post torrent_added_alert.
    12:After both 9 and 10 finished request files belong to the torrent.
    13:Pieces download ended post torrent_finished_alert.
    '''
    def _handle_alerts(self, session, alerts):
        while len(alerts):
            alert = alerts.pop()
            if isinstance(alert, lt.add_torrent_alert):
                '''
                session pop the torrent which attempted to be added.
                包含当前add动作的状态，属性：error、handle
                '''
                alert.handle.set_upload_limit(self._torrent_upload_limit)
                alert.handle.set_download_limit(self._torrent_download_limit)

            if isinstance(alert, lt.piece_finished_alert):
                logging.info('piece_finished_alert')
                print 'one piece...'

            if isinstance(alert, lt.torrent_finished_alert):
                logging.info('torrent_finished_alert')
                print 'finished'

            if isinstance(alert, lt.torrent_added_alert):
                logging.info('torrent_added_alert')
                # if self._ALERT_TYPE_SESSION is None:
                #     self._ALERT_TYPE_SESSION = session

            if isinstance(alert, lt.metadata_received_alert):
                logging.info('metadata_received_alert')
                print 'metadata received'
                handle = alert.handle
                if handle:
                    self._get_file_info_from_torrent(handle)
                    #不需要下载
                    session.remove_torrent(handle, True)


            elif isinstance(alert, lt.dht_announce_alert):
                '''
                DHT网路中一个Node对本Node上的一条info-hash认领
                '''
                logging.info('dht_announce_alert' + alert.message())
                info_hash = alert.info_hash.to_string().encode('hex')
                if info_hash in self._meta_list:
                    self._meta_list[info_hash] += 1
                else:
                    self._meta_list[info_hash] = 1
                    self._current_meta_count += 1
                    self.add_magnet(session, alert.info_hash)

            elif isinstance(alert, lt.dht_get_peers_alert):
                '''
                其他DHT node向本node针对一条info-hash发起对接
                '''
                logging.info('dht_get_peers_alert: ' + alert.message())
                info_hash = alert.info_hash.to_string().encode('hex')

                if info_hash in self._meta_list:
                    self._meta_list[info_hash] += 1
                else:
                    self._infohash_queue_from_getpeers.append(info_hash)
                    self._meta_list[info_hash] = 1
                    self._current_meta_count += 1
                    self.add_magnet(session, alert.info_hash)

            elif isinstance(alert, lt.torrent_removed_alert):
                logging.info('removed torrent: '+alert.message())

            #################################
            # elif self._ALERT_TYPE_SESSION is not None and self._ALERT_TYPE_SESSION == session:
            #    logging.info('********Alert message: '+ alert.message() + '    Alert category: ' + str(alert.category()))
            #################################

    # 创建 session 对象
    def create_session(self, begin_port=32800):
        self._start_port = begin_port
        #在限制nums个数范围内创建session
        for port in range(begin_port, begin_port + self._session_nums):
            session = lt.session()
            #设置alerts接受的mask类型，默认只接收errors类型
            session.set_alert_mask(lt.alert.category_t.all_categories)
            session.listen_on(port, port+10)

            for router in DHT_ROUTER_NODES:
                session.add_dht_router(router[0],router[1])

            settings = session.get_settings()
            settings['upload_rate_limit'] = self._upload_rate_limit
            settings['download_rate_limit'] = self._download_rate_limit
            settings['active_downloads'] = self._active_downloads
            settings['auto_manage_startup'] = self._auto_manage_startup
            settings['auto_manage_interval'] = self._auto_manage_interval
            settings['dht_announce_interval'] = self._dht_announce_interval
            settings['alert_queue_size'] = self._alert_queue_size
            session.set_settings(settings)
            self._sessions.append(session)
        return self._sessions

    # 添加磁力链接
    '''
    抽取到download类实现
    '''
    def add_magnet(self, session, info_hash):
        # 创建临时下载目录
        if not os.path.isdir('collections'):
            os.mkdir('collections')

        params = {'save_path': os.path.join(os.curdir,
                                            'collections',
                                            'magnet'),
                  'storage_mode': lt.storage_mode_t.storage_mode_sparse,
                  'paused': False,
                  'auto_managed': True,
                  'duplicate_is_error': True,
                  'info_hash': info_hash}

        session.async_add_torrent(params)

        # if self._ALERT_TYPE_SESSION == None:
            # self._ALERT_TYPE_SESSION = session

        logging.info('Get torrent starting.')

    def start_work(self):
        # 清理屏幕
        logging.info('beggin!')
        begin_time = time.time()
        show_interval = self._delay_interval
        while True:
            for session in self._sessions:
                '''
                request this session to POST state_update_alert
                信息包含从上一次POST之后state有过改变的所有torrent的status_notification[one of alert mask category]
                '''
                session.post_torrent_updates()
                '''
                session.pop_alerts & session.pop_alert
                pop set_alert_mask所指定了的alerts
                pop_alerts pop所有alerts
                pop_alert only pop errors or events which has occurred
                每一次pop查询都需要与network线程进行一次双方通信【耗性能】
                '''
                self._handle_alerts(session, session.pop_alerts())
            
            time.sleep(self._sleep_time)
            if show_interval > 0:
                show_interval -= 1
                continue
            show_interval = self._delay_interval

            # 统计信息显示
            show_content = ['torrents:']
            interval = time.time() - begin_time
            show_content.append('  pid: %s' % os.getpid())
            show_content.append('  time: %s' %
                                time.strftime('%Y-%m-%d %H:%M:%S'))
            show_content.append('  run time: %s' % self._get_runtime(interval))
            show_content.append('  start port: %d' % self._start_port)
            show_content.append('  collect session num: %d' %
                                len(self._sessions))
            show_content.append('  info hash nums from get peers: %d' %
                                len(self._infohash_queue_from_getpeers))
            show_content.append('  torrent collection rate: %f /minute' %
                                (self._current_meta_count * 60 / interval))
            show_content.append('  current torrent count: %d' %
                                self._current_meta_count)
            show_content.append('  total torrent count: %d' %
                                len(self._meta_list))
            show_content.append('\n')

            # 存储运行状态到文件
            try:
                with open(self._stat_file, 'wb') as f:
                    f.write('\n'.join(show_content))
                with open(self._result_file, 'wb') as f:
                    json.dump(self._meta_list, f)
            except Exception as err:
                pass

            # 每天结束备份结果文件
            self._backup_result()

            # 测试是否到达退出时间
            if interval >= self._exit_time:
                # stop
                logging.info('stoped!')
                break
            else:
                print '\t',interval,'----',self._exit_time

        # 销毁p2p客户端
        for session in self._sessions:
            torrents = session.get_torrents()
            for torrent in torrents:
                session.remove_torrent(torrent)
        logging.info('Ended.')

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'argument err:'
        print '\tpython dhtcollector.py result.json collector.state\n'
        sys.exit(-1)

    result_file = sys.argv[1]
    stat_file = sys.argv[2]
    # 创建采集对象
    sd = DHTCollector(result_file=result_file,
                   stat_file=stat_file)
    # 创建p2p客户端
    sd.create_session(32900)
    sd.start_work()
