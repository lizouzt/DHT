import libtorrent as lt
import time
import os

ses = lt.session()
ses.listen_on(6881, 6891)

e = lt.bdecode(open("Ab-Soul.torrent", 'rb').read())
info = lt.torrent_info(e)

params = {'save_path': os.path.join(os.curdir,
                                            'collections',
                                            'magnet'),
                  'storage_mode': lt.storage_mode_t.storage_mode_sparse,
                  'paused': False,
                  'auto_managed': True,
                  'duplicate_is_error': True,
                  'ti': info}

'''
sys_add with torrent_handle.params
'''
h = ses.add_torrent(params)

while (not h.is_seed()):
        s = h.status()

        state_str = ['queued', 'checking', 'downloading metadata', \
                'downloading', 'finished', 'seeding', 'allocating']

        print '%.2f%% complete (down: %.1f kb/s up: %.1f kB/s peers: %d) %s' % \
                (s.progress * 100, s.download_rate / 1000, s.upload_rate / 1000, \
                s.num_peers, s.state)

        time.sleep(1)