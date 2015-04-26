##DHT 
* Get torrent data from DHT net.
* Check for peers trackers.
* Analyse torrent list info.
* Download it if i want.

#### Two way to get bittorrent
1. DHT&TorrentServer
    * use krpc.py & btAnalyzer.py
2. use libtorrent
    * use dhtcollector.py
    
```
    Usage: dhtcollector.py [options]
    
    Options:
      -h, --help            show this help message and exit
      -p LISTEN-PORT, --port=LISTEN-PORT
                            the listen port.
      -n SESSION-NUM, --snum=SESSION-NUM
                            the dht sessions num.
      -t THRESHOLD-VAL, --threshold=THRESHOLD-VAL
                            the clean threshold value.
```