
"""
telnet 127.0.0.1 6010

PUT /sdb1/194424/AUTH_test/5sIhy/hell_1o HTTP/1.1
Content-Length: 5
X-Container-Host: 127.0.0.1:6041
X-Container-Partition: 200197
Connection: close
User-Agent: proxy-server 18635
Host: localhost:80
X-Container-Device: sdb4
X-Timestamp: 1384503211.65891
X-Trans-Id: -
Referer: PUT http://localhost/AUTH_test/5sIhy/hell_1o
Content-Type: application/octet-stream
"""


from swift.common.bufferedhttp import http_connect

nheaders = {
    'Content-Length': '5',
    'X-Container-Host': '127.0.0.1:6041',
    'X-Container-Partition': '109292',
    'Connection': 'close',
    'User-Agent': 'proxy-server 18770',
    'Host': 'localhost:80',
    'X-Container-Device': 'sdb4',
    'X-Timestamp': '1384504496.26945',
    'X-Trans-Id': '-',
    'Referer': 'PUT http://localhost/AUTH_test/0eVsZ/hell_1o',
    'Content-Type': 'application/octet-stream'
}

node = {
    'replication_port': 6010,
    'zone': 1,
    'weight': 1.0,
    'ip': '127.0.0.1',
    'region': 1,
    'port': 6010,
    'replication_ip': '127.0.0.1',
    'meta': '',
    'device': 'sdb1',
    'id': 0
}

partition = 71719
req_path = '/AUTH_test/0eVsZ/hell_1o'

conn = http_connect(
    node['ip'], node['port'], node['device'],
    partition, 'PUT', req_path, nheaders)

import pdb
pdb.set_trace()
resp = conn.getexpect()