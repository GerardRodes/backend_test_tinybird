import csv
import io
import sys
import asyncio

import tornado
from tornado import web
from datetime import date
# orjson is a much faster deserializer, it provides us a free speed up
# but I am not going to use it as it feels unfair for the purposes of this benchmark
import json


fieldnames = [
    'vendorid',
    'tpep_pickup_datetime',
    'trip_distance',
    'total_amount',
]

@tornado.web.stream_request_body
class DataReceiverHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.bytes_len = 0
        self.prev_tail = b''
        self.records = []
        self.records_valid = 0
        self.records_invalid = 0

    def data_received(self, chunk):
        self.bytes_len += len(chunk)
        head, tail = chunk.rsplit(b'\n', 1)
        self.load_json(self.prev_tail + head)
        self.prev_tail = tail

    def load_json(self, data: bytes):
        fr = io.BytesIO(data)
        for record in fr.readlines():
            try:
                self.records.append(json.loads(record))
                self.records_valid += 1
            except Exception:
                self.records_invalid += 1

    def post(self):
        self.load_json(self.prev_tail)

        with open(f"csv/nyc_taxi-{date.today()}.csv", 'a') as fw:
            writer = csv.DictWriter(fw, fieldnames, extrasaction='ignore')
            writer.writerows(self.records)

        self.write({
            'result': {
                'status': 'ok',
                'stats': {
                    'bytes': self.bytes_len,
                    'records': {
                        'valid': self.records_valid,
                        'invalid': self.records_invalid,
                        'total': self.records_valid + self.records_invalid,
                    },
                }
            }
        })

sockets = tornado.netutil.bind_sockets(8888)
tornado.process.fork_processes(num_processes=None)
async def run():
    application = tornado.web.Application([
        (r"/", DataReceiverHandler),
    ], debug=bool(sys.flags.debug))
    tornado.httpserver.HTTPServer(application).add_sockets(sockets)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(run())
