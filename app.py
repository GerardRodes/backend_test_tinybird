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
from concurrent import futures
import os


fieldnames = [
    'vendorid',
    'tpep_pickup_datetime',
    'trip_distance',
    'total_amount',
]

def write_csv(records):
    with open(f"csv/nyc_taxi-{date.today()}.csv", 'a') as fw:
        writer = csv.DictWriter(fw, fieldnames, extrasaction='ignore')
        writer.writerows(records)


def load_json(data: bytes):
    result = dict({
        'records_valid': 0,
        'records_invalid': 0,
        'records': []
    })

    fr = io.BytesIO(data)
    for record in fr.readlines():
        try:
            item = json.loads(record)
            # pick only required fields to reduce serialization cost on thread communication
            result['records'].append({k:item[k] for k in fieldnames})
            result['records_valid'] += 1
        except Exception:
            result['records_invalid'] += 1

    return result

@tornado.web.stream_request_body
class DataReceiverHandler(tornado.web.RequestHandler):
    def initialize(self, exec: futures.ProcessPoolExecutor, loop: asyncio.AbstractEventLoop):
        self.exec = exec
        self.loop = loop
        self.bytes_len = 0
        self.prev_tail = b''
        self.records = []
        self.records_valid = 0
        self.records_invalid = 0
        self.futures = []

    def data_received(self, chunk):
        self.bytes_len += len(chunk)
        head, tail = chunk.rsplit(b'\n', 1)
        self.futures.append(self.loop.run_in_executor(self.exec, load_json, self.prev_tail + head))
        # self.load_json(self.prev_tail + head)
        self.prev_tail = tail

    def load_json(self, data: bytes):
        fr = io.BytesIO(data)
        for record in fr.readlines():
            try:
                self.records.append(json.loads(record))
                self.records_valid += 1
            except Exception:
                self.records_invalid += 1

    async def post(self):
        self.futures.append(self.loop.run_in_executor(self.exec, load_json, self.prev_tail))

        records_valid = 0
        records_invalid = 0
        records = []
        # as_completed will change the futures order
        # but we don't care if we write the lines in a different order
        for future in asyncio.as_completed(self.futures):
            result = await future
            records_valid += result["records_valid"]
            records_invalid += result["records_invalid"]
            records += result['records']

        write_csv(records)
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
tornado.process.fork_processes(num_processes=6)
async def run():
    executor = futures.ProcessPoolExecutor(max_workers=3)
    application = tornado.web.Application([
        (r"/", DataReceiverHandler, {
            'exec': executor,
            'loop': asyncio.get_running_loop(),
        }),
    ], debug=bool(sys.flags.debug))
    tornado.httpserver.HTTPServer(application).add_sockets(sockets)
    await asyncio.Event().wait()
    executor.shutdown()

if __name__ == "__main__":
    asyncio.run(run())
