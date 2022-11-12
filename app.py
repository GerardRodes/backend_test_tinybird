import csv
import io
import sys
import tornado.ioloop
import tornado.web
import tornado.netutil
import asyncio
import os
import math

from concurrent import futures
from datetime import date
# orjson is a much faster deserializer, it provides us a free speed up
import orjson as json


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
    def initialize(self,
        load_json_executor: futures.ThreadPoolExecutor,
        write_csv_executor: futures.ThreadPoolExecutor,
        loop: asyncio.AbstractEventLoop,
    ):
        self.bytes_len = 0
        self.prev_tail = b''
        self.loop = loop
        self.load_json_executor = load_json_executor
        self.write_csv_executor = write_csv_executor
        self.load_json_futures = []

    def data_received(self, chunk):
        self.bytes_len += len(chunk)
        # split the chunk from the last occurrence of b'\n'
        # to prevent splitting in half a json string
        head, tail = chunk.rsplit(b'\n', 1)
        # submit to load previous tail + current head so we join
        # any splitted json and send the current chunk
        self.submit_load_json(self.prev_tail + head)
        # store current tail for next iteration
        self.prev_tail = tail

    def submit_load_json(self, data):
        # send data to json executor
        self.load_json_futures.append(
            self.loop.run_in_executor(
                self.load_json_executor,
                load_json,
                data,
            )
        )

    async def post(self):
        # send remaining line
        self.submit_load_json(self.prev_tail)

        records_valid = 0
        records_invalid = 0
        records = []
        # as_completed will change the futures order
        # but we don't care if we write the lines in a different order
        for future in asyncio.as_completed(self.load_json_futures):
            result = await future
            records_valid += result["records_valid"]
            records_invalid += result["records_invalid"]
            records += result['records']

        # We don't need to wait for the write before returning a response.
        self.write_csv_executor.submit(write_csv, records)
        # If we wanted to only return a response after the writes succeed
        # we would have to replace the previous line by the following:
        # await self.loop.run_in_executor(
        #     self.write_csv_executor,
        #     write_csv,
        #     records,
        # )
        # This alternative would also use much less memory, since we are
        # returning before writting and we only have 1 worker for the
        # write_csv_executor, the direct submit call will queue a lot of
        # requests that will pile up until the worker is able to process
        # the whole queue. If we were to await to the executor like in the
        # commented code, we would use much less memory in the
        #  write_csv_executor queue, at the cost of slower responses.

        self.write({
            'result': {
                'status': 'ok',
                'stats': {
                    'bytes': self.bytes_len,
                    'records': {
                        'valid': records_valid,
                        'invalid': records_invalid,
                        'total': records_valid + records_invalid,
                    },
                }
            }
        })

# I decided to not end up using this as it spawns multiple write_csv_executor
# sockets = tornado.netutil.bind_sockets(8888)
# tornado.process.fork_processes(6)

async def run():
    # setup 2 ProcessPoolExecutor
    # load_json_executor will handle all the json deserialization workload
    # we'll use cpu-2 leaving 2 free cpu
    load_json_executor = futures.ProcessPoolExecutor(max_workers=os.cpu_count()-2)
    # write_csv_executor will handle all the writting to the csv file
    # writting to the same file from multiples processes would cause corruption
    # so we leave a single process as the owner of the file
    write_csv_executor = futures.ProcessPoolExecutor(max_workers=1)
    # we will leave 1 cpu free to run the tornado.web.Application

    handlers = [
        (r"/", DataReceiverHandler, {
            'load_json_executor': load_json_executor,
            'write_csv_executor': write_csv_executor,
            'loop': asyncio.get_running_loop(),
        }),
    ]
    debug = bool(sys.flags.debug)
    settings = {
        'debug': debug
    }
    application = tornado.web.Application(handlers, **settings)
    application.listen(8888, '0.0.0.0')
    # I decided to not end up using this as it spawns multiple write_csv_executor
    # http_server = tornado.httpserver.HTTPServer(application)
    # http_server.add_sockets(sockets)
    await asyncio.Event().wait()
    load_json_executor.shutdown()
    write_csv_executor.shutdown()


if __name__ == "__main__":
    asyncio.run(run())
