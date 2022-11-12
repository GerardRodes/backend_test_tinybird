import csv
import io
import json
import sys
import tornado.ioloop
import tornado.web

from datetime import date


@tornado.web.stream_request_body
class DataReceiverHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.request_body = b''

    def data_received(self, chunk):
        self.request_body += chunk

    def post(self):
        records_valid = 0
        records_invalid = 0
        with open(f"csv/nyc_taxi-{date.today()}.csv", 'a') as fw:
            fieldnames = [
                'vendorid',
                'tpep_pickup_datetime',
                'trip_distance',
                'total_amount',
            ]
            writer = csv.DictWriter(fw, fieldnames, extrasaction='ignore')
            fr = io.BytesIO(self.request_body)
            for record in fr.readlines():
                try:
                    row = json.loads(record)
                    writer.writerow(row)
                    records_valid += 1
                except Exception:
                    records_invalid += 1
        result = {
            'result': {
                'status': 'ok',
                'stats': {
                    'bytes': len(self.request_body),
                    'records': {
                        'valid': records_valid,
                        'invalid': records_invalid,
                        'total': records_valid + records_invalid,
                    },
                }
            }
        }
        self.write(result)


def run():
    handlers = [
        (r"/", DataReceiverHandler),
    ]
    debug = bool(sys.flags.debug)
    settings = {
        'debug': debug
    }
    port = 8888
    address = '0.0.0.0'
    application = tornado.web.Application(handlers, **settings)
    application.listen(port, address)
    print(f"server listening at {address}:{port} debug={debug}")
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    run()
