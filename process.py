import time
import random
import logging
import argparse

from threading import Thread
from Queue import Queue
import Queue as Q

import dblp


HOLDINGQ = Queue(10)
READYQ = Queue()
WAIT_TIME = 2


def sequence():
    """Return a new sequence which starts from 0 and increments 1 each call."""
    counter = 0
    while True:
        yield counter
        counter += 1


class RecordProducer(Thread):
    """Read records from the data file and add them to the queue."""
    def __init__(self, fpath):
        Thread.__init__(self)
        self.fpath = fpath

    def run(self):
        global HOLDINGQ
        recordgen = dblp.iterdata(self.fpath)
        for record in recordgen:
            logging.debug("placing record in holding queue")
            HOLDINGQ.put(record)

        logging.debug("NO MORE RECORDS")


class RecordCaster(Thread):
    """Pick up records from the queue and clean/cast values as appropriate."""
    def run(self):
        global HOLDINGQ
        global READYQ
        while True:
            try:
                record = HOLDINGQ.get(True, WAIT_TIME)
            except Q.Empty:
                return 0

            logging.debug("pulled record from holding queue")
            cleaned = dblp.castrecord(record)
            READYQ.put(cleaned)
            logging.debug("placing record in ready queue")
            HOLDINGQ.task_done()


class RecordConsumer(Thread):
    """Pick up records from final queue and process into the DB."""
    def run(self):
        global READYQ
        while True:
            try:
                record = READYQ.get(True, WAIT_TIME)
            except Q.Empty:
                return 0

            logging.debug("pulled record from ready queue")
            dblp.process_record(record)
            READYQ.task_done()


def make_parser():
    parser = argparse.ArgumentParser(
        description="parse dblp data")
    parser.add_argument(
        'fpath', action='store',
        help='file to parse data from')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help="turn on verbose logging")
    parser.add_argument(
        '-vv', '--very-verbose', action='store_true',
        help='turn on very verbose logging')
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s][%(levelname)s]: %(message)s')
    elif args.very_verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(asctime)s][%(levelname)s]: %(message)s')
    else:
        logging.basicConfig(level=logging.CRITICAL)

    producer = RecordProducer(args.fpath)
    caster = RecordCaster()
    consumer1 = RecordConsumer()

    threads = [producer, caster, consumer1]

    # start all threads
    for thread in threads:
        thread.start()

    # join all threads
    for thread in threads:
        thread.join()

