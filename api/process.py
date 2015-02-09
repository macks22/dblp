import logging
import argparse

from multiprocessing import Process, JoinableQueue
import Queue as Q

import dblp


WAIT_TIME = 5


def read_records(f, queue):
    logging.debug("starting to read records")
    record = dblp.nextrecord(f)
    while record is not None:
        logging.debug("placing record in holding queue")
        queue.put(record)
        record = dblp.nextrecord(f)
    logging.debug("NO MORE RECORDS")


def process_records(queue):
    logging.debug("starting to process records")
    while True:
        try:
            record = queue.get(True, WAIT_TIME)
        except Q.Empty:
            return 0

        logging.debug("pulled record from ready queue")
        try:
            dblp.process_record(record)
        except Exception as e:
            print e
        queue.task_done()


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

    logging.info("setting up...")
    queue = JoinableQueue(20)
    f = open(args.fpath)

    producer1 = Process(target=read_records, args=(f, queue))
    consumer1 = Process(target=process_records, args=(queue,))
    consumer2 = Process(target=process_records, args=(queue,))

    procs = [
        producer1,
        consumer1,
        consumer2
    ]

    # start all processes
    logging.info(
        "starting %d threads to process records in %s" % (
            len(procs), args.fpath))
    for proc in procs:
        proc.start()

    # join all processes
    for proc in procs:
        proc.join()
