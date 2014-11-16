import logging
import argparse

from multiprocessing import Process, JoinableQueue
import Queue as Q

import dblp


WAIT_TIME = 2


def read_records(fpath, holdingq):
    logging.debug("starting to read records")
    recordgen = dblp.iterdata(fpath)
    for record in recordgen:
        logging.debug("placing record in holding queue")
        holdingq.put(record)
    logging.debug("NO MORE RECORDS")


def cast_records(holdingq, readyq):
    logging.debug("starting to cast records")
    while True:
        try:
            record = holdingq.get(True, WAIT_TIME)
        except Q.Empty:
            return 0

        logging.debug("pulled record from holding queue")
        casted = dblp.castrecord(record)
        readyq.put(casted)
        logging.debug("placing record in ready queue")
        holdingq.task_done()


def process_records(readyq):
    logging.debug("starting to process records")
    while True:
        try:
            record = readyq.get(True, WAIT_TIME)
        except Q.Empty:
            return 0

        logging.debug("pulled record from ready queue")
        dblp.process_record(record)
        readyq.task_done()


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

    logging.info("creating work queues")
    holdingq = JoinableQueue(10)
    readyq = JoinableQueue()

    producer = Process(target=read_records, args=(args.fpath, holdingq))
    caster = Process(target=cast_records, args=(holdingq, readyq))
    consumer = Process(target=process_records, args=(readyq,))

    procs = [producer, caster, consumer]

    # start all processes
    logging.info(
        "starting %d threads to process records in %s" % (
            len(procs), args.fpath))
    for proc in procs:
        proc.start()

    # join all processes
    for proc in procs:
        proc.join()
