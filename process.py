import logging
import argparse
import os
import sys

from datetime import datetime
import time
from dateutil.tz import gettz

import glob
import pandas as pd

from functools import partial
from multiprocessing import Pool
import json
from config import MAX_PROCESSES, FILENAME_APPLICANT_EMPLOYER,\
    FILENAME_APPLICANT_EMPLOYER_NATIONALITY, PATH_RIGHT_TO_WORK, \
    PATH_IDENTITY, OUTPUT_FILEPATH, LOG_FILE

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
log = logging.getLogger('main')
log.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
hdlr = logging.FileHandler(LOG_FILE)
log.addHandler(sh)
log.addHandler(hdlr)


EMPLOYERS = pd.read_json(FILENAME_APPLICANT_EMPLOYER).\
    set_index(0).T.to_dict('index')[1]
NATIONALITY = pd.read_json(FILENAME_APPLICANT_EMPLOYER_NATIONALITY).\
    set_index(0).T.to_dict('index')[1]


def _produce_output(file):
    """
    Extract and Process checks

    :param file: filename date wise
    :raise Exception
    """
    try:
        log.info('Hour %s ETL start.', os.path.basename(file))
        start_time = time.time()
        if (EMPLOYERS or NATIONALITY) is None:
            raise Exception(
                'There is either no employer or nationality information found'
            )

        df_right_to_work = pd.read_csv(file)
        if df_right_to_work.empty:
            return log.error('Error: %s file has no data', file)

        df_right_to_work['applicant_employer'] = \
            df_right_to_work['applicant_employer'].map(EMPLOYERS)
        df_right_to_work['applicant_nationality'] = \
            df_right_to_work['applicant_nationality'].map(NATIONALITY)

        df_identity = pd.read_csv(PATH_IDENTITY + os.path.basename(file))
        if df_identity.empty:
            return log.error('Error: %s file has no data', file)

        df_joined = pd.merge(df_right_to_work, df_identity,
                             on=['applicant_id'], how='left')
        df_joined['iso8601_timestamp'] = df_joined['unix_timestamp_x'].\
            apply(
            lambda item: datetime.fromtimestamp(
                item, gettz("Europe/London")
            ).strftime('%Y-%m-%dT%H:%M:%S')
        )

        df_final = df_joined[['iso8601_timestamp', 'applicant_id',
                              'applicant_employer', 'applicant_nationality',
                              'is_eligble', 'is_verified']]
        output_filename = OUTPUT_FILEPATH + os.path.\
            splitext(os.path.basename(file))[0] + '.json'

        # Has to go with simple write as we dont want NULL values
        with open(output_filename, 'w') as fp:
            for index, row in df_final.iterrows():
                json.dump({**row.dropna().to_dict()}, fp) # noqa

        # more neater way in comment with valid json, if values can be null
        # df_final.to_json(output_filename, orient='records')

        elapsed_time = time.time() - start_time
        log.info('Hour %s ETL complete, elapsed time: %s',
                 os.path.basename(file), elapsed_time)
    except Exception as e:
        log.error('Hour %s ETL Error %s', os.path.basename(file), e)


def main():
    """Read CSV files and distribute to workers."""
    parser = argparse.ArgumentParser(description='Process checks ETL.')
    parser.add_argument('-p', type=int, default=MAX_PROCESSES,
                        help='an integer for number of workers')
    args = parser.parse_args()
    log.info('Starting job with %s workers', args.p)
    files = [file for file in glob.glob(PATH_RIGHT_TO_WORK + '*.csv')]
    if not files:
        raise Exception('There are no files to process')

    with Pool(processes=int(args.p)) as pool:
        pool.map(
            partial(
                _produce_output), files)


if __name__ == '__main__':
    main()
