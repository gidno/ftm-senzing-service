import logging
import os
import argparse
import sys
from datetime import datetime
from senzing_utils import process_redo_records

if __name__ == "__main__":
    start_time = datetime.now()
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-lp', '--log_file_path', default=os.getenv('log_file_path', None), type=str, help='Optional RELATIVE path to directory to store statistics filename.')
    argparser.add_argument('-l', '--log_file', default=os.getenv('log_file', None), type=str, help='Optional statistics filename.')
    argparser.add_argument('-t', '--number_of_threads', default=os.getenv('number_of_threads', 4), type=int, help='Optional number of threads.')
    argparser.add_argument('-i', '--init_json', default=os.getenv('init_json', 'senzing_init_settings.json'), type=str, help='Name of .json file wich contains senzing paths and SQL connections (if not specified, default will be used).')
    args = argparser.parse_args()
    log_file_path = args.log_file_path
    log_file = args.log_file
    number_of_threads = args.number_of_threads
    init_json = args.init_json
    if log_file:
        if log_file_path:
            if os.path.isdir(os.getcwd() + log_file_path):
                log_file = os.getcwd() + log_file_path + log_file
            else:
                try:
                    os.mkdir(os.getcwd() + log_file_path)   
                    log_file = os.getcwd() + log_file_path + log_file
                except: 
                    print('')
                    print('Incorrect log file directory path. storing log file in cwd.')
                    print('')
        logging.basicConfig(filename = log_file,
                            filemode = 'a',
                            format = '%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt = '%H:%M:%S',
                            level = logging.DEBUG)
    else:
        logging.basicConfig(level = logging.DEBUG)
    
    log = logging.getLogger("redo_records_script")
        
    try:
        process_redo_records(init_json, log, number_of_threads)
        
    except Exception as err:
        log.info('Error occured! Time spent: ' + str(datetime.now()-start_time))
        log.info(' %s' % err)
        sys.exit(1)