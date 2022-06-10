import logging
import os
import argparse
import sys
from datetime import datetime
from senzing_utils import add_new_data_sources_to_config

if __name__ == "__main__":
    start_time = datetime.now()
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-p', '--path_to_files', default=os.getenv('path_to_files', None), type=str, help='A FULL path to directory containing senzing .json input files.')
    argparser.add_argument('-lp', '--log_file_path', default=os.getenv('log_file_path', None), type=str, help='Optional RELATIVE path to directory to store statistics filename.')
    argparser.add_argument('-l', '--log_file', default=os.getenv('log_file', None), type=str, help='Optional statistics filename.')
    argparser.add_argument('-i', '--init_json', default=os.getenv('init_json', 'senzing_init_settings.json'), type=str, help='name of .json file wich contains senzing paths and SQL connections (if not specified, default will be used).')
    args = argparser.parse_args()
    path_to_files = args.path_to_files
    log_file_path = args.log_file_path
    log_file = args.log_file
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
    
    log = logging.getLogger("add_new_data_sources_script")
    if not path_to_files:
        print('')
        print('Please chose a path to directory containig Senzing .json input files.')
        print('')
        sys.exit(1)
    else:
        if not os.path.isdir(path_to_files):
            print('')
            print('Incorrect path. Please provide correct path to directory OR list of paths to input FtM .json input files instead.')
            print('')
            sys.exit(1)
        try:
            if (sys.platform == "linux") or (sys.platform == "linux2"):
                source_files_list = [path_to_files +'/' + filename for filename in os.listdir(path_to_files)]
            elif sys.platform == "win32":
                source_files_list = [path_to_files +'\\' + filename for filename in os.listdir(path_to_files)]
            '''elif sys.platform == "darwin":
                source_files_list = [path_to_files +'/' + filename for filename in os.listdir(path_to_files)]
            '''
        except Exception as err:
            log.info('Source files list not loaded!')
            log.info(' %s' % err)
            sys.exit(1)
    
    try:
        if source_files_list:
            add_new_data_sources_to_config(source_files_list, init_json, log)
            log.info('Success. Time spent: ' + str(datetime.now()-start_time))
        else:
            log.info('Source files list is empty. Time spent: ' + str(datetime.now()-start_time))
        sys.exit(0)

    except Exception as err:
        log.info('Error occured! Time spent: ' + str(datetime.now()-start_time))
        log.info(' %s' % err)
        sys.exit(1)
