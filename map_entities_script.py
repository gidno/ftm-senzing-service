import logging
import os
import argparse
import sys
from datetime import datetime
import shutil
from ftm_mapper import process_entities

def map_entites(source_files_list, data_sources_list, out_path, log = None):
    # init logging if no logger provided
    if not log:
        logging.basicConfig(level = logging.DEBUG)
        log = logging.getLogger("map_entities_function")
    else:
        log = logging.getLogger(log.name + ".map_entities_function")

    output_paths_list = []
    
    try:
        for i, input_file_name in enumerate(source_files_list):
            log.info('Processing file ' + input_file_name)
            try:
                output = process_entities(data_sources_list[i], input_file_name, log)
                temp_file_name = output.name
                output.close()
                output_file_name = 'out_' + os.path.basename(input_file_name)
                shutil.copy(temp_file_name, out_path + output_file_name)
                log.info('Saved as ' + out_path + output_file_name)
                os.remove(temp_file_name)
                output_paths_list.append(out_path + output_file_name)
            except Exception as err:
                log.info(' %s' % err)
        return output_paths_list
    
    except Exception as err:
        log.info(' %s' % err)
        return None

if __name__ == "__main__":
    start_time = datetime.now()
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f','--files_list',nargs='+', default=os.getenv('files_list', None), type=str, help='List of FULL paths to input FtM .json files (use this OR -p arg!).')
    argparser.add_argument('-p', '--path_to_files', default=os.getenv('path_to_files', None), type=str, help='A FULL path to directory containing FtM .json input files (use this OR -f arg!).')
    argparser.add_argument('-d','--data_sources',nargs='+', default=os.getenv('data_sources', None), type=str, help='Data source names list (you must provide one data source name for each input .json file!).')
    argparser.add_argument('-o', '--out_path', default=os.getenv('out_path', None), type=str, help='Output RELATIVE path for senzing .json files to be stored in.')
    argparser.add_argument('-lp', '--log_file_path', default=os.getenv('log_file_path', None), type=str, help='optional RELATIVE path to directory to store statistics filename.')
    argparser.add_argument('-l', '--log_file', default=os.getenv('log_file', None), type=str, help='Optional statistics filename.')
    args = argparser.parse_args()
    files_list = args.files_list
    path_to_files = args.path_to_files
    data_sources = args.data_sources
    out_path = args.out_path
    log_file_path = args.log_file_path
    log_file = args.log_file

    if (not files_list) and (not path_to_files):
        print('')
        print('You neeed to specify a path to directory containig FtM .json input files OR list of paths to input FtM .json input files.')
        print('')
        sys.exit(1)
    
    if files_list and path_to_files:
        print('')
        print('Path to directory containig FtM .json input files AND list of paths to input FtM .json input files BOTH provided, only list of paths will be used though.')
        print('')

    if not files_list:
        if not os.path.isdir(path_to_files):
            print('')
            print('Incorrect path. Please provide correct path to directory OR list of paths to input FtM .json input files instead.')
            print('')
            sys.exit(1)

        if (sys.platform == "linux") or (sys.platform == "linux2"):
            files_list = [path_to_files +'/' + filename for filename in os.listdir(path_to_files)]
        
        elif sys.platform == "win32":
            files_list = [path_to_files +'\\' + filename for filename in os.listdir(path_to_files)]

        '''elif sys.platform == "darwin":
            files_list = [path_to_files +'/' + filename for filename in os.listdir(path_to_files)]
        '''
        
    
    if not data_sources:
        print('')
        print('Please provide a list of data source names.')
        print('')
        sys.exit(1)

    if len(files_list) != len(data_sources):
        print('')
        print('Number of FtM .json input files NOT equal to number of data sources provided.')
        print('')
        sys.exit(1)               
    
    if not out_path:
        print('')
        print('You need to specify output path for Senzing .json files to be stored in.')
        print('')
        sys.exit(1)   

    if not os.path.isdir(os.getcwd() + out_path):
        try:
            os.mkdir(os.getcwd() + out_path)       
        except:
            print('')
            print('Incorrect output directory path. Please provide correct path to output directory instead.')
            print('')
            sys.exit(1)            

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
    
    log = logging.getLogger("map_entities_script")
        
    try:
        map_entites(files_list, data_sources, os.getcwd() + out_path, log)
        log.info('All files processed. Time spent: ' + str(datetime.now()-start_time))
        sys.exit(0)

    except Exception as err:
        log.info('Error occured! Time spent: ' + str(datetime.now()-start_time))
        log.info(' %s' % err)
        sys.exit(1)