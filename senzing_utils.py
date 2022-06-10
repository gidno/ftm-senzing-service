import json
import os
import sys
import time
import logging
import itertools
import concurrent.futures
from senzing import G2Exception, G2Engine, G2ConfigMgr, G2Config

# load init json data (paths and SQL connections) from file
def load_senzing_path_and_connections(senzing_init_settings_filename, log):
    
    log = logging.getLogger(log.name + ".load_senzing_path_and_connections_function")
    
    try:
        with open(senzing_init_settings_filename, "r") as fh:
            json_init_data = json.loads(fh.readline())
    except Exception as err:
        log.info('Senzing init settings JSON file can not be loaded.')
        log.info(' %s' % err)
        sys.exit(1)
    return json_init_data

# init global variables
def init_vars(json_init_data, log):
    
    log = logging.getLogger(log.name + ".init_vars_function")
    
    # init vars
    global module_name
    module_name = json_init_data['module_name']        
    global verbose_logging
    verbose_logging = False 
    # sys path
    global python_path 
    python_path = "{0}/python".format(
        os.environ.get("SENZING_G2_DIR", json_init_data['SENZING_G2_DIR']))
    sys.path.append(python_path)

# Ensure a default configuration exists
def ensure_default_config_exists(config_mgr_engine, senzing_config_json, log):
    
    log = logging.getLogger(log.name + ".ensure_default_config_exists_function")
    
    # If a Senzing default configuration does not exist in the database, create a G2Config instance to be used in creating a default configuration.
    config_id_bytearray = bytearray()
    try:
        config_mgr_engine.getDefaultConfigID(config_id_bytearray)
        if config_id_bytearray:
            log.info("Default config already set")

        else:
            log.info("No default configuration set, creating one in the Senzing repository")
            # Create a G2Config instance.

            
            g2_config = G2Config()
            try:
                g2_config.init(module_name, senzing_config_json, verbose_logging)

                # Create configuration from template file.

                config_handle = g2_config.create()

                # Save Senzing configuration to string.

                response_bytearray = bytearray()
                g2_config.save(config_handle, response_bytearray)
                senzing_model_config_str = response_bytearray.decode()

            except G2Exception as err:
                log.info(' %s' % err)

            # Externalize Senzing configuration to the database.

            config_comment = "senzing-init added at {0}".format(time.time())
            config_id_bytearray = bytearray()
            try:
                config_mgr_engine.addConfig(
                    senzing_model_config_str,
                    config_comment,
                    config_id_bytearray)

                # Set new configuration as the default.

                config_mgr_engine.setDefaultConfigID(config_id_bytearray)
            except G2Exception as err:
                log.info(' %s' % err)

    except G2Exception as err:
        log.info('Setup of def config failed')
        log.info(' %s' % err)
        sys.exit(1)

# init senzing
def init_senzing(senzing_init_settings_filename, log, ensure_default_config = False):
    
    log = logging.getLogger(log.name + ".init_senzing_function")
    
    json_init_data = load_senzing_path_and_connections(senzing_init_settings_filename, log)
    
    init_vars(json_init_data, log)
    
    # paths
    data_dir = os.environ.get("SENZING_DATA_DIR", "/opt/senzing/data/3.0.0")
    etc_dir = os.environ.get("SENZING_ETC_DIR", "/etc/opt/senzing")
    g2_dir = os.environ.get("SENZING_G2_DIR", "/opt/senzing/g2")

    # create senzing config
    config_path = etc_dir
    support_path = os.environ.get("SENZING_DATA_VERSION_DIR", data_dir)
    resource_path = "{0}/resources".format(g2_dir)

    sql_connection = os.environ.get(
        "SENZING_SQL_CONNECTION", "postgresql://senzing:1488@172.31.20.18:5432:g2")

    senzing_config_dictionary = {
        "PIPELINE": {
            "CONFIGPATH": config_path,
            "SUPPORTPATH": support_path,
            "RESOURCEPATH": resource_path
        },
        "SQL": {
            "CONNECTION": sql_connection,
        }
    }

    senzing_config_json = json.dumps(senzing_config_dictionary)

    '''# init vars
    module_name = 'pyG2'
    verbose_logging = False'''

    # init a G2ConfigMgr instance
    
    g2_configuration_manager = G2ConfigMgr()
    try:
        g2_configuration_manager.init(
            module_name,
            senzing_config_json,
            verbose_logging)

    except G2Exception as err:
        log.info('Initalization failed')
        log.info(' %s' % err)
        sys.exit(1)

    if ensure_default_config:
        ensure_default_config_exists(g2_configuration_manager, senzing_config_json, log)

    return senzing_config_json

# init G2ConfigMgr and return engine
def init_configuration_manager(senzing_config_json, log):

    log = logging.getLogger(log.name + ".init_configuration_manager_function")

    # init G2ConfigMgr
    g2_configuration_manager = G2ConfigMgr()
    try:
        g2_configuration_manager.init(
            module_name,
            senzing_config_json,
            verbose_logging)

    except G2Exception as err:
        log.info('G2ConfigMgr initalization failed')
        log.info(' %s' % err)
        sys.exit(1)
    return g2_configuration_manager

# get current default config json
def get_current_def_config(senzing_config_json, log):

    log = logging.getLogger(log.name + ".get_current_def_config_function")

    # init G2ConfigMgr
    g2_configuration_manager = init_configuration_manager(senzing_config_json, log)

    config_id_bytearray = bytearray()

    # get current defualt config
    try:
        g2_configuration_manager.getDefaultConfigID(config_id_bytearray)

    except G2Exception as err:
        log.info(' Default config ID can not be loaded.')
        log.info(' %s' % err)
        sys.exit(1)

    log.info("Default config ID loaded. Configuration ID: {0}".format(config_id_bytearray.decode()))
    response_bytearray = bytearray()
    try:
        g2_configuration_manager.getConfig(
            config_id_bytearray,
            response_bytearray)
    except G2Exception as err:
        log.info(' Default config can not be loaded.')
        log.info(' %s' % err)
        sys.exit(1)
    log.info("Default config loaded.")
    return response_bytearray

# Add data source to config
def add_data_source_to_config(data_source_names_list, senzing_config_json, log):

    log = logging.getLogger(log.name + ".add_data_source_to_config_function")

    # init G2Config
    g2_config = G2Config()
    try:
        g2_config.init(module_name, senzing_config_json, verbose_logging)
    except G2Exception as err:
        log.info('G2Config initalization failed')
        log.info(' %s' % err)
        return None # initialization failed func returns None so add_new_config() does nothing

    # Create configuration handle
    #try:
    #    config_handle = g2_config.create()
    #except G2Exception as err:
    #    log.info(' %s' % err)
    
    config_bytearray = get_current_def_config(senzing_config_json, log)
    try:
        # load default config
        config_handle = g2_config.load(config_bytearray)
    except G2Exception as err:
        log.info('Config handle loading failed')
        log.info(' %s' % err)
        return None # loading failed, func returns None so add_new_config() does nothing

    # Add Data Source
    for data_source_name in data_source_names_list:
        log.info('New data source name: ' + data_source_name.upper())
        try:
            datasource_json = "{\"DSRC_CODE\": \"" + data_source_name.upper() + "\"}" 
            response_bytearray = bytearray()
            g2_config.addDataSource(config_handle, datasource_json, response_bytearray)
            log.info('Successfully added')
        except G2Exception as err:
            log.info(' %s' % err)

    # Save
    try:
        response_bytearray = bytearray()
        g2_config.listDataSources(config_handle, response_bytearray)
        total_amount = str(len(json.loads(response_bytearray.decode())["DATA_SOURCES"]))
        log.info('Total amount of data sources: ' + total_amount)
        response_bytearray = bytearray()
        g2_config.save(config_handle, response_bytearray) 
        log.info('Config saved.')   
    except G2Exception as err:
        log.info('Config not saved')
        log.info(' %s' % err)
        sys.exit(1)
        
    senzing_model_config_json = response_bytearray.decode()
    
    # Cleanup
    # clear exception
    try:
        g2_config.clearLastException()
    except G2Exception as err:
        log.info(' %s' % err)
    # close
    try:
        g2_config.close(config_handle)
    except G2Exception as err:
        log.info(' %s' % err)
    # destroy
    try:
        g2_config.destroy()
    except G2Exception as err:
        log.info(' %s' % err)

    return senzing_model_config_json

# add new config and set it as default
def add_new_config(senzing_model_config_json, senzing_config_json, log):
    
    log = logging.getLogger(log.name + ".add_new_config_function")

    if senzing_model_config_json:
        
        # init G2ConfigMgr
        g2_configuration_manager = init_configuration_manager(senzing_config_json, log)

        config_id_bytearray = bytearray()
        
        # add new config
        config_comment = "New configuration."
        try:
            g2_configuration_manager.addConfig(
                senzing_model_config_json,
                config_comment,
                config_id_bytearray)
            log.info("New config added, ID: {0}".format(config_id_bytearray.decode()))
        except G2Exception as err:
            log.info(' %s' % err)
    
        # set as default
        try:
            g2_configuration_manager.setDefaultConfigID(config_id_bytearray)
            log.info('New configuration set as default')
        except G2Exception as err:
            log.info(' %s' % err)
    
    else:
        log.info('New config is empty')

# add data sources from each file from source_files_list - list of paths to files
def add_new_data_sources_to_config(source_files_list, senzing_init_settings_filename, log = None):
    
    if not log: # init logging if no logger provided
        logging.basicConfig(level = logging.DEBUG)
        log = logging.getLogger("add_new_data_sources_function")
    else:
        log = logging.getLogger(log.name + ".add_new_data_sources_function")
    
    try:
        # get list of data source names from first line of each file
        data_source_list = []
        log.info('Loading data source names from filenames list')
        for filename in source_files_list:
            try:
                with open(filename, "r") as fh:
                    log.info('Processing ' + filename)
                    data_source_list.append(json.loads(fh.readline())["DATA_SOURCE"])
            except Exception as err:
                log.info(' %s' % err)
        
        # if data source list not empty
        if data_source_list:
            # init senzing
            try:
                senzing_init_config_json = init_senzing(senzing_init_settings_filename, log, ensure_default_config = True)
            except Exception as err:
                log.info('Senzing initalization failed')
                log.info(' %s' % err)
                sys.exit(1)         
            
            # add new datasources to config
            try:
                add_new_config(add_data_source_to_config(data_source_list, senzing_init_config_json, log), senzing_init_config_json, log)
            except Exception as err:
                log.info('New config not added')
                log.info(' %s' % err)
                sys.exit(1)
            sys.exit(0)
    
    except Exception as err:
        log.info('Error occured!')
        log.info(' %s' % err)
        sys.exit(1)

# load 1 line function
def load_line(line, engine, config_engine, log, unprocessed_lines_file = None):

    log = logging.getLogger(log.name + '.load_line_function')
    try:    
        data_as_json = json.loads(line)
        engine.addRecord(
                data_as_json["DATA_SOURCE"],
                data_as_json["RECORD_ID"],
                line)
    
            
    except G2Exception as err:
        config_id_bytearray = bytearray()
        #log.info(' %s' % err)
        if engine.getActiveConfigID() != config_engine.getDefaultConfigID(config_id_bytearray):
            engine.reinit(config_id_bytearray)
            log.info('G2Engine reinitialised')
            data_as_json = json.loads(line)
            engine.addRecord(
                data_as_json["DATA_SOURCE"],
                data_as_json["RECORD_ID"],
                line)
        else:
            if unprocessed_lines_file:
                log.info(' Error, line recording failed ' + line)
                try:
                    unprocessed_lines_file.write(line)
                except Exception as err:
                    log.info(' %s' % err)

# process file with G2Engine
def process_file(filename, senzing_init_config_json, log, num_of_threads = 4):
    
    log = logging.getLogger(log.name + '.process_file_function')
    
    # init G2Engine with config id
    g2_engine = G2Engine()
    
    # init G2ConfigMgr
    g2_configuration_manager = init_configuration_manager(senzing_init_config_json, log)
    
    try:
        g2_engine.init(
            module_name,
            senzing_init_config_json,
            verbose_logging)

    except G2Exception as err:
        log.info('G2Engine initalization failed')
        log.info(' %s' % err)
        sys.exit(1)
        
    # add records - seems to be right
    try:
        with open(filename, "r") as fp:
            numLines = 0
        
            with concurrent.futures.ThreadPoolExecutor(num_of_threads) as executor:
                futures = {executor.submit(load_line, line, g2_engine, g2_configuration_manager, log): line for line in itertools.islice(fp, executor._max_workers)}
            
                while futures:
                    done, futures = concurrent.futures.wait(futures, return_when = concurrent.futures.FIRST_COMPLETED)
            
                    for fut in done:                
                        numLines += 1
                        if numLines % 1000 == 0:
                            log.info(f'Processed {numLines} loads')
            
                    for line in itertools.islice(fp, len(done)):
                        futures.add(executor.submit(load_line, line, g2_engine, g2_configuration_manager, log))
        log.info('Load records process success')

    except Exception as err:
        log.info('Records not loaded')
        log.info(' %s' % err)
        sys.exit(1)
    
    # destroy G2Engine
    try:
        g2_engine.destroy()

    except G2Exception as err:
        log.info(g2_engine.getLastException)

# load records from senzing JSON file
def load_records_to_senzing(source_file, senzing_init_settings_filename, log = None, num_of_threads = 4):
       
    if not log: # init logging if no logger provided
        logging.basicConfig(level = logging.DEBUG)
        log = logging.getLogger("load_records_to_senzing_function")
    else:
        log = logging.getLogger(log.name + '.load_records_to_senzing_function')
    
    log.info('Loading records from file: ' + source_file)

    try:
        
        # init senzing
        senzing_init_config_json = init_senzing(senzing_init_settings_filename, log)
    
        # process entities with G2Engine
        process_file(source_file, senzing_init_config_json, log, num_of_threads)

        log.info('File loaded!')

    except Exception as err:
        log.info(' %s' % err)
        sys.exit(1)

# redo 1 record function
def redo_record(response_bytearray, engine, config_engine, log):
    
    log = logging.getLogger(log.name + '.redo_record_function')
    
    try:
        engine.processRedoRecord(response_bytearray)
    except G2Exception as err:
        config_id_bytearray = bytearray()
        if engine.getActiveConfigID() != config_engine.getDefaultConfigID(config_id_bytearray):
            engine.reinit(config_id_bytearray)
            log.info('G2Engine reinitialised')
        else:
            log.info(' %s' % err)

# redo records process
def process_redo_records(senzing_init_settings_filename, log = None, num_of_threads = 4):
    
    if not log: # init logging if no logger provided
        logging.basicConfig(level = logging.DEBUG)
        log = logging.getLogger("process_redo_records_function")
    else:
        log = logging.getLogger(log.name + '.process_redo_records_function')

    # init senzing
    senzing_init_config_json = init_senzing(senzing_init_settings_filename, log) 
    
    # init G2Engine with config id
    g2_engine = G2Engine()

    # init G2ConfigMgr
    g2_configuration_manager = init_configuration_manager(senzing_init_config_json, log)

    try:
        g2_engine.init(
            module_name,
            senzing_init_config_json,
            verbose_logging)
        log.info(" G2Engine init success")
    except G2Exception as err:
        log.info('G2Engine initalization failed')
        log.info(' %s' % err)
        sys.exit(1)
    log.info('There are ' + str(g2_engine.countRedoRecords()) + ' records for redoing for now' )
    # redo records - infinite loop
    numLines = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(num_of_threads) as executor:
            futures = {executor.submit(redo_record, response_bytearray, g2_engine, g2_configuration_manager, log): response_bytearray for response_bytearray in [bytearray()] * executor._max_workers} # dont sure, especially about "[bytearray()] * executor._max_workers" part
            while True:
                done, futures = concurrent.futures.wait(futures, return_when = concurrent.futures.FIRST_COMPLETED)
                
                for fut in done:                
                    numLines += 1
                    if numLines%1000 == 0:
                        log.info(f'Processed {numLines} redos')
                if g2_engine.countRedoRecords():               
                    for i in range(0, len(done)):
                        futures.add(executor.submit(redo_record, bytearray(), g2_engine, g2_configuration_manager, log))
                else:
                    if len(futures) == 0:
                        time.sleep(10) # if no new and all done
        
    except Exception as err:
        log.info('Redo process init failed')
        log.info(' %s' % err)