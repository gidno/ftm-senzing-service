import json
import os
import sys
import time
import logging
import itertools
import concurrent.futures
from senzing import G2Exception, G2Engine, G2ConfigMgr, G2Config

# init vars
module_name = 'pyG2'       
verbose_logging = False 
# sys path
python_path = "{0}/python".format(
    os.environ.get("SENZING_G2_DIR", "/opt/senzing/g2"))
sys.path.append(python_path)

# init senzing
def init_senzing(log):
    # paths
    data_dir = os.environ.get("SENZING_DATA_DIR", "/opt/senzing/data")
    etc_dir = os.environ.get("SENZING_ETC_DIR", "/etc/opt/senzing")
    g2_dir = os.environ.get("SENZING_G2_DIR", "/opt/senzing/g2")

    # create senzing config
    config_path = etc_dir
    support_path = os.environ.get("SENZING_DATA_VERSION_DIR", data_dir)
    resource_path = "{0}/resources".format(g2_dir)

    sql_connection = os.environ.get(
        "SENZING_SQL_CONNECTION", "sqlite3://na:na@/var/opt/senzing/sqlite/G2C.db")

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

    # init a G2ConfigMgr instance
    g2_configuration_manager = G2ConfigMgr()
    try:
        g2_configuration_manager.init(
            module_name,
            senzing_config_json,
            verbose_logging)

    except G2Exception.G2ModuleGenericException as err:
        log.info(' %s' % err)

    # Ensure a default configuration exists
    # If a Senzing default configuration does not exist in the database, create a G2Config instance to be used in creating a default configuration.
    config_id_bytearray = bytearray()
    try:
        g2_configuration_manager.getDefaultConfigID(config_id_bytearray)
        if config_id_bytearray:
            log.info("Default config already set")

        else:
            log.info("No default configuration set, creating one in the Senzing repository")
            # Create a G2Config instance.

            
            g2_config = G2Config()
            try:
                g2_config.initV2(module_name, senzing_config_json, verbose_logging)

                # Create configuration from template file.

                config_handle = g2_config.create()

                # Save Senzing configuration to string.

                response_bytearray = bytearray()
                g2_config.save(config_handle, response_bytearray)
                senzing_model_config_str = response_bytearray.decode()

            except G2Exception.G2ModuleGenericException as err:
                log.info(' %s' % err)

            # Externalize Senzing configuration to the database.

            config_comment = "senzing-init added at {0}".format(time.time())
            config_id_bytearray = bytearray()
            try:
                g2_configuration_manager.addConfig(
                    senzing_model_config_str,
                    config_comment,
                    config_id_bytearray)

                # Set new configuration as the default.

                g2_configuration_manager.setDefaultConfigID(config_id_bytearray)
            except G2Exception.G2ModuleGenericException as err:
                log.info(' %s' % err)

    except G2Exception.G2ModuleGenericException as err:
        log.info(' %s' % err)

    return senzing_config_json

# load 1 line function
def load_line(line, engine, log):
    try:    
        data_as_json = json.loads(line)
        try:
            engine.addRecord(
                    data_as_json["DATA_SOURCE"],
                    data_as_json["RECORD_ID"],
                    data_as_json)
    
        except G2Exception as err:
            log.info(engine.getLastException())
            
    except Exception as err:
        log.info(' %s' % err)

# redo 1 record function
def redo_record(response_bytearray, engine, log):
    try:
        engine.processRedoRecord(response_bytearray)
    except G2Exception as err:
            log.info(engine.getLastException())   
            
# process file with G2Engine
def process_file(filename, senzing_init_config_json, log):
    
    # init G2Engine with config id
    g2_engine = G2Engine()
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
        
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(load_line, g2_engine, line, log): line for line in itertools.islice(fp, executor._max_workers)}
            
                while futures:
                    done, futures = concurrent.futures.wait(futures, return_when = concurrent.futures.FIRST_COMPLETED)
            
                    for fut in done:                
                        numLines += 1
                        if numLines%1000 == 0:
                            log.info(f'Processed {numLines} loads')
            
                    for line in itertools.islice(fp, len(done)):
                        futures.add(executor.submit(load_line, g2_engine, line, log))
        log.info('Load records process success')
    except Exception as err:
        log.info('Records not loaded')
        log.info(' %s' % err)
        sys.exit(1)
    # redo records - may be wrong
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(redo_record, response_bytearray, log): response_bytearray for response_bytearray in [bytearray()] * executor._max_workers} # dont sure, especially about "[bytearray()] * executor._max_workers" part
            while futures:
                done, futures = concurrent.futures.wait(futures, return_when = concurrent.futures.FIRST_COMPLETED)
                
                for fut in done:                
                    numLines += 1
                    if numLines%1000 == 0:
                        log.info(f'Processed {numLines} redos')
                
                if g2_engine.countRedoRecords(): # really dont sure about this, I dont know how fast countRedoRecords() is but I need this whole thing to stop at some moment, maybe I need to use something like response_bytearray list and than use something like any() or use fut.result() and check if its not 0, dont know for sure how that's must be done 
                    for i in range(0, len(done)):
                        futures.add(executor.submit(redo_record, bytearray(), log))
        log.info('Redo process succes')
    except Exception as err:
        log.info('Redo process not done')
        log.info(' %s' % err)
    # destroy G2Engine
    try:
        g2_engine.destroy()

    except G2Exception as err:
        log.info(g2_engine.getLastException)

# load records from senzing JSON file
def load_records_to_senzing(source_file, log = None):
    
    # init logging if no logger provided
    if not log:
        logging.basicConfig(level = logging.DEBUG)
        log = logging.getLogger("add_new_datasources_script")

    try:
        
        # init senzing
        senzing_init_config_json = init_senzing(log)
    
        # process entities with G2Engine
        process_file(source_file, senzing_init_config_json, log)

        log.info('File loaded!')
        sys.exit(0)
    except Exception as err:
        log.info(' %s' % err)
        sys.exit(1)