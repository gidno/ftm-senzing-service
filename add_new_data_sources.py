import json
import os
import sys
import time
import logging
from senzing import G2ConfigMgr, G2Exception, G2Config

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

    except G2Exception.G2ModuleGenericException as err:
        log.info('Initalization failed')
        log.info(' %s' % err)
        sys.exit(1)

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

# Add data source to config
def add_data_source_to_config(data_source_names_list, senzing_config_json, log):

    # init G2Config
    g2_config = G2Config()
    try:
        g2_config.initV2(module_name, senzing_config_json, verbose_logging)
    except G2Exception.G2ModuleGenericException as err:
        log.info('G2Config initalization failed')
        log.info(' %s' % err)
        return None # initialization failed func returns None so add_new_config() does nothing

    # Create configuration handle
    try:
        config_handle = g2_config.create()
    except G2Exception.G2ModuleGenericException as err:
        log.info(' %s' % err)

    # Add Data Source
    for data_source_name in data_source_names_list:
        log.info('New data source name: ' + data_source_name.upper())
        try:
            datasource_json = "{\"DSRC_CODE\": \"" + data_source_name.upper() + "\"}" 
            response_bytearray = bytearray()
            g2_config.addDataSourceV2(config_handle, datasource_json, response_bytearray)
            log.info('Successfully added')
        except G2Exception.G2ModuleGenericException as err:
            log.info(' %s' % err)

    # Save
    try:
        response_bytearray = bytearray()
        g2_config.save(config_handle, response_bytearray)    
    except G2Exception.G2ModuleGenericException as err:
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
    
    if senzing_model_config_json:
        # init
        g2_configuration_manager = G2ConfigMgr()
        try:
            g2_configuration_manager.init(
                module_name,
                senzing_config_json,
                verbose_logging)

        except G2Exception as err:
            log.info('G2ConfigMgr initalization failed')
            log.info(' %s' % err)

        # add new config
        config_comment = "New configuration."
        config_id_bytearray = bytearray()
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
    
# add data sources from each file from source_files_list - list of paths to files
def add_new_datasources_to_config(source_files_list, log = None):
    
    # init logging if no logger provided
    if not log:
        logging.basicConfig(level = logging.DEBUG)
        log = logging.getLogger("add_new_datasources_script")

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
                senzing_init_config_json = init_senzing(log)
            except Exception as err:
                log.info('Senzin initalization failed')
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

