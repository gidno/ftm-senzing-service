from ftm_mapper import process_entities
import logging
import os
import shutil

def map_entites(source_files_list, data_sources_list, out_path = None, log = None):
    # init logging if no logger provided
    if not log:
        logging.basicConfig(level = logging.DEBUG)
        log = logging.getLogger("add_new_datasources_script")

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