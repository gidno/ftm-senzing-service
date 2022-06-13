## FtM-Senzing Service
> Service for converting FtM JSONs to Senzing format and then processing them with Senzing API 

 #### Structure: 
01. ftm_mapper.py - mapper for FtM to Senzing JSON transformations could be used as standalone script or as module 
02. map_entities_script.py - script for mapping multiple JSON files from FtM to Senzing based on ftm_mapper.py (could be used on Windows an Linux OSs)
03. senzing_ftm_config.json - Senzing configuration which contains all Features and Attributes used in ftm_mapper.py
04. senzing_utils.py - module containing all SenzingAPI functions for initialization, working with Senzing configs, loading records and redoing
05. add_new_data_sources.py - script for adding new Data Sources to Senzing config from Senzing JSONs
06. load_records.py - script for loading records from Senzing JSON file via G2Loader to Senzing
07. redo_records.py - script for redoing records via G2Loader
08. senzing_init_settings.json - JSON config file containig module name, directory paths and SQL connection info for senzing_utils.py
09. G2Module.ini - ini config file which contains directory paths, path to license file and SQL connection info for Senzing
10. setupEnv - enviorment file which contains required path variables, additional libs and Java dependencies
11. requierments.txt - requirements for service usage

#### Mapper (info and standalone usage):

##### Currently supported Entities:
- Things: 
1. Person
2. Organization
3. Company
4. LegalEntity
5. PublicBody
- Intervals:
1. Directorship
2. Employment
3. Membership
4. Representation
5. UnknownLink
6. Ownership
7. Identification - only Passport
8. Address
9. Family
10. Associate

##### Standalone usage:
1. install ftm via ``` pip install followthemoney ```

2. Use ```python3 /python/G2ConfigTool.py``` inside your Senzing g2 (or project) directory to import Senzing configuration for mapper: 

```(g2cfg) importFromFile /path_to_file/senzing_ftm_config.json```

3. use ftm_mapper.py script:
```
python3 ftm_mapper.py --h
usage: ftm_mapper.py [-h] [-i INPUT_FILE] [-o OUTPUT_FILE] [-d DATA_SOURCE] [-l LOG_FILE] [-u UNK_ENTITIES]

options:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
                        A FTM .json input file.
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        output filename, defaults to input file name with a .json extension and "out_" prefix.
  -d DATA_SOURCE, --data_source DATA_SOURCE
                        Data Source name.
  -l LOG_FILE, --log_file LOG_FILE
                        optional statistics filename.
  -u UNK_ENTITIES, --unk_entities UNK_ENTITIES
                        optional bool arg (default: False), if set to True - mapper gets stats about unknown entites.
```

#### Service installation:
0. Minimum system and hardware requirements for Senzing could be found at: https://senzing.zendesk.com/hc/en-us/articles/115010259947-System-Requirements
1. Install Senzing (senzingdata and senzingapi):
> You need to perform these steps to install Senzing (Debian version):
```
sudo apt install apt-transport-https
wget https://senzing-production-apt.s3.amazonaws.com/senzingrepo_1.0.0-1_amd64.deb
sudo apt install ./senzingrepo_1.0.0-1_amd64.deb
sudo apt update
sudo apt install senzingapi
```
> More info could be found at: https://senzing.zendesk.com/hc/en-us/articles/115002408867-Quickstart-Guide
2. Put all service files into local directory (refered to as ".../ftm-senzing-service-dir/")
3. Modify your setupEnv file in .../ftm-senzing-service-dir/:
> set "SENZING_ROOT" variable as path to your Senzing g2 directory (by default thats "/opt/senzing/g2")

> set "SENZING_CONFIG_FILE" variable as path to your G2Module.ini file in .../ftm-senzing-service-dir/
4. Install, configure and tune PostgeSQL DB: 
> for DB setup you may use this guide (stop at "Configure G2Module.ini" section you would need those but you need to modify your G2Module.ini file from next step): https://senzing.zendesk.com/hc/en-us/articles/360041965973-Setup-PostgreSQL-on-Debian-Linux

> for tuning PostgreSQL DB use this guide: https://senzing.zendesk.com/hc/en-us/articles/360016288254-Tuning-Your-Database
5. Modify your G2Module.ini file in .../ftm-senzing-service-dir/:
> set "SUPPORTPATH" variable as path to your Senzing data directory (by default thats "/opt/senzing/data/3.0.0" or "/opt/senzing/data")

> set "CONFIGPATH" variable as path to your Senzing config directory (by default thats "/etc/opt/senzing")

> set "RESOURCEPATH" variable as path to your Senzing resources directory (by default thats "/opt/senzing/g2/resources")

> set "LICENSEFILE" variable as path to your Senzing license file (or remove this line if you dont have license) 

> set "CONNECTION" variable as your PostgeSQL connection setting in format "postgresql://username:password@hostname:port:database" (look step 4 guides for more info)
6. Modify your senzing_init_settings.json file in .../ftm-senzing-service-dir/:
> "SENZING_G2_DIR" variable must be equal to your "SENZING_ROOT" variable from setupEnv file from .../ftm-senzing-service-dir/

> "SENZING_DATA_DIR" variable must be equal to your "SUPPORTPATH" variable from G2Module.ini file from .../ftm-senzing-service-dir/

> "SENZING_ETC_DIR" variable must be equal to your "CONFIGPATH" variable from G2Module.ini file from .../ftm-senzing-service-dir/

> "SENZING_SQL_CONNECTION" variable must be equal to your "CONNECTION" variable from G2Module.ini file from .../ftm-senzing-service-dir/
8. Install followthemoney ```pip install followthemoney``` or just use ```pip install requirements.txt```
9. Run ```source .../ftm-senzing-service-dir/setupEnv```
10. Run ```python /python/G2ConfigTool.py -c .../ftm-senzing-service-dir/G2Module.ini``` inside your Senzing g2 directory to import Senzing configuration for mapper from senzing_ftm_config.json (use importFromFile inside G2ConfigTool)
11. Setup done))

#### Usage:
- at first you need to run ```source .../ftm-senzing-service-dir/setupEnv```

- map_entites_script.py
```
python3 map_entities_script.py --h
usage: map_entities_script.py [-h] [-f FILES_LIST [FILES_LIST ...]] [-p PATH_TO_FILES] [-d DATA_SOURCES [DATA_SOURCES ...]] [-o OUT_PATH] [-lp LOG_FILE_PATH] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -f FILES_LIST [FILES_LIST ...], --files_list FILES_LIST [FILES_LIST ...]
                        List of FULL paths to input FtM .json files (use this OR -p arg!).
  -p PATH_TO_FILES, --path_to_files PATH_TO_FILES
                        A FULL path to directory containing FtM .json input files (use this OR -f arg!).
  -d DATA_SOURCES [DATA_SOURCES ...], --data_sources DATA_SOURCES [DATA_SOURCES ...]
                        Data source names list (you must provide one data source name for each input .json file!).
  -o OUT_PATH, --out_path OUT_PATH
                        Output RELATIVE path for senzing .json files to be stored in.
  -lp LOG_FILE_PATH, --log_file_path LOG_FILE_PATH
                        optional RELATIVE path to directory to store statistics filename.
  -l LOG_FILE, --log_file LOG_FILE
                        Optional statistics filename.
```

- add_new_data_sorces.py
```
python3 add_new_data_sources.py --h
usage: add_new_data_sources.py [-h] [-p PATH_TO_FILES] [-lp LOG_FILE_PATH] [-l LOG_FILE] [-i INIT_JSON]

optional arguments:
  -h, --help            show this help message and exit
  -p PATH_TO_FILES, --path_to_files PATH_TO_FILES
                        A FULL path to directory containing senzing .json input files.
  -lp LOG_FILE_PATH, --log_file_path LOG_FILE_PATH
                        Optional RELATIVE path to directory to store statistics filename.
  -l LOG_FILE, --log_file LOG_FILE
                        Optional statistics filename.
  -i INIT_JSON, --init_json INIT_JSON
                        name of .json file wich contains senzing paths and SQL connections (if not specified, default will be used).
```

- load_records.py
```
python3 load_records.py --h
usage: load_records.py [-h] [-p PATH_TO_FILE] [-lp LOG_FILE_PATH] [-l LOG_FILE] [-t NUMBER_OF_THREADS] [-i INIT_JSON]

optional arguments:
  -h, --help            show this help message and exit
  -p PATH_TO_FILE, --path_to_file PATH_TO_FILE
                        A path to senzing .json input file.
  -lp LOG_FILE_PATH, --log_file_path LOG_FILE_PATH
                        Optional RELATIVE path to directory to store statistics filename.
  -l LOG_FILE, --log_file LOG_FILE
                        Optional statistics filename.
  -t NUMBER_OF_THREADS, --number_of_threads NUMBER_OF_THREADS
                        Optional number of threads.
  -i INIT_JSON, --init_json INIT_JSON
                        Name of .json file wich contains senzing paths and SQL connections (if not specified, default will be used).
```

- redo_records.py
```
python3 redo_records.py --h
usage: redo_records.py [-h] [-lp LOG_FILE_PATH] [-l LOG_FILE] [-t NUMBER_OF_THREADS] [-i INIT_JSON]

optional arguments:
  -h, --help            show this help message and exit
  -lp LOG_FILE_PATH, --log_file_path LOG_FILE_PATH
                        Optional RELATIVE path to directory to store statistics filename.
  -l LOG_FILE, --log_file LOG_FILE
                        Optional statistics filename.
  -t NUMBER_OF_THREADS, --number_of_threads NUMBER_OF_THREADS
                        Optional number of threads.
  -i INIT_JSON, --init_json INIT_JSON
                        Name of .json file wich contains senzing paths and SQL connections (if not specified, default will be used).
```
(note that redo_records.py script starts infinite loop and thats normal. you need this script to run in background all the time usually)

#### Checking results via G2Explorer.py:

- at first you need to run ```source .../ftm-senzing-service-dir/setupEnv```

- create snapshot using G2Snapshot.py:
>```python3 /python/G2Snapshot.py -c .../ftm-senzing-service-dir/G2Module.ini -o output_snapshot_dir/snapshot_file``` inside your Senzing g2 directory to create snapshot "snapshot_file.json" in directory "output_snapshot_dir" (use full path to directory here)

- load snapshot and explore it via G2Explorer.py:
>```python /python/G2Explorer.py -c .../ftm-senzing-service-dir/G2Module.ini -o output_snapshot_dir/snapshot_file.json``` inside your Senzing g2 directory to load snapshot "snapshot_file.json" from directory "output_snapshot_dir" (use full path to directory here)

> explore you results (you may want to look through articles about EDA from Senzing website: https://senzing.zendesk.com/hc/en-us/sections/360009388534-Exploratory-Data-Analysis-EDA-)
