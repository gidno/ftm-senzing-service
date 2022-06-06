import json
import logging
from typing import Dict, List
import re
from datetime import datetime
from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry
from followthemoney import helpers
import argparse
import os
import sys
import tempfile
import shutil

# to get unknown entities
def catch_unk_entities(source_file, thing_entity_list, intervals_entity_list, OTHER_ENTITIES, log):
    start_time = datetime.now()
    known_types_list = thing_entity_list + intervals_entity_list
    with open(source_file, "r") as fh:
        while line := fh.readline():
            data = json.loads(line)
            entity = model.get_proxy(data)
            if not entity.schema.name in known_types_list:
                OTHER_ENTITIES.append(entity)
    log.info('found ' + str(len(OTHER_ENTITIES)) +' unknown entities in source file, time spent: ' + str(datetime.now()-start_time))

# for reading known entities
def read_entities (source_file, thing_entity_list, intervals_entity_list, 
                   DIRECTORSHIPS, EMPLOYMENTS, MEMBERSHIPS, REPRESENTATIONS, UNKNOWN_LINKS,  OWNERSHIPS, IDENTIFICATIONS, ADDRESSES, FAMILIES, ASSOCIATIONS, log):
    start_time = datetime.now()
    # Interval entities first - stored in interval Dicts
    log.info("Caching aux entities: %r", source_file)
    with open(source_file, "r") as fh:
        while line := fh.readline():
            data = json.loads(line)
            entity = model.get_proxy(data)
            if entity.schema.name in intervals_entity_list:
                if entity.schema.is_a("Succession"):
                    continue # will be addeed later
                elif entity.schema.is_a("Directorship"):
                    for director in entity.get("director"):
                        if director not in DIRECTORSHIPS:
                            DIRECTORSHIPS[director] = []
                        DIRECTORSHIPS[director].append(entity)
                elif entity.schema.is_a("Employment"):
                    for employer in entity.get("employer"):
                        if employer not in EMPLOYMENTS:
                            EMPLOYMENTS[employer] = []
                        EMPLOYMENTS[employer].append(entity)
                elif entity.schema.is_a("Membership"):
                     for member in entity.get("member"):
                        if member not in MEMBERSHIPS:
                            MEMBERSHIPS[member] = []
                        MEMBERSHIPS[member].append(entity)   
                elif entity.schema.is_a("Representation"):
                    for agent in entity.get("agent"):
                        if agent not in REPRESENTATIONS:
                            REPRESENTATIONS[agent] = []
                        REPRESENTATIONS[agent].append(entity)
                elif entity.schema.is_a("UnknownLink"):
                    for subject in entity.get("subject"):
                        if subject not in UNKNOWN_LINKS:
                            UNKNOWN_LINKS[subject] = []
                        UNKNOWN_LINKS[subject].append(entity)
                elif entity.schema.is_a("Ownership"):
                    for owner in entity.get("owner"):
                        if owner not in OWNERSHIPS:
                            OWNERSHIPS[owner] = []
                        OWNERSHIPS[owner].append(entity)
                elif entity.schema.is_a("Identification"):
                    for holder in entity.get("holder"):
                        if holder not in IDENTIFICATIONS:
                            IDENTIFICATIONS[holder] = []
                        IDENTIFICATIONS[holder].append(entity)   
                elif entity.schema.is_a("Address"):
                    ADDRESSES[entity.id] = entity
                elif entity.schema.is_a("Family"):
                    for person in entity.get("person"):
                        if person not in FAMILIES:
                            FAMILIES[person] = []
                        FAMILIES[person].append(entity)
                elif entity.schema.is_a("Associate"):
                    for person in entity.get("person"):
                        if person not in ASSOCIATIONS:
                            ASSOCIATIONS[person] = []
                        ASSOCIATIONS[person].append(entity)
    log.info("all intervals cached, time spent: " + str(datetime.now()-start_time))
    # Thing entities
    log.info("Reading entities: %r", source_file)
    with open(source_file, "r") as fh:
        while line := fh.readline():
            data = json.loads(line)
            entity = model.get_proxy(data)
            if entity.schema.name in thing_entity_list:
                yield entity

# get only one attribute     
def get_attribute(entity, prop, attr, conc = False):
    value = entity.get(prop, quiet=True)
    if value:
        if len(value) > 1:
            if conc: # concatenation of all values
                res = ''
                for val in value:
                    res += val + ' '
                return {attr: res}
            else: # only first value
                return {attr: value[0]}
        else: # only first (and only) value
            return {attr: value[0]}
    else: # empty list
        return {attr: value}

# get only attributes_list
def get_attribute_list(entity, props, attr):
    values = []
    for prop in props:
        values += entity.get(prop, quiet=True)
    list_name = attr + '_LIST'
    return {list_name:[{attr:value} for value in values]}

# convert time to list of date and time from iso (not used for now, but maybe might be helpful for some data)
def split_time(timestring):
    return [v for v in re.findall(r'[^T]*', timestring) if v != '']

# helper function for some data, splits WeakALias FtM attribute on '\n' symbol
def weak_alias_split(weak_alias):
    return weak_alias.split('\n')

# for disclosed relationships
def create_disclosed_relashionships(data_source, relationship_list, intervals_dict, entity_id, role_name, subj_name, anchor):
        for adj in intervals_dict.get(entity_id, []):
            for role in adj.get(role_name, quiet = True):
                if not anchor:
                    relationship_list += [{
                        "REL_ANCHOR_DOMAIN": data_source,
                        "REL_ANCHOR_KEY": entity_id}]
                    anchor = True
                for subject in set(adj.get(subj_name, quiet = True)):
                    relationship_list += [{
                        "REL_POINTER_DOMAIN": data_source,
                        "REL_POINTER_KEY": subject,
                        "REL_POINTER_ROLE": role
                        }]
        return  relationship_list, anchor

# map_record function        
def transform(data_source: str, entity: EntityProxy,
                   DIRECTORSHIPS, EMPLOYMENTS, MEMBERSHIPS, REPRESENTATIONS, UNKNOWN_LINKS,  OWNERSHIPS, IDENTIFICATIONS, ADDRESSES, FAMILIES, ASSOCIATIONS, alias_split = True):
    entity = helpers.simplify_provenance(entity) # for removing prefix dates and so on
    record = {
        "DATA_SOURCE": data_source,
        "RECORD_ID": entity.id,
    }
    is_org = False
    if entity.schema.name == "Person":
        record["RECORD_TYPE"] = "PERSON"
        addr_type  =     "HOME"
        name_field =     "NAME_FULL"
        attr = get_attribute_list(entity, ["country"], "CITIZENSHIP")
        if attr["CITIZENSHIP_LIST"]:
            if len(attr["CITIZENSHIP_LIST"]) > 1:
                record.update(attr)
            else:
                record.update(attr["CITIZENSHIP_LIST"][0])
    elif entity.schema.name in ("Organization", "Company", "PublicBody"):
        record["RECORD_TYPE"] = "ORGANIZATION"
        addr_type  =    "BUSINESS"
        name_field =    "NAME_ORG"
        attr = get_attribute_list(entity, ["mainCountry"], "REGISTRATION_COUNTRY")
        if attr["REGISTRATION_COUNTRY_LIST"]:
            if len(attr["REGISTRATION_COUNTRY_LIST"]) > 1:
                record.update(attr)
            else:
                record.update(attr["REGISTRATION_COUNTRY_LIST"][0])
        is_org = True
    else:
        addr_type = "LEGAL"
        name_field = "NAME_FULL"
    name_list = []
    for name in entity.get_type_values(registry.name): # fof a while not using firstName, secondName, middleName, lastName, fatherName, motherName
        name_type = "PRIMARY" if name == entity.caption else "ALIAS"
        if not name is list:
            name_list.append({"NAME_TYPE": name_type, name_field: name})
        else:
            for name_el in name:
                name_list.append({"NAME_TYPE": name_type, name_field: name_el})
    if entity.has('weakAlias'):
        weak_alias_list = entity.get('weakAlias', quiet=True)
        if alias_split:
            splitted_alias_list = []
            for splitted_alias in [weak_alias_split(alias) for alias in weak_alias_list]:
                splitted_alias_list += splitted_alias
            weak_alias_list = splitted_alias_list
        for other_alias in weak_alias_list:
            name_list.append({"NAME_TYPE": "ALIAS", name_field: other_alias})    
    record['NAME_LIST'] = name_list
    addr_list = []          
    for addr_id in entity.get("addressEntity"):
        addr = ADDRESSES.get(addr_id)
        if addr is None:
            continue
        elif addr.has("postalCode") or addr.has("city")):
                addr_data = {
                    "ADDR_TYPE":            addr_type,
                    "ADDR_LINE1":           addr.first("street"),
                    "ADDR_LINE2":           addr.first("street2"),
                    "ADDR_CITY":            addr.first("city"),
                    "ADDR_STATE":           addr.first("state"),
                    "ADDR_COUNTRY":         addr.first("country"),
                    "ADDR_POSTAL_CODE":     addr.first("postalCode")
                }
        else:
            addr_data = {
                "ADDR_TYPE": addr_type,
                "ADDR_FULL": addr.first("full")
                }
        addr_type = "OTHER"
        addr_list.append(addr_data)
    for value in entity.get('address', quiet=True):
        addr_data = {
            "ADDR_TYPE": addr_type,
            "ADDR_FULL": value
        }
        addr_type = "OTHER" # if it is correct type for address?
        addr_list.append(addr_data)
    if len(addr_list):
        record["ADDRESS_LIST"] = addr_list
    for gender in entity.get("gender", quiet=True):
        if gender == "male":
            record["GENDER"] = "M"
        if gender == "female":
            record["GENDER"] = "F"
    map_dict_single = {
        "DATE_OF_BIRTH":        "birthDate", 
        "DATE_OF_DEATH":        "deathDate", 
        "PLACE_OF_BIRTH":       "birthPlace", 
        "NATIONALITY":          "nationality", 
        "REGISTRATION_DATE":    "incorporationDate", 
        "NATIONAL_ID_NUMBER":   "idNumber", 
        "TAX_ID_NUMBER":        "taxNumber"
        }
    for key, value in map_dict_single.items():
        attr = get_attribute(entity, value, key)
        if attr[key]:
            record.update(attr)
    map_dict_lists = {
        "WEBSITE_ADDRESS":          ["website"], 
        "EMAIL_ADDRESS":            ["email"], 
        "PHONE_NUMBER":             ["phone"],
        "COUNTRY_OF_ASSOCIATION":   ["jurisdiction"]  
        }
    if is_org:
        map_dict_lists["COUNTRY_OF_ASSOCIATION"].append('country')
    for key, value in map_dict_lists.items():
        attr = get_attribute_list(entity, value, key)
        if attr[key + '_LIST']:
            if len(attr[key + "_LIST"]) > 1:
                record.update(attr)
            else:
                record.update(attr[key + "_LIST"][0])
    relationship_list = []
    anchor = False   
    relationship_list, anchor = create_disclosed_relashionships(data_source, relationship_list, UNKNOWN_LINKS,      entity.id,  'role',         'object',       anchor)
    relationship_list, anchor = create_disclosed_relashionships(data_source, relationship_list, OWNERSHIPS,         entity.id,  'role',         'asset',        anchor)        
    relationship_list, anchor = create_disclosed_relashionships(data_source, relationship_list, DIRECTORSHIPS,      entity.id,  'role',         'organization', anchor)
    relationship_list, anchor = create_disclosed_relashionships(data_source, relationship_list, EMPLOYMENTS,        entity.id,  'role',         'eployee',      anchor)
    relationship_list, anchor = create_disclosed_relashionships(data_source, relationship_list, MEMBERSHIPS,        entity.id,  'role',         'organiztion',  anchor)
    relationship_list, anchor = create_disclosed_relashionships(data_source, relationship_list, REPRESENTATIONS,    entity.id,  'role',         'client',       anchor)
    relationship_list, anchor = create_disclosed_relashionships(data_source, relationship_list, FAMILIES,           entity.id,  'relationship', 'realtive',     anchor)
    relationship_list, anchor = create_disclosed_relashionships(data_source, relationship_list, ASSOCIATIONS,       entity.id,  'relationship', 'associate',    anchor)
    if relationship_list:
        record.update({"RELATIONSHIP_LIST":relationship_list})
    for adj in IDENTIFICATIONS.get(entity.id, []):
        if adj.schema.is_a("Passport"):
                record.update({
                    "PASSPORT_NUMBER":  adj.first("number"),
                    "PASSPORT_COUNTRY": adj.first("country"),
                })
    if 'PASSPORT_NUMBER' not in record.keys():
        attr = get_attribute(entity, "passportNumber", "PASSPORT_NUMBER")
        if attr["PASSPORT_NUMBER"]:
            record.update(attr) 
    
    map_dict_list = {
    "INN_CODE":         "innCode",
    "VAT_CODE":         "vatCode",
    "DUNS_NUMBER":      "dunsCode", 
    "SWIFT_BIC_CODE":   "swiftBic", 
    "ICIJ_ID_CODE":     "icijId",
    "OKPO_CODE":        "okpoCode",
    "BVDID_CODE":       "bvdid"  
    }
    if is_org:
        map_dict_list.update({
        "VOEN_CODE":        "voenCode",
        "BIK_CODE":         "bikCode",
        "IRS_CODE":         "irsCode",
        "IPO_CODE":         "ipoCode",
        "CIK_CODE":         "cikCode",
        "JIB_CODE":         "jibCode",
        "CAEM_CODE":        "caemCode",
        "COATO_CODE":       "coatoCode",
        #"MBS_CODE":        "mbsCode",
        #"IBC_RUC_CODE":    "ibcRuc",
        "OGRN_CODE":        "ogrnCode",
        "PRF_NUMBER_CODE":  "pfrNumber",
        "OKSM_CODE":        "oksmCode"
        })
    for key, value in map_dict_list.items():
        attr = get_attribute(entity, value, key)
        if attr[key]:
            record.update(attr)
    return record

# process ftm entities to senzing entities function
def process_entities(data_source, source_file, log = None, alias_split = True, catch_unknown_entities = False):
    if not log:
        logging.basicConfig(level = logging.DEBUG)
        log = logging.getLogger("ftm_processor")
    try:
        start_time = datetime.now()
        target_file = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
        # lists of entity types used in mapper
        thing_entity_list = ["Person", "Organization", "Company", "LegalEntity", "PublicBody"] # "Asset" - not used for now
        interest_entity_list = ["Succession", "Directorship", "Employment", "Membership", "Representation", "UnknownLink", "Ownership"]# "ProjectParticipant", "ContractAward", "Documentation", "CourtCaseParty" - not used for now
        intervals_entity_list = interest_entity_list + ["Identification", "Address", "Family", "Associate"]

        # dicts for interval entities
        DIRECTORSHIPS: Dict[str, List[EntityProxy]] = {}
        EMPLOYMENTS: Dict[str, List[EntityProxy]] = {}
        MEMBERSHIPS: Dict[str, List[EntityProxy]] = {} 
        REPRESENTATIONS: Dict[str, List[EntityProxy]] = {}
        UNKNOWN_LINKS: Dict[str, List[EntityProxy]] = {}
        OWNERSHIPS: Dict[str, List[EntityProxy]] = {}
        ADDRESSES: Dict[str, EntityProxy] = {}
        IDENTIFICATIONS: Dict[str, List[EntityProxy]] = {}
        FAMILIES: Dict[str, List[EntityProxy]] = {}
        ASSOCIATIONS: Dict[str, List[EntityProxy]] = {}
        #SUCCESIONS: Dict[str, List[EntityProxy]] = {}

        # List for unknown entities
        OTHER_ENTITIES = []
        
        log.info('Proccessing file: ' + source_file)
        log.info('Data Source: ' + data_source)

        if catch_unknown_entities:
            catch_unk_entities(source_file, thing_entity_list, intervals_entity_list, OTHER_ENTITIES, log)
        
        for entity in read_entities(source_file, thing_entity_list, intervals_entity_list, 
                                    DIRECTORSHIPS, EMPLOYMENTS, MEMBERSHIPS, REPRESENTATIONS, UNKNOWN_LINKS,  OWNERSHIPS, IDENTIFICATIONS, ADDRESSES, 
                                    FAMILIES, ASSOCIATIONS, log):
            record = transform(data_source, entity, DIRECTORSHIPS, EMPLOYMENTS, MEMBERSHIPS, REPRESENTATIONS, UNKNOWN_LINKS,  OWNERSHIPS, IDENTIFICATIONS, 
                                ADDRESSES, FAMILIES, ASSOCIATIONS, alias_split)
            target_file.write(json.dumps(record))
            target_file.write("\n")    
        log.info('All records processed! total time spent: ' + str(datetime.now()-start_time))
        target_file.seek(0)
        return target_file
    except Exception as err:
        log.info('Error occured!')
        log.info(' %s' % err)
        sys.exit(1)

# do all stuff
if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-i', '--input_file', default=os.getenv('input_file', None), type=str, help='A FTM .json input file.')
    argparser.add_argument('-o', '--output_file', default=os.getenv('output_file', None), type=str, help='output filename, defaults to input file name with a .json extension and \"out_\" prefix.')
    argparser.add_argument('-d', '--data_source', default=os.getenv('data_source'.upper(), None), type=str, help='Data Source name.')
    argparser.add_argument('-l', '--log_file', default=os.getenv('log_file', None), type=str, help='optional statistics filename.')
    argparser.add_argument('-u', '--unk_entities', default=os.getenv('unk_entities', False), type=bool, help='optional bool arg (default: False), if set to True - mapper gets stats about unknown entites.')
    args = argparser.parse_args()
    input_file_name = args.input_file
    output_file_name = args.output_file
    data_source = args.data_source
    log_file = args.log_file
    if not input_file_name:
        print('')
        print('Please select a ftm .json input file')
        print('')
        sys.exit(1)
    if not output_file_name:
        print('')
        print('Please select a .json output file')
        print('')
        sys.exit(1)
    if not data_source:
        print('')
        print('Please select a Data Source')
        print('')
        sys.exit(1)
    if log_file:
        logging.basicConfig(filename = log_file,
                            filemode = 'a',
                            format = '%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt = '%H:%M:%S',
                            level = logging.DEBUG)
    else:
        logging.basicConfig(level = logging.DEBUG)
    
    log = logging.getLogger("ftm_processor")
    
    try:
        output = process_entities(data_source, input_file_name, log, alias_split = True, catch_unknown_entities = args.unk_entities)
        temp_file_name = output.name
        output.close()
        shutil.copy(temp_file_name, output_file_name)
        log.info('Saved as ' + output_file_name)
        os.remove(temp_file_name)
        sys.exit(0)
    except Exception as err:
        log.info('Error occured!')
        log.info(' %s' % err)
        sys.exit(1)
