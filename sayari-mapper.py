import sys
import os
import argparse
import csv
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random
import pandas
import glob
import gzip
import io
import sqlite3
import psutil

#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

    #----------------------------------------
    def map(self, raw_data, json_data):

        if args.debug:
            print('-' * 50)
            json.dumps(raw_data, indent=4)

        #--column mappings

        # columnName: entity_id
        # 100.0 populated, 69.81 unique
        #      -0ZyB1NT_MI-68Hruorlmw (100)
        #      4pcAZU2pBub4dhH3YNkM-g (100)
        #      BQD3yBNu13jyF00Wa8J8QA (100)
        #      Gwi1mHFsTgwYGjCLHHvDTg (100)
        #      RdDfqQ6-epbp0SDy2N6Ufg (100)
        #--json_data['entity_id'] = raw_data['entity_id']

        # columnName: i
        # 100.0 populated, 0.0 unique
        #      0 (1638078)
        #      1 (622524)
        #      2 (34716)
        #      3 (13159)
        #      4 (6775)
        #--json_data['i'] = raw_data['i']

        #-----------------------
        #--the zero record holds values that are supposed to be same for all rows for the entity
        if raw_data['i'] == '0':

   
            #--standardize the record type to senzing
            # columnName: type
            # 100.0 populated, 0.0 unique
            #      company (1380456)
            #      person (862138)
            #      tradename (101304)
            #      vessel (1449)
            #      aircraft (1267)
            entity_type = raw_data['type'].upper()
            if entity_type in self.variant_data['ENTITY_TYPE']:
                json_data['RECORD_TYPE'] = self.variant_data['ENTITY_TYPE'][entity_type]['VALUE1']

            # columnName: label (its the best name for the entity)
            # 100.0 populated, 39.78 unique
            #      CYPROSERVUS CO. LIMITED (6952)
            #      CYPROLIAISON LIMITED (3086)
            #      TOTALSERVE MANAGEMENT LIMITED (3059)
            #      CYMANCO SERVICES LIMITED (2969)
            #      MONTRAGO SERVICES LIMITED (2307)
            if raw_data['label'] and raw_data['label'] not in json_data['distinct_name_list']: 
                json_data['distinct_name_list'].append(raw_data['label'])

            # columnName: label_en
            # 0.07 populated, 8.75 unique
            #      Militaire vleugel van Hizbu'llah (62)
            #      Esercito/Fronte/Partito rivoluzionario popolare di liberazione (53)
            #      Volksfront voor de bevrijding van Palestina-Algemeen Commando (45)
            #      The Organization Base of Jihad/Country of the Two Rivers (45)
            #      Zamaat Modzhakhedov Tsentralnoy Asii (43)
            if raw_data['label_en'] and raw_data['label_en'] not in json_data['distinct_name_list']:
                json_data['distinct_name_list'].append(raw_data['label_en'])

            # columnName: num_documents
            # 100.0 populated, 0.01 unique
            #      1 (1938523)
            #      2 (297124)
            #      4 (33314)
            #      3 (20940)
            #      5 (15002)
            #json_data['num_documents'] = raw_data['num_documents']

            # columnName: sanctioned
            # 100.0 populated, 0.0 unique
            #      false (2274227)
            #      true (72387)
            json_data['sanctioned'] = 'Yes' if raw_data['sanctioned'].upper() == 'TRUE' else ''

            # columnName: pep
            # 100.0 populated, 0.0 unique
            #      false (2346614)
            json_data['pep'] = 'Yes' if raw_data['pep'].upper() == 'TRUE' else ''

            # columnName: degree
            # 100.0 populated, 0.01 unique
            #      1 (1322510)
            #      2 (473755)
            #      3 (191237)
            #      0 (155049)
            #      4 (81311)
            #--json_data['degree'] = raw_data['degree']

            # columnName: source_counts
            # 100.0 populated, 0.02 unique
            #      {"CYP/companies":1.0} (1843993)
            #      {"MLT/malta_companies_register":2.0} (253695)
            #      {"MLT/malta_companies_register":1.0} (79660)
            #      {"MLT/malta_companies_register":4.0} (29292)
            #      {"USA/consolidated_screening_list":1.0,"USA/ofac_sdn":1.0} (20094)
            if raw_data['source_counts']: 
                if type(raw_data['source_counts']) != dict:
                    raw_data['source_counts'] = json.loads(raw_data['source_counts'])
                json_data['sources'] = ','.join(sorted(list(raw_data['source_counts'].keys())))

            # columnName: edge_counts
            # 93.39 populated, 0.64 unique
            #      {"director_of":{"out":1,"in":0,"total":1}} (451651)
            #      {"officer_of":{"out":0,"in":1,"total":1},"director_of":{"out":0,"in":1,"total":1}} (277183)
            #      {"officer_of":{"out":1,"in":0,"total":1}} (267798)
            #      {"director_of":{"out":0,"in":1,"total":1},"officer_of":{"out":0,"in":1,"total":1}} (144205)
            #      {"officer_of":{"out":1,"in":0,"total":1},"director_of":{"out":1,"in":0,"total":1}} (104776)
            #if raw_data['edge_counts']:
            #    json_data['roles'] = ' | '.join(list(json.loads(raw_data['edge_counts']).keys()))
                #print(f"{json_data['RECORD_ID']}\n{raw_data['edge_counts']}\n{json_data['roles']}\n")
                #input('press any key')

            # columnName: shares
            # 4.49 populated, 18.4 unique
            #      {"extra":{"IssuedShares":"1200.0","PerShareValue":"1.0"},"num_shares":1200.0,"monetary_value":1200.0,"currency":"EUR","type":"Ordinary"} (20491)
            #      {"extra":{"IssuedShares":"1.0","PerShareValue":"1.0"},"num_shares":1.0,"monetary_value":1.0,"currency":"EUR","type":"Ordinary B"} (6994)
            #      {"extra":{"IssuedShares":"500.0","PerShareValue":"2.329373"},"num_shares":500.0,"monetary_value":1164.6865,"currency":"EUR","type":"Ordinary"} (5058)
            #      {"extra":{"IssuedShares":"1500.0","PerShareValue":"1.0"},"num_shares":1500.0,"monetary_value":1500.0,"currency":"EUR","type":"Ordinary"} (3634)
            #      {"extra":{"IssuedShares":"1199.0","PerShareValue":"1.0"},"num_shares":1199.0,"monetary_value":1199.0,"currency":"EUR","type":"Ordinary A"} (2703)
            #json_data['shares'] = raw_data['shares']

        #-----------------------
        #--the remaining fields are not necessarily the same for each row for the entity

        # columnName: name
        # 71.87 populated, 58.39 unique
        #      {"value":"CYPROSERVUS CO. LIMITED"} (6951)
        #      {"value":"CYPROLIAISON LIMITED"} (3085)
        #      {"value":"TOTALSERVE MANAGEMENT LIMITED"} (3058)
        #      {"value":"CYMANCO SERVICES LIMITED"} (2967)
        #      {"value":"MONTRAGO SERVICES LIMITED"} (2305)
        if raw_data['name']: 
            if type(raw_data['name']) != dict:
                raw_data['name'] = json.loads(raw_data['name'])
            if raw_data['name']['value'] not in json_data['distinct_name_list']:
                json_data['distinct_name_list'].append(raw_data['name']['value'])

        # columnName: address
        # 33.35 populated, 44.89 unique
        #      {"value":"Αρχ. Μακαρίου ΙΙΙ, 284, FORTUNA COURT BLOCK B, Floor 2, 3105, Λεμεσός, Κύπρος","house":"Fortuna Court Block B","house_number":"3105","level":"Floor 2","road":"Αρχ. Μακαρίου Ιιι","city":"Λεμεσός","country":"Κύπρος"} (4983)
        #      {"value":"Αγίου Παύλου, 15, LEDRA HOUSE, 'Αγιος Ανδρέας, 1105, Λευκωσία, Κύπρος","house":"Ledra House 'αγιος Ανδρέας","house_number":"15","road":"Αγίου Παύλου","state":"Λευκωσία","postcode":"1105","country":"Κύπρος"} (3698)
        #      {"value":"Αρχ. Μακαρίου ΙΙΙ, 155, PROTEAS HOUSE, Floor 5, 3026, Λεμεσός, Κύπρος","house":"Proteas House","house_number":"155","level":"Floor 5","road":"Αρχ. Μακαρίου Ιιι","city":"Λεμεσός","postcode":"3026","country":"Κύπρος"} (2960)
        #      {"value":"Θεμιστοκλή Δέρβη, 3, JULIA HOUSE, 1066, Λευκωσία, Κύπρος","house":"Julia House","house_number":"3","road":"Θεμιστοκλή Δέρβη","state":"Λευκωσία","postcode":"1066","country":"Κύπρος"} (2796)
        #      {"value":"Γρ. Ξενοπούλου, 17, 3106, Λεμεσός, Κύπρος","house_number":"17","road":"Γρ. Ξενοπούλου","city":"Λεμεσός","postcode":"3106","country":"Κύπρος"} (2698)
        if raw_data['address']: 
            addr_value = json.loads(raw_data['address'])['value']
            if addr_value not in json_data['distinct_address_list']:
                json_data['distinct_address_list'].append(addr_value)

        # columnName: date_of_birth
        # 0.28 populated, 72.78 unique
        #      {"value":"1963"} (47)
        #      {"value":"1958"} (44)
        #      {"value":"1962"} (42)
        #      {"value":"1961"} (42)
        #      {"value":"1964"} (41)
        if raw_data['date_of_birth']:
            dob_value = self.format_dob(json.loads(raw_data['date_of_birth'])['value'])
            mapped_dict = {'DATE_OF_BIRTH': dob_value}
            if mapped_dict not in json_data['ATTRIBUTE_LIST']:
                json_data['ATTRIBUTE_LIST'].append(mapped_dict)

        # columnName: gender
        # 0.1 populated, 0.09 unique
        #      {"value":"male"} (2075)
        #      {"value":"female"} (204)
        if raw_data['gender']:
            gender_value = json.loads(raw_data['gender'])['value']
            mapped_dict = {'GENDER': gender_value}
            if mapped_dict not in json_data['ATTRIBUTE_LIST']:
                json_data['ATTRIBUTE_LIST'].append(mapped_dict)

        # columnName: contact
        # 0.06 populated, 98.19 unique
        #      {"value":"http://nitcshipping.com","type":"url"} (4)
        #      {"value":"nmz@nmz.kirov.ru","type":"email"} (2)
        #      {"value":"office@gpsm.ru","type":"email"} (2)
        #      {"value":"general@sovfracht.ru","type":"email"} (2)
        #      {"value":"http://mriyaresort.com","type":"url"} (2)
        if raw_data['contact']:
            contact_dict = json.loads(raw_data['contact'])
            self.update_stat('X-CONTACT_TYPES', contact_dict['type'])
            contact_type = contact_dict['type'].upper()
            if contact_type in self.variant_data['CONTACT_TYPE']:
                senzing_attr = self.variant_data['CONTACT_TYPE'][contact_type]['ATTRIBUTE']
                mapped_dict = {senzing_attr: contact_dict['value']}
                if mapped_dict not in json_data['CONTACT_METHODS']: 
                    json_data['CONTACT_METHODS'].append(mapped_dict)
            else:
                self.update_stat('?-CONTACT_TYPES', contact_dict['type'])

        # columnName: identifier
        # 26.92 populated, 100.0 unique
        #      {"value":"20468A","type":"malta_national_id"} (2)
        #      {"value":"17258A","type":"malta_national_id"} (2)
        #      {"value":"AB/26/84/65","type":"malta_accountancy_registration_id"} (2)
        #      {"value":"AB/26/84/62","type":"malta_accountancy_registration_id"} (2)
        #      {"value":"312764M","type":"malta_national_id"} (2)
        if raw_data['identifier']:
            identifier_dict = json.loads(raw_data['identifier'])
            mapped_dict = self.map_identifier('IDENTIFIER_TYPE', identifier_dict)
            if mapped_dict and mapped_dict not in json_data['IDENTIFIER_LIST']: 
                json_data['IDENTIFIER_LIST'].append(mapped_dict)

        # columnName: weak_identifier
        # 5.07 populated, 73.08 unique
        #      {"extra":{"Type":"Additional Sanctions Information -"},"value":"Subject to Secondary Sanctions","type":"unknown"} (1721)
        #      {"extra":{"Type":"Warrant Number"},"value":"19007","type":"unknown"} (1172)
        #      {"extra":{"Type":"Warrant Number"},"value":"10914","type":"unknown"} (788)
        #      {"extra":{"Type":"Warrant Number"},"value":"02286","type":"unknown"} (760)
        #      {"extra":{"Type":"Warrant Number"},"value":"19808","type":"unknown"} (615)
        #
        if raw_data['weak_identifier']:
            identifier_dict = json.loads(raw_data['weak_identifier'])
            mapped_dict = self.map_identifier('WEAK_IDENTIFIER_TYPE', identifier_dict)
            if mapped_dict and mapped_dict not in json_data['IDENTIFIER_LIST']: 
                json_data['IDENTIFIER_LIST'].append(mapped_dict)

        # columnName: status
        # 47.44 populated, 2.57 unique
        #      {"value":"closed","text":"Διαγραμμένη"} (177297)
        #      {"value":"active","text":"Εγγεγραμμένη"} (171304)
        #      {"value":"active","text":"Στάληκε επιστολή Υπενθύμισης"} (98201)
        #      {"value":"closed","text":"Struck Off"} (24452)
        #      {"value":"closed","text":"Διάλυση λόγω Ολοκλήρωσης Εκούσιας Εκκαθάρισης"} (18259)
        if raw_data['status']:

            #--full json so you don't miss nothin
            # json_data['status_list'].append(raw_data['status'])

            #--key value only for cleaner display 
            # json_data['status_list'].append(json.loads(raw_data['status'])['value'])
            
            #--attempt to pick fields user wants to see
            parsed_data = json.loads(raw_data['status'])
            if 'value' in parsed_data:
                string_data = parsed_data['value']
                if 'date' in parsed_data:
                    string_data += ' ' + parsed_data['date']
                json_data['status_list'].append(string_data)

        # columnName: company_type
        # 18.07 populated, 0.0 unique
        #      {"extra":{"Code":"C","SubType":"Ιδιωτική"},"value":"Εταιρεία"} (408959)
        #      {"extra":{"Code":"P","SubType":"Ομόρρυθμος"},"value":"Συνεταιρισμός"} (8614)
        #      {"extra":{"Code":"O"},"value":"Αλλοδαπή Εταιρεία"} (3332)
        #      {"extra":{"Code":"P","SubType":"Ετερόρρυθμος"},"value":"Συνεταιρισμός"} (1096)
        #      {"extra":{"Code":"C","SubType":"Δι΄ Εγγυήσεως Χωρίς Κεφάλαιο"},"value":"Εταιρεία"} (1012)
        if raw_data['company_type']:
            self.update_stat('?-REVIEW', 'HAS_COMPANY_TYPE', json_data['RECORD_ID'])

            #--full json so you don't miss nothin
            # json_data['company_type_list'].append(raw_data['company_type'])

            #--key value only for cleaner display 
            json_data['company_type_list'].append(json.loads(raw_data['company_type'])['value'])

        # columnName: country
        # 10.84 populated, 0.39 unique
        #      {"value":"MLT","context":"address"} (101496)
        #      {"value":"MLT","context":"nationality"} (56077)
        #      {"value":"ITA","context":"nationality"} (10543)
        #      {"value":"GBR","context":"nationality"} (8188)
        #      {"extra":{"Original Text":"Malta"},"value":"MLT","context":"address"} (7462)
        if raw_data['country']:
            country_dict = json.loads(raw_data['country'])
            mapped_dict = self.map_country('COUNTRY_CONTEXT', country_dict)
            if mapped_dict and mapped_dict not in json_data['ATTRIBUTE_LIST']: 
                json_data['ATTRIBUTE_LIST'].append(mapped_dict)

        # columnName: additional_information
        # 21.65 populated, 2.02 unique
        #      {"extra":{"Name Status":"Τελευταίο Όνομα","Name Status Code":"ACR"},"type":"Cyprus Additional Information"} (474722)
        #      {"extra":{"Programs":"SDNTK","Source List":"Specially Designated Nationals (SDN) - Treasury Department"},"type":"Sanction Information"} (1794)
        #      {"extra":{"Additional Sanctions Information -":"Subject to Secondary Sanctions"},"type":"Other OFAC Sanctions Information"} (1719)
        #      {"extra":{"Programs":"SDGT","Source List":"Specially Designated Nationals (SDN) - Treasury Department"},"type":"Sanction Information"} (1144)
        #      {"extra":{"Programs":"SYRIA","Source List":"Specially Designated Nationals (SDN) - Treasury Department"},"type":"Sanction Information"} (563)
        if raw_data['additional_information']:
            self.update_stat('?-REVIEW', 'HAS_ADDITION_INFO', json_data['RECORD_ID'])
            parsed_data = json.loads(raw_data['additional_information'])
            if 'extra' in parsed_data:
                for key in parsed_data['extra']:
                    if key not in json_data:
                        json_data[key] = parsed_data['extra'][key]
                    else:
                        temp_data = json_data[key]
                        if temp_data[0] + temp_data[-1] != '[]':
                            temp_list = [temp_data]
                        else: 
                            temp_list = json.loads(temp_data)
                        if parsed_data['extra'][key] not in temp_list:
                            temp_list.append(parsed_data['extra'][key]) 
                            json_data[key] = json.dumps(temp_list)

        # columnName: finances
        # 7.97 populated, 5.75 unique
        #      {"value":1200.0,"context":"registered_capital","currency":"EUR"} (27586)
        #      {"value":1200.0,"context":"authorized_capital","currency":"EUR"} (25420)
        #      {"value":0.0,"context":"registered_capital","currency":"EUR"} (14702)
        #      {"value":0.0,"context":"authorized_capital","currency":"EUR"} (8564)
        #      {"value":1164.69,"context":"authorized_capital","currency":"EUR"} (8224)
        if raw_data['finances'] and args.extended_format:
            #json_data['finances'] = raw_data['finances']
            if 'finances' not in json_data:
                json_data['finances'] = json.loads(raw_data['finances'])
            else:
                json_data['finances'].update(json.loads(raw_data['finances']))

        return json_data

    #-----------------------------------
    def map_identifier(self, code_type, identifier_dict):

        id_value = identifier_dict['value']
        if 'extra' in identifier_dict and 'Type' in identifier_dict['extra']:
            id_type = identifier_dict['extra']['Type']
        else:
            id_type = identifier_dict['type']
        self.update_stat(f'X-{code_type}', id_type)

        #--direct mapping
        id_type = id_type.upper()
        if id_type in self.variant_data[code_type]:
            senzing_attr = self.variant_data[code_type][id_type]['ATTRIBUTE']
            country_code = self.variant_data[code_type][id_type]['VALUE1']

        #--try to figure it out,. e.g. "AUS-AUSTRALIAN PASSPORT"
        else:
            self.update_stat(f'?-{code_type}', id_type)

            if 'PASSPORT' in id_type:
                senzing_attr = 'PASSPORT'
            else:
                senzing_attr = 'OTHER_ID'

            if len(id_type) > 4 and id_type[3:1] == '-' and id_type[0:3] in self.variant_data['COUNTRY_CODE']:
                country_code = id_type[0:3]
            else:
                country_code = ''

        mapped_dict = {senzing_attr + '_NUMBER': id_value}
        if country_code:
            mapped_dict[senzing_attr + '_COUNTRY'] = country_code
        if senzing_attr in ('OTHER_ID'):
            mapped_dict[senzing_attr + '_TYPE'] = id_type

        return mapped_dict

    #-----------------------------------
    def map_country(self, code_type, country_dict):
        if 'context' in country_dict and 'value' in country_dict:
            #--direct mapping
            country_context = country_dict['context'].upper()

            if country_context in self.variant_data[code_type]:
                senzing_attr = self.variant_data[code_type][country_context]['ATTRIBUTE']
                mapped_dict = {senzing_attr: country_dict['value']}
            else:
                prefix = country_context.replace('_', '-').replace(' ', '')
                mapped_dict = {prefix + '_COUNTRY_OF_ASSOCIATION': country_dict['value']}
                self.update_stat('?-COUNTRY_CONTEXT', country_dict['context'])
            return mapped_dict
        else:
            self.update_stat('?-INVALID_COUNTRY', json.dumps(country_dict))
            return None

    #----------------------------------------
    def load_reference_data(self):

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

        with open('sayari_codes.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['CODE_TYPE'] = row['CODE_TYPE'].upper()
                row['CODE'] = row['CODE'].upper()
                if row['CODE_TYPE'] not in self.variant_data:
                    self.variant_data[row['CODE_TYPE']] = {}
                self.variant_data[row['CODE_TYPE']][row['CODE']] = row

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value

    #----------------------------------------
    def format_dob(self, raw_date):
        try: new_date = dateparse(raw_date)
        except: return ''

        #--correct for prior century dates
        if new_date.year > datetime.now().year:
            new_date = datetime(new_date.year - 100, new_date.month, new_date.day)

        if len(raw_date) == 4:
            output_format = '%Y'
        elif len(raw_date) in (5,6):
            output_format = '%m-%d'
        elif len(raw_date) in (7,8):
            output_format = '%Y-%m'
        else:
            output_format = '%Y-%m-%d'

        return datetime.strftime(new_date, output_format)

    #----------------------------------------
    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]['count'] = 0

        self.stat_pack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]['examples'] = []
            if example not in self.stat_pack[cat1][cat2]['examples']:
                if len(self.stat_pack[cat1][cat2]['examples']) < 5:
                    self.stat_pack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]['examples'][randomSampleI] = example
        return

    #----------------------------------------
    def capture_mapped_stats(self, json_data):

        if 'DATA_SOURCE' in json_data:
            data_source = json_data['DATA_SOURCE']
        else:
            data_source = 'UNKNOWN_DSRC'

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(data_source, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    if type(subrecord) == dict:
                        for key2 in subrecord:
                            self.update_stat(data_source, key2, subrecord[key2])

#----------------------------------------
def load_relationships(relationship_file_list):
    if not relationship_file_list:
        return None

    if False: #--args.in_memory:
        relationdb_path = ':memory:'
    else:        
        relationdb_path = os.path.split(relationship_file_list[0])[0] 
        if relationdb_path and relationdb_path[-1] != os.path.sep:
            relationdb_path = relationdb_path + os.path.sep
        relationdb_path = relationdb_path + 'relationships.db'

        if os.path.exists(relationdb_path):
            relation_dbo = sqlite3.connect(relationdb_path)
            was_finished = relation_dbo.cursor().execute("select name from sqlite_master where type='table' and name='finished'").fetchone()
            if was_finished:
                response = input('\nRelation database already exists, do you want to rebuild it? (y/n) ')
                if not response.upper().startswith('Y'):
                    return relation_dbo
            os.remove(relationdb_path)

    print(f'\nPre-loading {len(relationship_file_list)} files ...\n')
    relation_dbo = sqlite3.connect(relationdb_path)
    for relationship_file_name in relationship_file_list:
        print(relationship_file_name, '...', end='', flush=True)
        timer_start = time.time()
        base_file_name, file_extension = os.path.splitext(relationship_file_name)
        compressed = file_extension.upper() == '.GZ'
        if compressed:
            base_file_name, file_extension = os.path.splitext(base_file_name)

        if file_extension.upper() == '.PARQUET':
            if compressed:
                df = pandas.read_parquet(relationship_file_name, engine='auto', encoding='utf-8', compression='gzip')
            else:
                df = pandas.read_parquet(relationship_file_name, engine='auto', encoding='utf-8')
        else:
            if compressed:
                df = pandas.read_csv(relationship_file_name, low_memory=False, encoding='utf-8', compression='gzip')
            else:
                df = pandas.read_csv(relationship_file_name, low_memory=False, encoding='utf-8')
        df[['src', 'dst', 'type']].to_sql('relationships', relation_dbo, index=False, method='multi', chunksize=10000, if_exists='append')
        print(f' completed in {round((time.time() - timer_start) / 60, 1)} minutes')

        display_process_stats(main_pid, 'after loading this file')

        if shut_down:
            break

    if not shut_down:
        print('\nindexing relationships ...', end='', flush = True)
        timer_start = time.time()
        relation_dbo.cursor().execute('create index ix_relationships on relationships (src)')
        print(f' completed in {round((time.time() - timer_start) / 60, 1)} minutes')

    relation_dbo.cursor().execute('create table finished (dummy integer)')

    return relation_dbo

# --------------------------------------
def sql_exec(dbo, sql, parm_list=None):
    try:
        exec_cursor = dbo.cursor()
        if parm_list:
            if type(parm_list) not in (list, tuple):
                parm_list = [parm_list]
            exec_cursor.execute(sql, parm_list)
        else:
            exec_cursor.execute(sql)
    except Exception as err:
        print(f'\nSQL ERROR:\n{sql}\n{err}\n')
        raise Exception(err)

    cursor_data = {}
    if exec_cursor:
        cursor_data['SQL'] = sql
        cursor_data['CURSOR'] = exec_cursor
        cursor_data['ROWS_AFFECTED'] = exec_cursor.rowcount
        if exec_cursor.description:
            cursor_data['FIELD_LIST'] = [field_data[0] for field_data in exec_cursor.description]
    return cursor_data

# --------------------------------------
def sql_fetch_next(cursor_data):
    row = cursor_data['CURSOR'].fetchone()
    if row:
        type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
        return dict(list(zip(cursor_data['FIELD_LIST'], type_fixed_row)))

# --------------------------------------
def sql_fetch_all(cursor_data):
    ''' fetch all the rows with column names '''
    row_list = []
    for row in cursor_data['CURSOR'].fetchall():
        type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
        row_dict = dict(list(zip(cursor_data['FIELD_LIST'], type_fixed_row)))
        row_list.append(row_dict)
    return row_list

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return

#----------------------------------------
def display_process_stats(pid, note):
    print(f'\n{note} memory used: {round(pid.memory_info().rss /1024 /1024 /1024.0,2)}gb\n')  # in bytes 

#----------------------------------------
if __name__ == "__main__":
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)
    main_pid = psutil.Process(os.getpid())

    csv_dialect = 'excel'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_path', dest='input_path', help='the directiory that contains the sayari files')
    parser.add_argument('-o', '--output_path', dest='output_path', help='the directory to write the mapped json files to')
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    parser.add_argument('-x', '--extended_format', dest='extended_format', action='store_true', default=False, help='map the all the fields including finances and other information')
    parser.add_argument('-D', '--debug', dest='debug', action='store_true', default=False, help='run in debug mode')
    #--parser.add_argument('-M', '--in_memory', dest='in_memory', action='store_true', default=False, help='use this option for faster performance (requires at least 16g of ram)')
    args = parser.parse_args()

    if not args.input_path:
        print('\nPlease supply a valid input file path on the command line\n')
        sys.exit(1)
    if not args.output_path:
        print('\nPlease supply a valid output file path on the command line\n') 
        sys.exit(1)

    #--validate output directory
    if not os.path.isdir(args.output_path):
        print('\nOutput file path is not a valid directory\n') 
        sys.exit(1)
    if args.output_path[-1] != os.path.sep:
        args.output_path = args.output_path + os.path.sep
    
    # get list of files, ajusting file spec for directory
    if os.path.isdir(args.input_path): 
        if args.input_path[-1] == os.path.sep:
            input_file_list = glob.glob(args.input_path + '*')
        else:
            input_file_list = glob.glob(args.input_path + os.path.sep + '*')
    else:
        input_file_list = glob.glob(args.input_path)
    if len(input_file_list) == 0:
        print(f'\nNo files found at {args.input_path}\n')
        sys.exit(1)

    # count files and confirm to proceed
    print('\nQualifying files ...\n')
    entity_file_list = []
    relationship_file_list = []
    for input_file_name in sorted(input_file_list):
        if not os.path.splitext(input_file_name)[1].upper() in ('.CSV', '.GZ'):
            continue
        if 'entities' in input_file_name:
            print('entity ->',input_file_name)
            entity_file_list.append(input_file_name)
        elif 'relationships' in input_file_name:
            print('relationship ->', input_file_name)
            relationship_file_list.append(input_file_name)
    print(f'\n{len(entity_file_list)} entity files and {len(relationship_file_list)} relationship files found\n')
    if not entity_file_list:
        print('ERROR: No sayari entity files found!\n')
        sys.exit(1)
    if not relationship_file_list:
        print('WARNING: No sayari relationship files found!\n')
    response = input('OK to proceed? (y/n) ')
    if not response.upper().startswith('Y'):
        print(f'\nProcess aborted!')
        sys.exit(1)

    proc_start_time = time.time()

    display_process_stats(main_pid, 'starting')

    relation_dbo = load_relationships(relationship_file_list) if relationship_file_list else False
    if shut_down:
        print(f'\nProcess aborted!')
        sys.exit(1)

    display_process_stats(main_pid, 'relationships loaded')

    mapper = mapper()

    for input_file_name in entity_file_list:
        print(f'\nPreparing {input_file_name} ...', end='', flush=True)

        timer_start = time.time()
        base_file_name, file_extension = os.path.splitext(input_file_name)
        compressed = file_extension.upper() == '.GZ'
        if compressed:
            base_file_name, file_extension = os.path.splitext(base_file_name)
        if file_extension.upper() == '.PARQUET':
            if compressed:
                df = pandas.read_parquet(input_file_name, engine='auto', encoding='utf-8', compression='gzip')
            else:
                df = pandas.read_parquet(input_file_name, engine='auto', encoding='utf-8')
        else:
            if compressed:
                df = pandas.read_csv(input_file_name, low_memory=False, encoding='utf-8', compression='gzip')
            else:
                df = pandas.read_csv(input_file_name, low_memory=False, encoding='utf-8')
        df.to_sql('entities', relation_dbo, index=False, method='multi', chunksize=1000, if_exists='replace')
        print(f' completed in {round((time.time() - timer_start) / 60, 1)} minutes')

        print(f'\nInitial query ...', end='', flush=True)
        timer_start = time.time()
        main_cursor = sql_exec(relation_dbo, 'select * from entities order by entity_id, i')
        input_row = sql_fetch_next(main_cursor)
        print(f' completed in {round((time.time() - timer_start) / 60, 1)} minutes')

        display_process_stats(main_pid, 'this file')

        file_start_time = time.time()
        output_file_name = os.path.splitext(base_file_name)[0] + '.json'
        if compressed:
            output_file_handle = gzip.open(output_file_name + '.gz', 'wb')
        else:
            output_file_handle = open(output_file_name, 'w', encoding='utf-8')

        input_row_count = 0
        output_row_count = 0
        while input_row:

            json_data = {}
            json_data['DATA_SOURCE'] = 'SAYARI'
            json_data['RECORD_ID'] = input_row['entity_id']

            #--initial lists of senzing attributes
            json_data['ATTRIBUTE_LIST'] = []
            json_data['CONTACT_METHODS'] = []
            json_data['IDENTIFIER_LIST'] = []

            #--temporary list of payload attributes
            json_data['distinct_name_list'] = []
            json_data['distinct_address_list'] = [] 
            json_data['status_list'] = [] 
            json_data['company_type_list'] = [] 

            #--there can be multiple rows for the same entity
            while input_row and input_row['entity_id'] == json_data['RECORD_ID']:
                input_row_count += 1
                if args.debug:
                    print()
                    print(json.dumps(input_row, indent=4))
                json_data = mapper.map(input_row, json_data)
                input_row = sql_fetch_next(main_cursor)

            #--set organization name attribute and first address label
            if 'RECORD_TYPE' in json_data and json_data['RECORD_TYPE'] == 'ORGANIZATION':
                name_attr = 'NAME_ORG'
                first_addr_label = 'BUSINESS'
            else:
                name_attr = 'NAME_FULL'
                first_addr_label = 'PRIMARY'

            #--map the distinct name list
            if json_data['distinct_name_list']:
                json_data['NAME_LIST'] = []
                for i in range(len(json_data['distinct_name_list'])):
                    if i == 0:
                        name_label = 'PRIMARY'
                    else:
                        name_label = 'ALT'
                    json_data['NAME_LIST'].append({name_label + '_' + name_attr: json_data['distinct_name_list'][i]})
            del(json_data['distinct_name_list'])

            #--map the distinct address list
            if json_data['distinct_address_list']:
                json_data['ADDRESS_LIST'] = []
                for i in range(len(json_data['distinct_address_list'])):
                    if i == 0:
                        addr_label = first_addr_label
                    else:
                        addr_label = 'ALT'
                    json_data['ADDRESS_LIST'].append({addr_label + '_ADDR_FULL': json_data['distinct_address_list'][i]})
            del(json_data['distinct_address_list'])

            if not json_data['ATTRIBUTE_LIST']:
                del(json_data['ATTRIBUTE_LIST'])
            if not json_data['CONTACT_METHODS']:
                del(json_data['CONTACT_METHODS'])
            if not json_data['IDENTIFIER_LIST']:
                del(json_data['IDENTIFIER_LIST'])

            if json_data['status_list']:
                temp_list = list(set(json_data['status_list']))
                if len(temp_list) == 1:
                    json_data['status'] = temp_list[0]
                else: 
                    json_data['status'] = json.dumps(sorted(temp_list))
            del(json_data['status_list'])

            if json_data['company_type_list']:
                temp_list = list(set(json_data['company_type_list']))
                if len(temp_list) == 1:
                    json_data['company_type'] = temp_list[0]
                else: 
                    json_data['company_type'] = json.dumps(sorted(temp_list))
            del(json_data['company_type_list'])

            #--add the relationships
            json_data['RELATIONSHIPS'] = [{
                'REL_ANCHOR_DOMAIN': 'SAYARI',
                'REL_ANCHOR_KEY': json_data['RECORD_ID']
            }]
            relation_list = []
            relation_rows = sql_fetch_all(sql_exec(relation_dbo, 'select * from relationships where src = ?', json_data['RECORD_ID']))
            for relation_row in relation_rows:
                json_data['RELATIONSHIPS'].append({
                    'REL_POINTER_DOMAIN': 'SAYARI',
                    'REL_POINTER_KEY': relation_row['dst'],
                    'REL_POINTER_ROLE': relation_row['type']
                    })

            #--capture the stats
            mapper.capture_mapped_stats(json_data)

            if args.debug:
                print()
                print(json.dumps(json_data, indent=4))
                input('press any key ...')

            if compressed:
                output_file_handle.write((json.dumps(json_data) + '\n').encode('utf-8'))
            else:
                output_file_handle.write(json.dumps(json_data) + '\n')
            output_row_count += 1

            if output_row_count % 10000 == 0:
                print('%s rows written, %s rows processed' % (output_row_count, input_row_count))
                #break # TEST FIRST 1000
            if shut_down:
                break

        output_file_handle.close()
        if shut_down:
            break
        else:
            elapsed_mins = round((time.time() - file_start_time) / 60, 1)
            run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
            print('%s rows written, %s rows processed, %s\n' % (output_row_count, input_row_count, run_status))

    relation_dbo.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)

    print('')
    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    if shut_down == 0:
        print('Process completed successfully in %s minutes!' % elapsed_mins)
    else:
        print('Process aborted after %s minutes!' % elapsed_mins)
    print('')

    display_process_stats(main_pid, 'final')

    sys.exit(0)

