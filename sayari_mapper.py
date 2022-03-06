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
    def __init__(self, codes_file_name, relationdb_name = None):

        self.load_reference_data()
        self.stat_pack = {}

        self.codes_file_name = codes_file_name

        self.code_conversion_data, self.unmapped_code_count = self.load_codes_file()
        self.new_code_records = []

        if os.path.exists(args.relationdb_name): #--will be opened later if does not exist
            self.open_relation_db(relationdb_name) if relationdb_name else None

    #----------------------------------------
    def open_relation_db(self, relationdb_name):
        self.relation_dbo = sqlite3.connect(f'file:{relationdb_name}?mode=ro', uri=True)
        self.relation_dbo.cursor().execute('PRAGMA query_only=ON')

    #----------------------------------------
    def load_codes_file(self):
        #--load the sayari code conversion table from csv    
        code_conversion_data = {}    
        unmapped_code_count = 0
        with open(self.codes_file_name, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['CODE_TYPE'] = row['CODE_TYPE'].upper()
                row['CODE'] = row['CODE'].upper()
                if row['CODE_TYPE'] not in code_conversion_data:
                    code_conversion_data[row['CODE_TYPE']] = {}
                code_conversion_data[row['CODE_TYPE']][row['CODE']] = row
                if row['REVIEWED'].upper() != 'Y':
                    unmapped_code_count += 1
        return code_conversion_data, unmapped_code_count

    #----------------------------------------
    def save_codes_file(self):
        if self.new_code_records:
            prior_codes_data, unmapped_code_count = self.load_codes_file()
            for code_data in self.new_code_records:
                if code_data['CODE_TYPE'] in prior_codes_data and code_data['CODE'] in prior_codes_data[code_data['CODE_TYPE']]:
                    continue  #--another process may have added it already
                if code_data['CODE_TYPE'] not in prior_codes_data:
                    prior_codes_data[code_data['CODE_TYPE']] = {}
                prior_codes_data[code_data['CODE_TYPE']][code_data['CODE']] = code_data

            with open(self.codes_file_name, 'w') as f:
                f.write(','.join(['REVIEWED', 'CODE_TYPE', 'CODE', 'ATTRIBUTE', 'VALUE1', 'COUNT', 'EXAMPLES']) + '\n')
                for code_type in ['ENTITY_TYPE', 'CONTACT_TYPE', 'COUNTRY_CONTEXT', 'IDENTIFIER_TYPE', 'WEAK_IDENTIFIER_TYPE', 'COUNTRY_CODE']:
                    if code_type in prior_codes_data:
                        for code in sorted(prior_codes_data[code_type]):
                            code_data = prior_codes_data[code_type][code]
                            if not code_data['COUNT']: 
                                code_data['COUNT'] = 0
                            if not code_data['EXAMPLES']: 
                                code_data['EXAMPLES'] = ''
                            code_record = [code_data['REVIEWED'],
                                           code_data['CODE_TYPE'], 
                                           code_data['CODE'], 
                                           code_data['ATTRIBUTE'], 
                                           code_data['VALUE1'], 
                                           str(code_data['COUNT']), 
                                           code_data['EXAMPLES'] if type(code_data['EXAMPLES']) != list else ' | '.join(code_data['EXAMPLES'])
                                          ]
                            f.write(','.join(code_record) + '\n')

    #----------------------------------------
    def map(self, input_rows):
        #print(json.dumps(input_rows, indent=4))

        json_data = {}
        json_data['DATA_SOURCE'] = 'SAYARI'
        json_data['RECORD_ID'] = input_rows[0]['entity_id']

        #--initial lists of senzing attributes
        json_data['ATTRIBUTE_LIST'] = []
        json_data['CONTACT_METHODS'] = []
        json_data['IDENTIFIER_LIST'] = []

        #--temporary list of payload attributes
        json_data['distinct_name_list'] = []
        json_data['distinct_address_list'] = [] 
        json_data['status_list'] = [] 
        json_data['company_type_list'] = [] 

        for input_row in input_rows:
            json_data = mapper.map_row(input_row, json_data)

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

                #--truncate long names
                this_name = json_data['distinct_name_list'][i]
                if len(this_name.split()) > 15:
                    self.update_stat('TRUNCATIONS', 'longNameCnt', json_data['RECORD_ID'] + ' [' + this_name + ']')
                    this_name = ' '.join(this_name.split()[:14]) + ' <truncated>'

                json_data['NAME_LIST'].append({name_label + '_' + name_attr: this_name})

                #--truncate too many names
                if i > 25:
                    self.update_stat('TRUNCATIONS', 'tooManyNames', json_data['RECORD_ID'] + ' [' + str(len(json_data['distinct_name_list'])) + ']')
                    break


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
        if self.relation_dbo:
            json_data['RELATIONSHIPS'] = [{
                'REL_ANCHOR_DOMAIN': 'SAYARI',
                'REL_ANCHOR_KEY': json_data['RECORD_ID']
            }]
            #if json_data['RECORD_ID'] in ('0Z4OIBVfSTjbIfCBaLzvSg', 'ciNgIv0eGHGH-OVDaUwX8Q', 'VJh3jW0Yk-m7iYUlEzYT4A'):
            #    print('-'*50 + '\n', json_data['NAME_LIST'][0])
            relation_rows = self.relation_dbo.cursor().execute('select * from relationships where src = ?', (json_data['RECORD_ID'],)).fetchall()
            for relation_row in relation_rows:
                #if json_data['RECORD_ID'] in ('0Z4OIBVfSTjbIfCBaLzvSg', 'ciNgIv0eGHGH-OVDaUwX8Q', 'VJh3jW0Yk-m7iYUlEzYT4A'):
                #    print(json.dumps(relation_row, indent=4))
                rel_pointer_data = {'REL_POINTER_DOMAIN': 'SAYARI',
                                    'REL_POINTER_KEY': relation_row[1],
                                    'REL_POINTER_ROLE': relation_row[2]
                                   }
                if relation_row[4]: #--from_date
                    rel_pointer_data['REL_POINTER_FROM_DATE'] = relation_row[4]
                if relation_row[5]: #--thru_date
                    rel_pointer_data['REL_POINTER_THRU_DATE'] = relation_row[5]
                    try:
                        if dateparse(relation_row[5]) < datetime.now():
                            rel_pointer_data['REL_POINTER_ROLE'] = '(former) ' + rel_pointer_data['REL_POINTER_ROLE']
                            #input(json.dumps(rel_pointer_data, indent=4))
                    except: pass
                if relation_row[8]: #--shares
                    percentage = 0
                    try: 
                        share_data = json.loads(relation_row[8])
                        for share_record in share_data:
                            if 'percentage' in share_record:
                                percentage = share_record['percentage']
                                break
                    except: 
                        pass
                        #print('-- bad share data on relation --')
                        #print(relation_row)
                    if percentage: 
                        rel_pointer_data['REL_POINTER_ROLE'] = str(percentage) + ' ' + rel_pointer_data['REL_POINTER_ROLE']

                json_data['RELATIONSHIPS'].append(rel_pointer_data)

        return json_data

    #----------------------------------------
    def map_row(self, raw_data, json_data):

        if args.debug:
            print('-' * 50)
            json.dumps(raw_data, indent=4)

        ##--clean values
        #for attribute in raw_data:
        #    raw_data[attribute] = self.clean_value(raw_data[attribute])

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
        if int(raw_data['i']) == 0:
   
            #--standardize the record type to senzing
            # columnName: type
            # 100.0 populated, 0.0 unique
            #      company (1380456)
            #      person (862138)
            #      tradename (101304)
            #      vessel (1449)
            #      aircraft (1267)
            entity_type = raw_data['type'].upper()
            if 'ENTITY_TYPE' not in self.code_conversion_data:
                self.code_conversion_data['ENTITY_TYPE'] = {}
            if entity_type not in self.code_conversion_data['ENTITY_TYPE']:
                self.add_code_record('ENTITY_TYPE', entity_type, 'RECORD_TYPE', entity_type.upper())
            self.update_sayari_code_stats('ENTITY_TYPE', entity_type, '')

            json_data['RECORD_TYPE'] = self.code_conversion_data['ENTITY_TYPE'][entity_type]['VALUE1']
            self.update_stat('ENTITY_TYPE', entity_type)

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
            if type(raw_data['sanctioned']) == str:
                json_data['sanctioned'] = 'Yes' if raw_data['sanctioned'].upper() == 'TRUE' else 'No'
            else:
                json_data['sanctioned'] = 'Yes' if raw_data['sanctioned'] else 'No'

            # columnName: pep
            # 100.0 populated, 0.0 unique
            #      false (2346614)
            if type(raw_data['pep']) == str:
                json_data['pep'] = 'Yes' if raw_data['pep'].upper() == 'TRUE' else 'No'
            else:
                json_data['pep'] = 'Yes' if raw_data['pep'] else 'No'

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
                if type(raw_data['source_counts']) == str and raw_data['source_counts'][0:1] in ['{', '[']:
                    raw_data['source_counts'] = json.loads(raw_data['source_counts'])
                if type(raw_data['source_counts']) == dict:
                    json_data['sources'] = ' | '.join(sorted(list(raw_data['source_counts'].keys())))
                elif type(raw_data['source_counts']) == list:
                    sub_list = []
                    for sub_item in raw_data['source_counts']:
                        sub_list.append(sub_item[0])
                    json_data['sources'] = ' | '.join(sorted(sub_list))

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
            if raw_data['shares'] and args.extended_format: 
                if type(raw_data['shares']) == str and raw_data['shares'][0:1] == '{':
                    raw_data['shares'] = json.loads(raw_data['shares'])
                if type(raw_data['shares']) == dict:
                    json_data['shares'] = json.dumps(raw_data['shares'])


            #print(json.dumps(json_data, indent=4))

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
            name = self.get_value_only(raw_data, 'name')
            if name and name not in json_data['distinct_name_list']:
                json_data['distinct_name_list'].append(name)

        # columnName: address
        # 33.35 populated, 44.89 unique
        #      {"value":"Αρχ. Μακαρίου ΙΙΙ, 284, FORTUNA COURT BLOCK B, Floor 2, 3105, Λεμεσός, Κύπρος","house":"Fortuna Court Block B","house_number":"3105","level":"Floor 2","road":"Αρχ. Μακαρίου Ιιι","city":"Λεμεσός","country":"Κύπρος"} (4983)
        #      {"value":"Αγίου Παύλου, 15, LEDRA HOUSE, 'Αγιος Ανδρέας, 1105, Λευκωσία, Κύπρος","house":"Ledra House 'αγιος Ανδρέας","house_number":"15","road":"Αγίου Παύλου","state":"Λευκωσία","postcode":"1105","country":"Κύπρος"} (3698)
        #      {"value":"Αρχ. Μακαρίου ΙΙΙ, 155, PROTEAS HOUSE, Floor 5, 3026, Λεμεσός, Κύπρος","house":"Proteas House","house_number":"155","level":"Floor 5","road":"Αρχ. Μακαρίου Ιιι","city":"Λεμεσός","postcode":"3026","country":"Κύπρος"} (2960)
        #      {"value":"Θεμιστοκλή Δέρβη, 3, JULIA HOUSE, 1066, Λευκωσία, Κύπρος","house":"Julia House","house_number":"3","road":"Θεμιστοκλή Δέρβη","state":"Λευκωσία","postcode":"1066","country":"Κύπρος"} (2796)
        #      {"value":"Γρ. Ξενοπούλου, 17, 3106, Λεμεσός, Κύπρος","house_number":"17","road":"Γρ. Ξενοπούλου","city":"Λεμεσός","postcode":"3106","country":"Κύπρος"} (2698)
        if raw_data['address']:
            address = self.get_value_only(raw_data, 'address')
            if address and address not in json_data['distinct_address_list']:
                extra_keys = []
                if 'extra' in raw_data['address'] and raw_data['address']['extra']:

                    if type(raw_data['address']['extra']) == list:
                        #[['Address Type', 'Place of Birth']]
                        extra_keys.extend(raw_data['address']['extra'])
                        for sub_list in raw_data['address']['extra']:
                            extra_keys.append([x.upper() for x in sub_list])
                            self.update_stat('?-ADDRESS_EXTRA_KEYS', ', '.join(sub_list))

                    elif type(raw_data['address']['extra']) == dict:
                        for key in raw_data['address']['extra'].keys():
                            extra_keys.append(key.upper())
                            self.update_stat('?-ADDRESS_EXTRA_KEYS', key)

                if 'PLACE_OF_BIRTH' in extra_keys:
                    if 'PLACE_OF_BIRTH' not in json_data:
                        json_data['PLACE_OF_BIRTH'] = address
                    elif json_data['PLACE_OF_BIRTH'] != address:
                        print(json_data['PLACE_OF_BIRTH'], '\n', address)
                        input('wait')
                else:
                    json_data['distinct_address_list'].append(address)

        # columnName: date_of_birth
        # 0.28 populated, 72.78 unique
        #      {"value":"1963"} (47)
        #      {"value":"1958"} (44)
        #      {"value":"1962"} (42)
        #      {"value":"1961"} (42)
        #      {"value":"1964"} (41)
        if raw_data['date_of_birth']:
            dob = self.get_value_only(raw_data, 'date_of_birth')
            if dob: 
                mapped_dict = {'DATE_OF_BIRTH': dob}
                if mapped_dict not in json_data['ATTRIBUTE_LIST']:
                    json_data['ATTRIBUTE_LIST'].append(mapped_dict)

        # columnName: gender
        # 0.1 populated, 0.09 unique
        #      {"value":"male"} (2075)
        #      {"value":"female"} (204)
        if raw_data['gender']:
            gender = self.get_value_only(raw_data, 'gender')
            if gender: 
                mapped_dict = {'GENDER': gender}
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
            contact_value, contact_type = self.get_value_and_type(raw_data, 'contact')
            if contact_value:
                if contact_type:
                    contact_type = contact_type.upper()
                else:
                    contact_type = 'NULL'
                if 'CONTACT_TYPE' not in self.code_conversion_data:
                    self.code_conversion_data['CONTACT_TYPE'] = {}
                if contact_type not in self.code_conversion_data['CONTACT_TYPE']:
                    self.add_code_record('CONTACT_TYPE', contact_type, '<unknown>', '')
                self.update_sayari_code_stats('CONTACT_TYPE', contact_type, contact_value)
                senzing_attr = self.code_conversion_data['CONTACT_TYPE'][contact_type]['ATTRIBUTE']
                if senzing_attr == '<unknown>':
                    self.update_stat('?-CONTACT_TYPES', contact_type, raw_data['contact'])
                else:
                    mapped_dict = {senzing_attr: contact_value}
                    if senzing_attr == 'PHONE_NUMBER' and self.code_conversion_data['CONTACT_TYPE'][contact_type]['VALUE1']:
                        mapped_dict['PHONE_TYPE'] = self.code_conversion_data['CONTACT_TYPE'][contact_type]['VALUE1']
                    if mapped_dict not in json_data['CONTACT_METHODS']: 
                        json_data['CONTACT_METHODS'].append(mapped_dict)

        # columnName: identifier
        # 26.92 populated, 100.0 unique
        #      {"value":"20468A","type":"malta_national_id"} (2)
        #      {"value":"17258A","type":"malta_national_id"} (2)
        #      {"value":"AB/26/84/65","type":"malta_accountancy_registration_id"} (2)
        #      {"value":"AB/26/84/62","type":"malta_accountancy_registration_id"} (2)
        #      {"value":"312764M","type":"malta_national_id"} (2)


        #https://docs.sayari.com/enums/#identifier-type-enum

        if raw_data['identifier']:
            mapped_dict = self.map_identifier('IDENTIFIER_TYPE', raw_data['identifier'])
            if 'payload' in mapped_dict:
                json_data[mapped_dict['payload']] = mapped_dict[mapped_dict['payload']]
            elif mapped_dict and mapped_dict not in json_data['IDENTIFIER_LIST']: 
                json_data['IDENTIFIER_LIST'].append(mapped_dict)

        # columnName: weak_identifier
        # 5.07 populated, 73.08 unique
        #      {"extra":{"Type":"Additional Sanctions Information -"},"value":"Subject to Secondary Sanctions","type":"unknown"} (1721)
        #      {"extra":{"Type":"Warrant Number"},"value":"19007","type":"unknown"} (1172)
        #      {"extra":{"Type":"Warrant Number"},"value":"10914","type":"unknown"} (788)
        #      {"extra":{"Type":"Warrant Number"},"value":"02286","type":"unknown"} (760)
        #      {"extra":{"Type":"Warrant Number"},"value":"19808","type":"unknown"} (615)

        #### weak because they are partial or known to be shared!! 
        if raw_data['weak_identifier']:
            mapped_dict = self.map_identifier('WEAK_IDENTIFIER_TYPE', raw_data['weak_identifier'])
            if 'payload' in mapped_dict:
                json_data[mapped_dict['payload']] = mapped_dict[mapped_dict['payload']]
            elif mapped_dict and mapped_dict not in json_data['IDENTIFIER_LIST']: 
                json_data['IDENTIFIER_LIST'].append(mapped_dict)

        return json_data

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
                if 'unknown' in mapped_dict:
                    raw_attr = [key for key in list(mapped_dict.keys()) if key != 'unknown'][0]
                    if raw_attr not in json_data:
                        json_data[raw_attr] = mapped_dict[raw_attr]
                    else:
                        json_data[raw_attr] += (' | ' + mapped_dict[raw_attr])
                else:
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
            if 'extra' in parsed_data and parsed_data['extra']:
                for key in parsed_data['extra']:
                    if key not in json_data:
                        json_data[key] = parsed_data['extra'][key]
                    else:
                        temp_data = json_data[key]
                        if temp_data[0] + temp_data[-1] != '[]':  #--PROTECT THIS!
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
    def get_value_only(self, field_dict, field_tag):
        if type(field_dict[field_tag]) == str and field_dict[field_tag][0:1] == '{':
            field_dict[field_tag] = json.loads(field_dict[field_tag])
        if type(field_dict[field_tag]) == dict and 'value' in field_dict[field_tag]:
            return field_dict[field_tag]['value']
        else:
            self.update_stat(f'!-{field_tag}', field_dict[field_tag])
            return None

    #-----------------------------------
    def get_value_and_type(self, field_dict, field_tag):
        field_value = None
        field_type = None
        if type(field_dict[field_tag]) == str and field_dict[field_tag][0:1] == '{':
            field_dict[field_tag] = json.loads(field_dict[field_tag])
        if type(field_dict[field_tag]) == dict and 'value' in field_dict[field_tag]:
            field_value = field_dict[field_tag]['value']
        if type(field_dict[field_tag]) == dict and 'type' in field_dict[field_tag]:
            field_type = field_dict[field_tag]['type']
        if not field_value:
            self.update_stat(f'!-{field_tag}', field_dict[field_tag])
        return field_value, field_type
            
    #-----------------------------------
    def map_identifier(self, code_type, identifier_data):

        if type(identifier_data) == str and identifier_data[0:1] == '{':
            identifier_data = json.loads(identifier_data)
        if type(identifier_data) == dict and 'value' in identifier_data:

            id_value = identifier_data['value']
            if 'extra' in identifier_data and identifier_data['extra'] and 'Type' in identifier_data['extra']:
                id_type = identifier_data['extra']['Type']
            elif 'type' in identifier_data:
                id_type = identifier_data['type']
            else:
                id_type = 'NOT_SPECIFIED'

            #--direct mapping
            id_type = id_type.upper()
            if code_type not in self.code_conversion_data:
                self.code_conversion_data[code_type] = {}
            if id_type in self.code_conversion_data[code_type]:
                senzing_attr = self.code_conversion_data[code_type][id_type]['ATTRIBUTE']
                country_code = self.code_conversion_data[code_type][id_type]['VALUE1']
                ##self.update_stat(f'=-{code_type}', f'{id_type} | {senzing_attr}', id_value)

            #--try to figure it out,. e.g. "AUS-AUSTRALIAN PASSPORT"
            else:

                if 'PASSPORT' in id_type:
                    senzing_attr = 'PASSPORT'
                else:
                    senzing_attr = 'OTHER_ID'

                self.update_stat(f'?-{code_type}', f'{id_type} | {senzing_attr}', id_value)

                if len(id_type) > 4 and id_type[3] in ('-', '_', ' ') and id_type[0:3] in self.code_conversion_data['COUNTRY_CODE']:
                    country_code = id_type[0:3]
                else:
                    country_code = ''

                self.add_code_record(code_type, id_type, senzing_attr, country_code)

            self.update_sayari_code_stats(code_type, id_type, id_value)

            if senzing_attr in ('OTHER_ID', 'NATIONAL_ID', 'TAX_ID', 'DRIVERS_LICENSE'): 
                mapped_dict = {senzing_attr + '_NUMBER': id_value}
                if country_code:
                    mapped_dict[senzing_attr + '_COUNTRY'] = country_code
                if senzing_attr in ('OTHER_ID', 'NATIONAL_ID'): #--, 'NATIONAL_ID') not an attribute at this time
                    mapped_dict[senzing_attr + '_TYPE'] = id_type
            elif senzing_attr:
                mapped_dict = {senzing_attr: id_value, 'payload': senzing_attr}
            else:
                mapped_dict = {id_type: id_value, 'payload': id_type}

            return mapped_dict
        else:
            self.update_stat(f'!-{code_type}', identifier_data)
            return None

    #-----------------------------------
    def map_country(self, code_type, country_dict):
        if 'context' in country_dict and 'value' in country_dict and country_dict['value']:
            #--direct mapping
            country_context = country_dict['context'].upper()

            if country_context not in self.code_conversion_data[code_type]:
                self.add_code_record(code_type, country_context, '<unknown>', '')
            self.update_sayari_code_stats(code_type, country_context, country_dict['value'])

            if self.code_conversion_data[code_type][country_context]['ATTRIBUTE'] == '<unknown>':
                self.update_stat('?' + code_type, country_context, country_dict['value'])
                mapped_dict = {country_context: country_dict['value'], 'unknown': 'yes'}
            else:
                senzing_attr = self.code_conversion_data[code_type][country_context]['ATTRIBUTE']
                mapped_dict = {senzing_attr: country_dict['value']}
            return mapped_dict
        else:
            self.update_stat('!' + code_type, json.dumps(country_dict))
            return None

    #----------------------------------------
    def load_reference_data(self):
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

    #-----------------------------------
    def add_code_record(self, code_type, raw_code, attribute, value1):
        #new_code_record = ['N', code_type, raw_code, attribute, value1, 0, '']
        #self.new_code_records.append(new_code_record)
        if code_type not in self.code_conversion_data:
            self.code_conversion_data[code_type] = {}
        self.code_conversion_data[code_type][raw_code] = {'REVIEWED': 'N', 'CODE_TYPE': code_type, 'CODE': raw_code, 'ATTRIBUTE': attribute, 'VALUE1': value1, 'COUNT': 0, 'EXAMPLES': []}
        self.new_code_records.append(self.code_conversion_data[code_type][raw_code])
        self.unmapped_code_count += 1
        return

    #-----------------------------------
    def update_sayari_code_stats(self, code_type, raw_code, example_value):
        if type(self.code_conversion_data[code_type][raw_code]['COUNT']) != int:
            if not self.code_conversion_data[code_type][raw_code]['COUNT']:
                self.code_conversion_data[code_type][raw_code]['COUNT'] = 0
            else:
                self.code_conversion_data[code_type][raw_code]['COUNT'] = int(self.code_conversion_data[code_type][raw_code]['COUNT'])
        self.code_conversion_data[code_type][raw_code]['COUNT'] += 1

        if not self.code_conversion_data[code_type][raw_code]['EXAMPLES']:
            self.code_conversion_data[code_type][raw_code]['EXAMPLES'] = []
        if len(self.code_conversion_data[code_type][raw_code]['EXAMPLES']) < 10 and example_value not in self.code_conversion_data[code_type][raw_code]['EXAMPLES']:
            if type(self.code_conversion_data[code_type][raw_code]['EXAMPLES']) != list:
                self.code_conversion_data[code_type][raw_code]['EXAMPLES'] = self.code_conversion_data[code_type][raw_code]['EXAMPLES'].split(' | ')
            self.code_conversion_data[code_type][raw_code]['EXAMPLES'].append(example_value)
        return

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
        #if (not cat1) or (not cat2):
        #    print(f'[{cat1}], [{cat2}], [{example}]')
        #    input('wait')
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
def load_relationships(relationdb_name, relationship_file_list):

    if os.path.exists(relationdb_name):
        os.remove(relationdb_name)

    print(f'\nLoading {len(relationship_file_list)} relationship files ...\n')
    relation_dbo = sqlite3.connect(relationdb_name, isolation_level=None)
    relation_dbo.cursor().execute("PRAGMA journal_mode=wal")
    relation_dbo.cursor().execute("PRAGMA synchronous=0")

    for relationship_file_name in relationship_file_list:
        print(relationship_file_name, '...', end='', flush=True)
        timer_start = time.time()
        base_file_name, file_extension = os.path.splitext(relationship_file_name)
        compressed_file = file_extension.upper() == '.GZ'
        if compressed_file:
            df = pandas.read_csv(relationship_file_name, low_memory=False, encoding='utf-8', compression='gzip')
        else:
            df = pandas.read_csv(relationship_file_name, low_memory=False, encoding='utf-8')

        #df[['src', 'dst', 'type']].to_sql('relationships', relation_dbo, index=False, method='multi', chunksize=10000, if_exists='append')
        df.to_sql('relationships', relation_dbo, index=False, method='multi', chunksize=10000, if_exists='append')
        print(f' completed in {round((time.time() - timer_start) / 60, 1)} minutes')

        #display_process_stats(main_pid, 'after loading this file')
        if shut_down:
            break

    if not shut_down:
        print('\nindexing relationships ...', end='', flush = True)
        timer_start = time.time()
        relation_dbo.cursor().execute('create index ix_relationships on relationships (src)')
        print(f' completed in {round((time.time() - timer_start) / 60, 1)} minutes')

    relation_dbo.cursor().execute('create table finished (dummy integer)')
    relation_dbo.close()


# -----------------------------------
def remove_json_nulls (d):
    if isinstance(d, dict):
        for k, v in list(d.items()):
            if v is None:
                del d[k]
            elif type(v) == str and v.upper() == 'NULL':
                del d[k]
            else:
                remove_json_nulls(v)
    if isinstance(d, list):
        for v in d:
            remove_json_nulls(v)
    return d

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

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_path', help='the directory that contains the Sayari files')
    parser.add_argument('-o', '--output_path', help='the directory to write the mapped json files to')
    parser.add_argument('-r', '--relationdb_name', help='the relationship db file to build or use')
    parser.add_argument('-l', '--log_file', help='optional name of the statistics log file')
    parser.add_argument('-x', '--extended_format', action='store_true', default=False, help='map all the fields including finances and other information')
    parser.add_argument('-D', '--debug', action='store_true', default=False, help='run in debug mode')
    parser.add_argument('-U', '--unattended', action='store_true', default=False, help='dont ask questions')
    parser.add_argument('-f', '--filter_file', help='optional name of a file containing a list of nodes')
    args = parser.parse_args()

    if not args.input_path:
        print('\nPlease supply a valid input file path on the command line\n')
        sys.exit(1)
    if not args.output_path:
        print('\nPlease supply a valid output file path on the command line\n') 
        sys.exit(1)
    if not args.relationdb_name:
        print('\nPlease supply a valid relationship database file name on the command line\n') 
        sys.exit(1)

    node_filter_list = None
    if args.filter_file:
        try:
            with open(args.filter_file, 'r') as f:
                node_filter_list = [l[0:-1].strip() for l in f.readlines() if len(l) > 5]
        except Exception as err:
            print(f'\nfilter file error: {err}\n')
            sys.exit(1)
        print(f'\nFiltering for {len(node_filter_list)} nodes:\n')
        print(json.dumps(node_filter_list, indent=4))
        print()
        if not args.unattended:
            response = input('OK to proceed? (y/n) ')
            if not response.upper().startswith('Y'):
                print(f'\nProcess aborted!')
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

    # count files and confirm to proceed
    print('\nQualifying files ...\n')
    entity_file_list = []
    relationship_file_list = []
    for input_file_name in sorted(input_file_list):
        if not os.path.splitext(input_file_name)[1].upper() in ('.CSV', '.GZ'):
            continue
        if 'relationships' in input_file_name:
            print('relationship ->', input_file_name)
            relationship_file_list.append(input_file_name)
        elif 'entities' in input_file_name or not os.path.isdir(args.input_path):
            print('entity ->',input_file_name)
            entity_file_list.append(input_file_name)
    print(f'\n{len(entity_file_list)} entity files and {len(relationship_file_list)} relationship files found\n')
    if not entity_file_list and not relationship_file_list:
        print(f'\nNo Sayari files found at {args.input_path}\n')
        print('\tSayari files must end with .csv or .gz ... parquet files are not currenty supported.')
        print('\tSayari entity files must have "entities" somewhere in the file name.')
        print('\tSayari relationship files must have "relationships" somewhere in the file name.\n')
        sys.exit(1)
    if not args.unattended:
        response = input('OK to proceed? (y/n) ')
        if not response.upper().startswith('Y'):
            print(f'\nProcess aborted!')
            sys.exit(1)

    #--create the mapper and warn if unmapped codes
    mapper = mapper('sayari_codes.csv', args.relationdb_name)
    if mapper.unmapped_code_count > 0 and entity_file_list and not args.unattended:
        print(f'\nWARNING: there are {mapper.unmapped_code_count} unmapped codes in sayari_codes.csv!!\n')
        response = input('Do you wish to continue anyway? (y/n) ')
        if not response.upper().startswith('Y'):
            print('\nProcess aborted!\n')
            sys.exit(1)

    #--determine if relationships need to be loaded
    load_relationships_files = True
    if os.path.exists(args.relationdb_name):
        relation_dbo = sqlite3.connect(args.relationdb_name, isolation_level=None)
        was_finished = relation_dbo.cursor().execute("select name from sqlite_master where type='table' and name='finished'").fetchone()
        relation_dbo.close()
        if was_finished: 
            load_relationships_files = False
            if relationship_file_list and not args.unattended:
                response = input('\nComplete relation database exists, type "REBUILD" if you want to rebuild it ... ')
                if response.upper() == 'REBUILD':
                    load_relationships_files = True

    if load_relationships_files:
        if not relationship_file_list:
            if not entity_file_list:
                print('\nNo relationship files to load!\n')
                sys.exit(1)
            else:
                if not args.unattended:
                    response = input('\nType YES if you want to map entities without any relationships ... ') 
                    if response.upper() == 'YES':
                        args.relationdb_name = None
                    else:
                        print('\nProcess aborted!\n')
                        sys.exit(1)

        if args.relationdb_name:
            load_relationships(args.relationdb_name, relationship_file_list)
            if shut_down:
                print(f'\nProcess aborted!')
                sys.exit(1)

    mapper.open_relation_db(args.relationdb_name)

    proc_start_time = time.time()
    input_file_count = 0
    for input_file_name in entity_file_list:
        print(f'\nProcessing {input_file_name} ...\n')
        input_file_count += 1
        base_input_file_name = os.path.basename(input_file_name)

        timer_start = time.time()
        base_file_name, file_extension = os.path.splitext(input_file_name)
        compressed_file = file_extension.upper() == '.GZ'
        if compressed_file:
            base_file_name, file_extension = os.path.splitext(base_file_name)

        if compressed_file:
            input_file_handle = gzip.open(input_file_name, 'r')
            csv_reader = csv.DictReader(io.TextIOWrapper(io.BufferedReader(input_file_handle), encoding='utf-8', errors='ignore'))
        else:
            input_file_handle = open(input_file_name, 'r')
            csv_reader = csv.DictReader(input_file_handle, dialect='excel')

        #try: input_row = next(csv_reader) #--skip header row
        #except: input_row = None
        try: input_row = next(csv_reader) #--get first row
        except: input_row = None

        #display_process_stats(main_pid, 'this file')

        file_start_time = time.time()
        if node_filter_list:
            output_file_name = 'filtered_nodes.json'
            output_file_handle = open(output_file_name, 'a', encoding='utf-8')

            csv_output_file_name = 'filtered_nodes.csv'
            csv_output_file_existed = os.path.exists(csv_output_file_name)
            csv_output_file_handle = open(csv_output_file_name, 'a', encoding='utf-8')
            csv_output_file_writer = csv.DictWriter(csv_output_file_handle, csv_reader.fieldnames)
            if not csv_output_file_existed:
                csv_output_file_writer.writeheader()
        else:
            output_file_name = os.path.splitext(args.output_path + os.path.split(base_file_name)[1])[0] + '.json'
            if compressed_file:
                output_file_handle = gzip.open(output_file_name + '.gz', 'wb')
            else:
                output_file_handle = open(output_file_name, 'w', encoding='utf-8')

        batch_start_time = time.time()
        batch_input_list = []
        batch_output_list = []

        input_row_count = 0
        output_row_count = 0
        while input_row:

            #--there can be multiple rows for the same entity
            last_entity_id = input_row['entity_id']
            input_rows = []
            while input_row and input_row['entity_id'] == last_entity_id:
                input_row_count += 1
                input_rows.append(input_row)
                if args.debug:
                    print()
                    print(json.dumps(input_row, indent=4))
                try: input_row = next(csv_reader)
                except: input_row = None

            if node_filter_list and last_entity_id not in node_filter_list:
                pass
            else:
                if node_filter_list: #--just special code to capture a list of the raw records 
                    for temp_row in input_rows:
                        csv_output_file_writer.writerow(temp_row)

                batch_output_list.append(mapper.map(input_rows))
                output_row_count += 1

            if output_row_count % 100000 == 0 or not input_row:
                if batch_output_list:
                    if compressed_file and not node_filter_list:
                        output_file_handle.write(('\n'.join([json.dumps(json_data) for json_data in batch_output_list]) + '\n').encode('utf-8'))
                    else:
                        output_file_handle.write('\n'.join([json.dumps(json_data) for json_data in batch_output_list]) + '\n')

                if mapper.new_code_records:
                    extra_info = f'\tWARNING: {mapper.unmapped_code_count} unmapped sayari codes!'
                else:
                    extra_info = ''

                elapsed_mins = round((time.time() - batch_start_time) / 60, 1)
                print(f'{base_input_file_name} {output_row_count} rows written, {input_row_count} rows processed in {elapsed_mins} minutes {extra_info}')

                batch_start_time = time.time()
                batch_input_list = []
                batch_output_list = []

            if shut_down:
                break

        output_file_handle.close()
        if node_filter_list:
            csv_output_file_handle.close()

        if shut_down:
            break

        elapsed_mins = round((time.time() - file_start_time) / 60, 1)
        run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
        print('\n%s files read, %s rows written, %s rows processed, %s\n' % (input_file_count, output_row_count, input_row_count, run_status))

    if input_file_count:

        #--append any new codes to the sayari_codes.csv
        mapper.save_codes_file()

        #--write statistics file
        if args.log_file: 
            with open(args.log_file, 'w') as outfile:
                json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
            print('Mapping stats written to %s\n' % args.log_file)

    print('')
    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    if shut_down == 0:
        print('Process completed successfully in %s minutes' % elapsed_mins)
    else:
        print('Process aborted after %s minutes!' % elapsed_mins)
    print('')

    #display_process_stats(main_pid, 'final')

    sys.exit(0)

