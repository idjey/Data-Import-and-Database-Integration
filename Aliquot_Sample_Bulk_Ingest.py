import psycopg2
import psycopg2.extras
import psycopg2.sql
from sshtunnel import SSHTunnelForwarder, create_logger
import getpass
import pandas as pd
import numpy as np
import datetime
import re
import json
import os

# Function to import inventories from CSV files
def import_inventories(directory):
    """
    Imports all CSV files into a dictionary of dataframes
    and then concatenates them into one dataframe.

    Required header values are as follows, but after processing,
    the header must have the following values:

    participant_id, study, visit, visit_week, draw_date,
    aliquot_type, volume, unit.

    Some specific coding is done with regex to standardize participant ID values.

    :param directory: file directory where CSV files reside
    :return: concatenated dataframe
    """
    inv = {}
    for file in os.listdir(directory):
        if re.match('.*_aliquot.csv', file):
            study = re.match('(.*)_aliquot.csv', file).group(1)
            file = os.path.join(directory, file)
            df_temp = pd.read_csv(file)
            df_temp.columns = df_temp.columns.str.strip()  # strip white space
            df_temp = df_temp.rename(columns={'Globally Unique Aliquot ID': 'guid',
                                              "Study Number": "participant_id",
                                              "Protocol Number": "study",
                                              "Visit": "visit",
                                              "Visit week": "visit_week",
                                              "Date Collected": "draw_date",
                                              "Aliquot Type": "aliquot_type",
                                              "Current Amount": 'volume',
                                              "Aliquot Volume": "volume",  # RV254 uses this terminology
                                              "Aliquot Units": "unit"})

            inv[study] = df_temp

    df_aliquots = pd.concat(inv)

    # Fills patient IDs to 4 places with leading zeros, Adds P to beginning if no P exists
    df_aliquots['participant_id'] = (df_aliquots['participant_id']
                                     .apply(lambda x: "P" + str(x).zfill(4) if not re.search('^P|^T|^S', str(x)) else str(x).zfill(4))
                                     )
    # Removes V from the beginning of visit and fills to 2 places
    df_aliquots['visit'] = df_aliquots['visit'].apply(
        lambda x: re.sub("V", "", x))
    df_aliquots['visit'] = df_aliquots['visit'].apply(
        lambda x: str(x).zfill(2))

    # Removes WK from week values and fills weeks to 3 digits
    df_aliquots['visit_week'] = df_aliquots['visit_week'].apply(
        lambda x: re.sub('Wk', '', str(x)).strip().zfill(3))

    # Replace empty Visit IDs with the visit week ID (UD1 or UD2)
    is_nan = df_aliquots['visit_week'] == 'nan'
    df_aliquots['visit_week'][is_nan] = df_aliquots['visit'][is_nan].copy()

    # Removes 397-01 from leading characters of PID.
    df_aliquots['participant_id'] = df_aliquots['participant_id'].apply(
        lambda x: re.sub('397-01-', '', str(x)))

    return df_aliquots

# Function to create tables in the PostgreSQL database
def create_tables(cursor):
    """
    Create tables to store specimen data
    :param cursor: cursor in postgreSQL database
    :return: nothing
    """
    # SQL statements to create tables
    sql_study = ('CREATE TABLE IF NOT EXISTS study('
                 'id    SERIAL UNIQUE,'
                 'study VARCHAR UNIQUE,'
                 'is_open BOOL, '
                 'parent_study VARCHAR DEFAULT NULL, '
                 'create_date DATE NOT NULL DEFAULT CURRENT_DATE,'
                 'PRIMARY KEY (id)'
                 ');')

    sql_participant = ('CREATE TABLE IF NOT EXISTS participant('
                       'id  SERIAL UNIQUE,'
                       'study   VARCHAR,'
                       'participant_id  VARCHAR,'
                       'create_date DATE NOT NULL DEFAULT CURRENT_DATE,'
                       'PRIMARY KEY (study, participant_id),'
                       'FOREIGN KEY (study) REFERENCES study (study)'
                       ');')

    # ... (similar SQL statements for other tables)

    # Execute SQL statements to create tables
    cursor.execute(sql_study)
    cursor.execute(sql_participant)
    cursor.execute(sql_visit)
    cursor.execute(sql_specimen)
    cursor.execute(sql_aliquot)
    cursor.execute(sql_location)
    cursor.execute(sql_lab)
    cursor.execute(sql_sample_request)
    cursor.execute(sql_request_batch)
    cursor.execute(sql_alt_id)
    cursor.execute(sql_general_classifier)  # NOT SOLD ON THIS NAME

# Function to insert study data into the study table
def insert_study(cursor, data_, parent_study_="RV254"):
    # Get the list of unique studies
    studies = data_.study.unique()
    test = pd.DataFrame(
        [{"study": member, 'is_open': True, "parent_study": ""} for member in studies])
    children = test['study'] != parent_study_
    test['parent_study'][children] = parent_study_
    test = test.T.to_dict().values()
    # Insert unique studies to the study table
    sql_ = """
    INSERT INTO study(study, is_open, parent_study)
    VALUES %s 
    ON CONFLICT (study) DO NOTHING;"""  # Only inserts the study table if the study name doesn't already exist
    psycopg2.extras.execute_values(
        cursor, sql_, test, template="(%(study)s, %(is_open)s, %(parent_study)s)")

# Function to insert participant data into the participant table
def insert_participants(cursor, data_):
    # Get unique participants and studies to prepare for participants table
    cols = ["study", "participant_id"]
    participant_dict = data_.drop_duplicates(cols)[cols].T.to_dict().values()

    sql_ = """
    INSERT INTO participant(study, participant_id)
    VALUES %s
    ON CONFLICT (study, participant_id) DO NOTHING;"""
    psycopg2.extras.execute_values(
        cursor, sql_, participant_dict, template="(%(study)s, %(participant_id)s)")

# Function to insert visit data into the visit table
def insert_visits(cursor, data_):
    # Get unique participants, studies, visit to prepare for visits table
    cols = ["study", "participant_id", "visit"]
    visit_dict = data_.drop_duplicates(
        cols)[cols + ["visit_week"]].T.to_dict().values()

    sql = """
    INSERT INTO visit(study, participant_id, visit, visit_week)
    VALUES %s
    ON CONFLICT (study, participant_id, visit) DO NOTHING;"""

    psycopg2.extras.execute_values(cursor, sql, visit_dict, template="(%(study)s, %(participant_id)s, "
                                                                     "%(visit)s, %(visit_week)s)")

# Function to insert specimen data into the specimen table

# Function to insert specimens data into the specimen table
def insert_specimens(cursor, data_):
    cols = ["study", "participant_id", "visit",
            "draw_date", "aliquot_type", 'volume', 'unit']
    specimen_dict = data_.drop_duplicates(cols)[cols].T.to_dict().values()

    sql = """
    INSERT INTO specimen(study, participant_id, visit, draw_date, aliquot_type, volume, unit)
    VALUES %s
    ON CONFLICT (study, participant_id, draw_date, aliquot_type, volume) DO NOTHING;
    """

    psycopg2.extras.execute_values(cursor, sql, specimen_dict, template="(%(study)s, %(participant_id)s, "
                                   "%(visit)s, %(draw_date)s, "
                                   "%(aliquot_type)s, %(volume)s, "
                                   "%(unit)s)"
                                   )

# Function to insert aliquots data into the aliquot table
def insert_aliquots(cursor, data_):
    cols = ["study", "participant_id", 'aliquot_type',
            'draw_date', 'guid', 'volume', 'unit']
    aliq_dict = data_[cols].T.to_dict().values()

    sql = """
    INSERT INTO aliquot(study, participant_id, draw_date, guid, aliquot_type, volume, unit)
    VALUES %s
    ON CONFLICT (guid) DO NOTHING;
    """

    psycopg2.extras.execute_values(cursor, sql, aliq_dict, template="(%(study)s, %(participant_id)s, "
                                   "%(draw_date)s, %(guid)s, "
                                   "%(aliquot_type)s, %(volume)s, "
                                   "%(unit)s)"
                                   )

# Function to import alternate IDs data from an Excel file
def import_alt_ids(filename, sheetname='sub_id_jvs', key_col='RV254_ID'):
    var_key = 'alt_study'
    var_value = 'alt_participant_id'
    df_id_mapper = pd.read_excel(filename, sheetname=sheetname)
    df_id_mapper = df_id_mapper.melt(
        id_vars=[key_col], var_name=var_key, value_name=var_value).dropna()
    df_id_mapper[var_key] = df_id_mapper[var_key].apply(
        lambda x: re.sub("_ID", "", x))

    dict_id_mapper = {}
    for index, row in df_id_mapper.iterrows():
        if row[key_col] in dict_id_mapper.keys():
            dict_id_mapper[row[key_col]][row[var_key]] = row[var_value]
        else:
            dict_id_mapper[row[key_col]] = {row[var_key]: row[var_value]}

    d = []
    for key, value in dict_id_mapper.items():
        d.append([key, json.dumps(value)])

    df_ids = pd.DataFrame(d)
    df_ids.columns = ["participant_id", 'alt_ids_json']
    df_ids['init_study'] = 'RV254'
    df_ids = pd.merge(df_id_mapper, df_ids, right_on='participant_id',
                      left_on='RV254_ID').sort_values('participant_id')
    id_dict = df_ids.T.to_dict().values()
    pd.unique(df_ids['alt_study'].sort_values())

    return id_dict

# Function to insert alternate IDs data into the alt_id table
def insert_alt_ids(cursor, id_dict_):
    sql = ('INSERT INTO alt_id(init_study, init_participant_id, alt_ids_json, alt_study, alt_participant_id) '
           'VALUES %s '
           'ON CONFLICT (init_study, init_participant_id, alt_study) DO NOTHING;')
    psycopg2.extras.execute_values(cursor, sql, id_dict_,
                                   template="(%(init_study)s, %(participant_id)s, %(alt_ids_json)s,"
                                            "%(alt_study)s, %(alt_participant_id)s)")

# Function to import general classifier data from a CSV file
def import_general_classifiers(filename):
    df_classifiers = pd.read_csv(filename, true_values='Y', false_values='N')

    # Change from Y or N to True and False
    is_thai = df_classifiers['is_thai'] == 'Y'
    not_thai = df_classifiers['is_thai'] == 'N'
    nan_thai = df_classifiers['is_thai'].isnull()

    df_classifiers['is_thai'][is_thai] = True
    df_classifiers['is_thai'][not_thai] = False
    df_classifiers['is_thai'][nan_thai] = None

    return df_classifiers.T.to_dict().values()

# Function to insert general classifier data into the general_classifier table
def insert_general_classifiers(cursor, class_dict_):
    sql = (
        'INSERT INTO general_classifier(study, participant_id, gender, hiv_subtype, fiebig_stage, '
        'fourth_gen_stage, is_thai, risk, first_arv_regimen) '
        'VALUES %s'
        'ON CONFLICT(study, participant_id) DO NOTHING;')

    psycopg2.extras.execute_values(cursor, sql, class_dict_,
                                   template='(%(study)s, %(participant_id)s, %(gender)s, '
                                            '%(hiv_subtype)s, %(fiebig_stage)s, %(fourth_gen_stage)s, '
                                            '%(is_thai)s, %(risk)s, %(first_arv_regimen)s)'
                                   )

# SSH tunnel configuration
user_name_ssh = "**************"
pw_ssh = '****************'

with SSHTunnelForwarder(
        ("lkinstance.hivresearch.org", 22),
        ssh_username=user_name_ssh,
        ssh_password=pw_ssh,
        remote_bind_address=("localhost", 5432),
        local_bind_address=("localhost", 6452)) as tunnel:

    tunnel.start()

    # Connect to PostgreSQL database on the tunnel server
    hostname = '127.0.0.1'
    username = 'postgres'
    password = '**************'  # Set your database password
    database = 'test_jvs'

    with psycopg2.connect(host=tunnel.local_bind_host, port=tunnel.local_bind_port,
                          user=username, password=password, dbname=database) as myConnection:

        cur = myConnection.cursor()
        # Create Tables if not exist
        create_tables(cur)

        # Import specimens
        # Make sure you have 'df' defined before calling this function
        # insert_specimens(cursor=cur, data_=df)

        # Import aliquots
        # Make sure you have 'df' defined before calling this function
        # insert_aliquots(cursor=cur, data_=df)

        # Import the alternate IDs
        # filename = 'data/meta/sub_study_mapper.xlsx'
        # id_dict = import_alt_ids(filename)
        # insert_alt_ids(cursor=cur, id_dict_=id_dict)

        # Import general classifier table
        # filename = 'data/labkey_filter_vars/filter_vars.csv'
        # classifier_dict = import_general_classifiers(filename)
        # insert_general_classifiers(cursor=cur, class_dict_=classifier_dict)
        filename = 'data/inv/RV254_shipments_102519.xlsx'
        shipments = pd.read_excel(filename)
        shipments['Ship POC'] = shipments['Ship POC'].apply(
            lambda x: x.strip())

        # Performed to break apart the names in the sheet to individuals,
        # to be able to track to the individual level
        def split_to_json(x_):
            jsonb = {}
            if re.search('/', x_):
                names = x_.split(sep='/')
                jsonb['names'] = [name.strip().title() for name in names]
            elif re.search(',', x_):
                names = x_.split(sep='/')
                jsonb['names'] = [name.strip().title() for name in names]
            else:
                jsonb['names'] = x_.strip().title()
            return json.dumps(jsonb)

        shipments['Ship POC_json'] = shipments['Ship POC'].apply(
            lambda x: split_to_json(x))
        shipments[shipments['Ship POC_json'] ==
                  '{"names": ["Andrey Tokarev", "Diane Bolton", "Bonnie Slike", "Kier Om"]}']

        # Continue with other database operations and commit the changes
        myConnection.commit()