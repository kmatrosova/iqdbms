import csv
import json
import pymongo
from pymongo import MongoClient
import datetime
import pprint
from dateutil.parser import parse
import re
import numpy as np
from difflib import SequenceMatcher
import sys
import pandas as pd

client = MongoClient()
client.drop_database('csv2tab')
db = client.csv2tab



# Création d'une table contenant tous les regex

with open('ddre.csv', newline='') as csvfile:

    data = []

    reader = csv.reader(csvfile, delimiter=';')
    for row in reader:
        data.append(row)

nb_col = len(data[0])-1
nb_rows = len(data)
col_names = []

for i in range (0, nb_col):
    col_names.append("col_"+ str(i));



new_posts = []

for r in range (0, nb_rows):

    insert_line = {}

    for i in range (0, nb_col):
            insert_line[col_names[i]] = data[r][i]
    new_posts.append(insert_line)


ddre = db.ddre.insert_many(new_posts)


# Lecture du fichier csv

with open('data.csv', newline='') as csvfile:

    data = []

    reader = csv.reader(csvfile, delimiter=';')
    for row in reader:
        data.append(row)

nb_col = len(data[0])
nb_rows = len(data)
col_names = []

for i in range (1, nb_col+1):
    col_names.append("col_"+ str(i));

# Premiere table: on eclate le fichier csv

new_posts = []

for r in range (0, nb_rows):

    insert_line = {}

    for i in range (0, nb_col):
        if data[r][i] != "":
            insert_line[col_names[i]] = data[r][i]
        else:
            insert_line[col_names[i]] = ""

    new_posts.append(insert_line)


first_table = db.first_table
result = first_table.insert_many(new_posts)

print("First table created...")

# Fonctions relatives à la détection de types syntaxiques et sémantiques

def syn_type(anonstring):

    if anonstring == "":
        return "NULL"
    else:
        try:
            anonstring = float(anonstring)
            return "NUMBER"
        except ValueError:
            for r in db.ddre.find({"col_0": "DATE"}):
                regex = re.compile(r['col_2'])
                if regex.match(anonstring) != None and regex.match(anonstring).group() == anonstring:
                    return "DATE"
            return "STRING"


def syn_sub_type(anonstring, syn_type):

    if anonstring == "":
        return "NULL"
    else:
        if syn_type == "NUMBER":

            try:
                anonstring = int(anonstring)
                return "INT"
            except ValueError:
                return "REAL"

        elif syn_type == "DATE":
            return "DATE"

        else:

            alpha = re.compile('[A-Za-z]+')
            alphanum = re.compile('[A-Za-z0-9]+')

            if alpha.match(anonstring) != None and alpha.match(anonstring).group() == anonstring:
                return "ALPHA"
            elif alpha.match(anonstring) != None and alphanum.match(anonstring).group() == anonstring:
                return "ALPHANUM"
            else:
                return "ALPHASPECIALCHAR"


def sem_types(anonstring):

    if anonstring == "":
        return "NULL"

    else:

        for r in db.ddre.find():
            regex = re.compile(r['col_2'])
            if regex.match(anonstring) != None and regex.match(anonstring).group() == anonstring:
                return r['col_0']

        return "NULL"

def sem_sub_types(anonstring, sem_type):

    if anonstring == "":
        return "NULL"
    else:
        types = []

        for r in db.ddre.find():
            if r['col_0'] == sem_type:
                regex = re.compile(r['col_2'])
                if regex.match(anonstring) != None and regex.match(anonstring).group() == anonstring:
                    return r['col_1']

        return "NULL"



# Création des tables col_1 ... col_n

col_tables = []
posts_col_tables = []

for c in range (0, nb_col):

    col_tables.append(db[col_names[c]])
    new_posts = []

    for r in first_table.find():

        my_syn_type = syn_type(r[col_names[c]])
        my_syn_sub_type = syn_sub_type(r[col_names[c]], my_syn_type)
        my_sem_types = sem_types(r[col_names[c]])
        my_sem_sub_types = sem_sub_types(r[col_names[c]], my_sem_types)

        new_posts.append({'old_value': r[col_names[c]],
                          'length': len(r[col_names[c]]),
                          'nb_words': len(r[col_names[c]].split()),
                          'syn_type': my_syn_type,
                          'syn_sub_type': my_syn_sub_type,
                          'sem_type': my_sem_types,
                          'sem_sub_type': my_sem_sub_types,
                          'anomaly': '',
                          'new_value': r[col_names[c]]})

    col_tables[c].insert_many(new_posts)

print("Column tables created...")

# Création de la métatable

new_posts = []

for c in range (0, nb_col):

    nb_types = [0,0,0,0]
    types = ["STRING", "NUMBER", "DATE", "NULL"]
    nb_sub_types = [0,0,0,0,0,0,0]
    sub_types = ["INT", "REAL", "DATE", "ALPHA", "ALPHANUM", "ALPHASPECIALCHAR", "NULL"]
    sem_types = []
    nb_sem_types = []
    sem_sub_types = []
    nb_sem_sub_types = []

    for r in col_tables[c].find():
        for i, t in enumerate(types):
            if r['syn_type'] == t:
                nb_types[i] += 1
        for i, st in enumerate(sub_types):
            if r['syn_sub_type'] == st:
                nb_sub_types[i] += 1

        if r['sem_type'] not in sem_types:
            sem_types.append(r['sem_type'])
            nb_sem_types.append(0)

        if r['sem_sub_type'] not in sem_sub_types:
            sem_sub_types.append(r['sem_sub_type'])
            nb_sem_sub_types.append(0)

    for r in col_tables[c].find():
        for i, t in enumerate(sem_types):
            if r['sem_type'] == t:
                nb_sem_types[i] += 1
        for i, st in enumerate(sem_sub_types):
            if r['sem_sub_type'] == st:
                nb_sem_sub_types[i] += 1


    new_posts.append({'table_name': col_names[c],
                      'dom_syn_type': types[np.argmax(nb_types)],
                      'dom_syn_sub_type': sub_types[np.argmax(nb_sub_types)],
                      'dom_sem_type': sem_types[np.argmax(nb_sem_types)],
                      'dom_sem_sub_type': sem_sub_types[np.argmax(nb_sem_sub_types)]})


metatable = db.metatable.insert_many(new_posts)

print("Metatable created...")

# Homogénisation des dates

dates = db.metatable.find({"dom_syn_type": "DATE"})
date_tables = []

months = [["JAN", "JANV", "JANUARY", "JANVIER"],
          ["FEB", "FÉV", "FEBRUARY", "FÉVRIER"],
          ["MAR", "MARCH", "MARS"],
          ["APR", "AVR", "APRIL", "AVRIL"],
          ["MAY", "MAI"],
          ["JUN", "JUNE", "JUIN"],
          ["JUL", "JULY", "JUILLET"],
          ["AUG", "AUGUST", "AOÛT"],
          ["SEPT", "SEPTEMBER", "SEPTEMBRE"],
          ["OCT", "OCTOBER", "OCTOBRE"],
          ["NOV", "NOVEMBER", "NOVEMBRE"],
          ["DEC", "DÉC", "DECEMBER", "DÉCEMBRE"]]

for d in dates:
    date_tables.append(d["table_name"])

for t in date_tables:

    for r in col_tables[int(re.findall(r'\d+', t)[0])-1].find():

        if r['old_value'] != "":

            if "/" in r['old_value']:
                split_date = r['old_value'].split('/')
            elif "." in r['old_value']:
                split_date = r['old_value'].split('.')
            elif "-" in r['old_value']:
                split_date = r['old_value'].split('-')


            if r['sem_sub_type'] == "DATE_JOUR_FR":
                split_date[0] = split_date[0].split(' ')[1]

            if r['sem_sub_type'] == "DATE_FR" or r['sem_sub_type'] == "DATE_EN" or r['sem_sub_type'] == "DATE_JOUR_FR":

                for i, m in enumerate(months):
                    if split_date[1] in months[i]:
                        split_date[1] = str(i+1)

            col_tables[int(re.findall(r'\d+', t)[0])-1].update_one({'_id': r['_id']},{'$set': {'new_value': split_date[0]+"-"+split_date[1]+"-"+split_date[2],
                                                                                                     'sem_sub_type': "DATE"}}, upsert=False)

for d in date_tables:
    db.metatable.update_one({"table_name": d},{'$set': {'dom_sem_sub_type': "DATE"}}, upsert=False)


# Homogénisation des poids

weight = db.metatable.find({"dom_sem_type": "WEIGHT"})
weight_tables = []

for w in weight:
    weight_tables.append(w["table_name"])

for t in weight_tables:
    dom_sem_sub_type = db.metatable.find_one({"table_name": t})['dom_sem_sub_type']
    for r in col_tables[int(re.findall(r'\d+', t)[0])-1].find():

        if r['old_value'] != "":

            split_weigth = r['old_value'].split(' ')

            if r['sem_sub_type'] == "WEIGHT_FR_G" and dom_sem_sub_type == "WEIGHT_FR_KG":
                new_value = str(round(float(split_weigth[0].replace(',', '.'))/1000, 4)).replace('.', ',') + " KG"
                col_tables[int(re.findall(r'\d+', t)[0])-1].update_one({'_id': r['_id']},{'$set': {'new_value': new_value, 'sem_sub_type': "WEIGHT_FR_KG"}}, upsert=False)

            elif r['sem_sub_type'] == "WEIGHT_FR_KG" and dom_sem_sub_type == "WEIGHT_FR_G":
                new_value = str(round(float(split_weigth[0].replace(',', '.'))*1000 , 4)).replace('.', ',') + " G"
                col_tables[int(re.findall(r'\d+', t)[0])-1].update_one({'_id': r['_id']},{'$set': {'new_value': new_value, 'sem_sub_type': "WEIGHT_FR_G"}}, upsert=False)


# Homogénisation des températures

temp = db.metatable.find({"dom_sem_type": "TEMPERATURE"})
temp_tables = []

for t in temp:
    temp_tables.append(t["table_name"])

for t in temp_tables:
    dom_sem_sub_type = db.metatable.find_one({"table_name": t})['dom_sem_sub_type']
    for r in col_tables[int(re.findall(r'\d+', t)[0])-1].find():

        if r['old_value'] != "":

            split_temp = r['old_value'].split(' ')

            if r['sem_sub_type'] == "TEMPERATURE_CELSIUS" and dom_sem_sub_type == "TEMPERATURE_FAHRENHEIT":
                new_value = str(round((float(split_temp[0].replace(',', '.')) * 9/5) + 32 , 4)).replace('.', ',')+ " °F"
                col_tables[int(re.findall(r'\d+', t)[0])-1].update_one({'_id': r['_id']},{'$set': {'new_value': new_value, 'sem_sub_type': "TEMPERATURE_FAHRENHEIT"}}, upsert=False)

            elif r['sem_sub_type'] == "TEMPERATURE_FAHRENHEIT" and dom_sem_sub_type == "TEMPERATURE_CELSIUS":
                new_value = str(round((float(split_temp[0].replace(',', '.')) - 32) * 5/9, 4)).replace('.', ',') + " °C"
                col_tables[int(re.findall(r'\d+', t)[0])-1].update_one({'_id': r['_id']},{'$set': {'new_value': new_value, 'sem_sub_type': "TEMPERATURE_CELSIUS"}}, upsert=False)


# Homogénisation des longueurs

size = db.metatable.find({"dom_sem_type": "SIZEDISTANCE_LENGTH"})
size_tables = []

for s in size:
    size_tables.append(s["table_name"])

for t in size_tables:
    dom_sem_sub_type = db.metatable.find_one({"table_name": t})['dom_sem_sub_type']
    for r in col_tables[int(re.findall(r'\d+', t)[0])-1].find():

        if r['old_value'] != "":

            split_size = r['old_value'].split(' ')

            if r['sem_sub_type'] == "SIZEDISTANCE_LENGTH_FR_M" and dom_sem_sub_type == "SIZEDISTANCE_LENGTH_FR_CM":
                new_value = str(round(float(split_size[0].replace(',', '.'))*1000, 4)).replace('.', ',') + " CM"
                col_tables[int(re.findall(r'\d+', t)[0])-1].update_one({'_id': r['_id']},{'$set': {'new_value': new_value, 'sem_sub_type': "SIZEDISTANCE_LENGTH_FR_CM"}}, upsert=False)

            elif r['sem_sub_type'] == "SIZEDISTANCE_LENGTH_FR_CM" and dom_sem_sub_type == "SIZEDISTANCE_LENGTH_FR_M":
                new_value = str(round(float(split_size[0].replace(',', '.'))/1000, 4)).replace('.', ',') + " M"
                col_tables[int(re.findall(r'\d+', t)[0])-1].update_one({'_id': r['_id']},{'$set': {'new_value': new_value, 'sem_sub_type': "SIZEDISTANCE_LENGTH_FR_M"}}, upsert=False)



# Détection d'anomalies et homogénisation

for c in range (0, nb_col):

    meta = db.metatable.find_one({"table_name": col_names[c]})
    my_types = [meta['dom_syn_type'], meta['dom_syn_sub_type'], meta['dom_sem_type'], meta['dom_sem_sub_type']]

    for r in col_tables[c].find():

        if r['old_value'] != "":

            if r['sem_sub_type'] != my_types[3]:
                col_tables[c].update_one({'_id': r['_id']},{'$set': {'anomaly': '<SEM_SUB Anomaly>',
                                                                     'new_value': r['old_value']+'<SEM_SUB Anomaly>'}}, upsert=False)
                if r['sem_type'] != my_types[2]:
                    col_tables[c].update_one({'_id': r['_id']},{'$set': {'anomaly': '<SEM Anomaly>',
                                                                         'new_value': r['old_value']+'<SEM Anomaly>'}}, upsert=False)

                    if r['syn_sub_type'] != my_types[1]:
                        col_tables[c].update_one({'_id': r['_id']},{'$set': {'anomaly': '<SYN_SUB Anomaly>',
                                                                             'new_value': r['old_value']+'<SYN_SUB Anomaly>'}}, upsert=False)

                        if r['syn_type'] != my_types[0]:
                            col_tables[c].update_one({'_id': r['_id']},{'$set': {'anomaly': '<SYN Anomaly>',
                                                                                 'new_value': r['old_value']+'<SYN Anomaly>'}}, upsert=False)

print("Anomalies detection and homogenisation completed...")

# Fusion des données nettoyées

new_posts = []

for c in range (0, nb_col):

    meta = db.metatable.find_one({'table_name':col_names[c]})

    if meta['dom_sem_sub_type'] != "NULL":
        col_type = meta['dom_sem_sub_type']
    elif meta['dom_sem_type'] != "NULL":
        col_type = meta['dom_sem_type']
    elif meta['dom_syn_sub_type'] != "NULL":
        col_type = meta['dom_syn_sub_type']
    elif meta['dom_syn_type'] != "NULL":
        col_type = meta['dom_syn_type']

    col_name = meta['table_name'] + "_" + col_type

    if c == 0:
        for r in col_tables[c].find():
            new_posts.append({col_name : r['new_value']})

    else:
        for i, r in enumerate(col_tables[c].find()):
            new_posts[i].update({col_name : r['new_value']})

clean_table = db.clean_table.insert_many(new_posts)

print("Second table created...")


# Création d'une clé unique, détection et suppression des doubles

new_posts = []

for r in db.clean_table.find():
    new_posts.append(r)


deduplicated = db.deduplicated.insert_many(new_posts)

for r in db.deduplicated.find():
    key = list(r.values())
    key.remove(key[0])
    key = ''.join(key)
    db.deduplicated.update_one({'_id': r['_id']},{'$set': {'key': key}}, upsert=False)


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


deleted_ids = []
for r1 in db.deduplicated.find():
    if r1['_id'] not in deleted_ids:

        for r2 in db.deduplicated.find():

            if r1['_id'] != r2['_id'] and similar(r1['key'],r2['key']) > 0.85 and r2['_id'] not in deleted_ids:

                    #print(r1['key'], "\n", "\n", r2['key'], "\n", "\n", "\n")
                    deleted_ids.append(r2['_id'])
                    db.deduplicated.delete_one({'_id': r2['_id']})

for r in db.deduplicated.find():
    db.deduplicated.update_one({'_id': r['_id']}, {'$unset': {'key': "1"}})


print("Similar rows deleted...")


# Calcul des dépendances fonctionnelles

new_posts = []

#dependences = np.empty([nb_col+1, nb_col+1])

r = db.deduplicated.find_one()
columns = list(r.keys())
columns.remove(columns[0])
columns.remove(columns[19])

for i, c1 in enumerate(columns):
    for j, c2 in enumerate(columns):



        pairs = []
        n = 0

        for r in db.deduplicated.find():
            if r[c1] != "" or r[c2] != "":

                n += 1
                pairs.append([r[c1], r[c2]])

        df = pd.DataFrame(pairs, columns =['col1', 'col2'])
        n_col1_df = df.groupby("col1")["col2"].count()
        pairs = n_col1_df.values.tolist()
        new_posts.append({"col_A": c1, "col_B": c2, "DF": round((pairs.count(1)/(n)) * 100 ,2 )})
        #dependences[i][j] = round((pairs.count(1)/(n)) * 100 ,2 )

#print(dependences)

db.dependencies.insert_many(new_posts)

print("Data processing completed successfully!")
