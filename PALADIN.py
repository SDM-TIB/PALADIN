# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 12:23:55 2022

@author: tonid
"""

import mysql.connector
import json
from SPARQLWrapper import SPARQLWrapper, JSON
import time
import pandas as pd

print_mode = True


def generate_db_connection(ip, port, db_name, user, password):
    db_conn = mysql.connector.connect(
         host=ip,
         port=port,
         user=user,
         password=password,
         database=db_name
       )
    db_cursor = db_conn.cursor()
    return {"db_mngr": db_conn, "db_crs": db_cursor}


def sparql_endpoint_connection(url):
    sparql_endpoint = SPARQLWrapper(url)
    sparql_endpoint.setReturnFormat(JSON)
    return sparql_endpoint


def close_connection(db_mngr):
    db_mngr["db_crs"].close()


def query_data_mysql(query, conn):
    try:
        conn["db_crs"].execute(query)
        data = set([x[0] for x in conn["db_crs"].fetchall()])
        return data
    except mysql.connector.Error as err:
        print('Error in the SQL query')
    return set()


def query_data_sparql(query, conn):
    try:
        conn.setQuery(query)
        result = conn.queryAndConvert()
        proj_var = result['head']['vars'][0]
        return set([res[proj_var]['value'] for res in result['results']['bindings']])
    except:
        print('Error in the SPARQL query')
    return set()


def query_data(query, data_source, conn):
    if data_source == 'mysql':
        return query_data_mysql(query, conn)
    elif data_source == 'sparql-endpoint':
        return query_data_sparql(query, conn)


def traverse_tree_recursion(tree, population, data_source, result, conn):
    
    if tree['target']:
        target = validated = query_data(tree['target'], data_source, conn)
        population = population.intersection(target)
    
    validated = population
    #print(tree['name'])
    #print('Target population: ' + str(len(population)))
    
    if tree['validation']:
        validated = query_data(tree['validation'], data_source, conn)
        validated = validated.intersection(population)
        unvalidated = population - validated
        #print('Validated: ' + str(len(validated)))
        #print('Validation violated: ' + str(len(unvalidated)))
    
    #print('---------------------------')
    
    result[tree['name']+'_validated'] = pd.Series(list(validated), dtype=pd.Int64Dtype())
    result[tree['name']+'_violated'] = pd.Series(list(unvalidated), dtype=pd.Int64Dtype())
    
    if tree['left'] and len(unvalidated)>0:
        traverse_tree_recursion(tree['left'], validated, data_source, result, conn)
    if tree['right'] and len(validated)>0:
        traverse_tree_recursion(tree['right'], unvalidated, data_source, result, conn)
    

def traverse_tree_depth(tree, population, data_source, result, conn):
    stack = []
    stack.append((tree,population,))
    
    while stack:
        tree, population = stack.pop()
        
        if tree['target']:
            target = validated = query_data(tree['target'], data_source, conn)
            population = population.intersection(target)
        
        validated = population
        if print_mode:
            print(tree['name'])
            print('Target population: ' + str(len(population)))
        
        if tree['validation']:
            validated = query_data(tree['validation'], data_source, conn)
            validated = validated.intersection(population)
            unvalidated = population - validated
            if print_mode:
                print('Validated: ' + str(validated))
                print('Validation violated: ' + str(unvalidated))
        
        if print_mode:
            print('---------------------------')
        
        result[tree['name']+'_validated'] = pd.Series(list(validated), dtype=pd.Int64Dtype())
        result[tree['name']+'_violated'] = pd.Series(list(unvalidated), dtype=pd.Int64Dtype())
        
        if tree['right'] and len(unvalidated)>0:
            stack.append((tree['right'],unvalidated,))
        if tree['left'] and len(validated)>0:
            stack.append((tree['left'],validated,))
        

def traverse_tree_width(tree, population, data_source, result, conn):
    stack_level = []
    stack_next_level = []
    stack_next_level.append((tree,population,))
    
    while stack_next_level:
        if stack_next_level:
            stack_level = stack_next_level
            stack_next_level = []
            
        while stack_level:
            tree, population = stack_level.pop()
            
            if tree['target']:
                target = validated = query_data(tree['target'], data_source, conn)
                population = population.intersection(target)
            
            validated = population
            #print(tree['name'])
            #print('Target population: ' + str(len(population)))
            
            if tree['validation']:
                validated = query_data(tree['validation'], data_source, conn)
                validated = validated.intersection(population)
                unvalidated = population - validated
                #print('Validated: ' + str(len(validated)))
                #print('Validation violated: ' + str(len(unvalidated)))
            
            #print('---------------------------')
            
            result[tree['name']+'_validated'] = pd.Series(list(validated), dtype=pd.Int64Dtype())
            result[tree['name']+'_violated'] = pd.Series(list(unvalidated), dtype=pd.Int64Dtype())
            
            if tree['right'] and len(unvalidated)>0:
                stack_next_level.append((tree['right'],unvalidated,))
            if tree['left'] and len(validated)>0:
                stack_next_level.append((tree['left'],validated,))
    
            
# SINGLE EXECUTION

f = open('C:\\Users\\tonid\\Desktop\\data_validation\\targeted_therapy.json')
# f = open('/home/rohde/Downloads/PALADIN/testing/targeted_therapy_KG.json')
data = json.load(f)
f.close()

result = pd.DataFrame([])

if data['data_source'] == 'mysql':
    db_mngr = generate_db_connection(data['url'], data['port'], data['database'], data['user'], data['password'])
    db_mngr["db_crs"].execute(data['population'])
    population = set([x[0] for x in db_mngr["db_crs"].fetchall()])
    traverse_tree_depth(data['tree'], population, data['data_source'], result, db_mngr)
    close_connection(db_mngr)
elif data['data_source'] == 'sparql-endpoint':
    endpoint = sparql_endpoint_connection(data['url'])
    population = query_data_sparql(data['population'], endpoint)
    traverse_tree_depth(data['tree'], population, data['data_source'], result, endpoint)

result.to_csv('../result.csv', index=False)
