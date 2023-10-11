# -*- coding: utf-8 -*-
"""
PALADIN Tree Validator

@author: Antonio Jesús Díaz-Honrubia
@author: Philipp D. Rohde
"""

import argparse
import json
from enum import Enum
from functools import partial

import mysql.connector
from SPARQLWrapper import SPARQLWrapper, JSON


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
    except mysql.connector.Error:
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


def traverse_tree_recursion(tree, population, data_source, conn):
    """Traverse the tree in a recursive fashion."""
    if tree['target']:
        target = query_data(tree['target'], data_source, conn)
        population = population.intersection(target)
    
    validated = population
    unvalidated = set()
    if print_mode:
        print(tree['name'])
        print('Target population: ' + str(len(population)))
    
    if tree['validation']:
        validated = query_data(tree['validation'], data_source, conn)
        validated = validated.intersection(population)
        unvalidated = population - validated
        if print_mode:
            print('Validated: ' + str(len(validated)))
            print('Validation violated: ' + str(len(unvalidated)))

    if print_mode:
        print('---------------------------')
    
    if tree['left'] and len(unvalidated) > 0:
        traverse_tree_recursion(tree['left'], validated, data_source, conn)
    if tree['right'] and len(validated) > 0:
        traverse_tree_recursion(tree['right'], unvalidated, data_source, conn)
    

def traverse_tree_depth(tree, population, data_source, conn):
    """Traverse the tree with Depth-first search (DFS)."""
    stack = []
    stack.append((tree, population,))

    while stack:
        tree, population = stack.pop()

        if tree['target']:
            target = query_data(tree['target'], data_source, conn)
            population = population.intersection(target)

        validated = population
        unvalidated = set()
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

        if tree['right'] and len(unvalidated) > 0:
            stack.append((tree['right'], unvalidated,))
        if tree['left'] and len(validated) > 0:
            stack.append((tree['left'], validated,))
        

def traverse_tree_width(tree, population, data_source, conn):
    """Traverse the tree with Breadth-first search (BFS)."""
    stack_level = []
    stack_next_level = []
    stack_next_level.append((tree, population,))

    while stack_next_level:
        if stack_next_level:
            stack_level = stack_next_level
            stack_next_level = []

        while stack_level:
            tree, population = stack_level.pop()

            if tree['target']:
                target = query_data(tree['target'], data_source, conn)
                population = population.intersection(target)

            validated = population
            unvalidated = set()
            if print_mode:
                print(tree['name'])
                print('Target population: ' + str(len(population)))
            
            if tree['validation']:
                validated = query_data(tree['validation'], data_source, conn)
                validated = validated.intersection(population)
                unvalidated = population - validated
                if print_mode:
                    print('Validated: ' + str(len(validated)))
                    print('Validation violated: ' + str(len(unvalidated)))

            if print_mode:
                print('---------------------------')
            
            if tree['right'] and len(unvalidated) > 0:
                stack_next_level.append((tree['right'], unvalidated,))
            if tree['left'] and len(validated) > 0:
                stack_next_level.append((tree['left'], validated,))


class TraversalStrategy(Enum):
    """An enum acting as a wrapper to map the strings DFS, BFS, and REC to the appropriate traversal function."""
    BFS = partial(traverse_tree_width)
    DFS = partial(traverse_tree_depth)
    REC = partial(traverse_tree_recursion)

    def traverse(self, tree, population, data_source, conn):
        """Calls the traversal function mapped to this instance of the enum."""
        return self.value(tree, population, data_source, conn)


def paladin(tree_file, traversal_strategy):
    """Performs the validation of the given PALADIN tree following the specified traversal strategy."""
    traversal_strategy = TraversalStrategy[traversal_strategy]
    with open(tree_file, 'r') as tf:
        data = json.load(tf)

    if data['data_source'] == 'mysql':
        db_mngr = generate_db_connection(data['url'], data['port'], data['database'], data['user'], data['password'])
        db_mngr["db_crs"].execute(data['population'])
        population = set([x[0] for x in db_mngr["db_crs"].fetchall()])
        traversal_strategy.traverse(data['tree'], population, data['data_source'], db_mngr)
        close_connection(db_mngr)
    elif data['data_source'] == 'sparql-endpoint':
        endpoint = sparql_endpoint_connection(data['url'])
        population = query_data_sparql(data['population'], endpoint)
        traversal_strategy.traverse(data['tree'], population, data['data_source'], endpoint)


if __name__ == '__main__':
    """Entry point for the PALADIN script.
    
    PALADIN requires two positional parameters. The first one is the path to the PALADIN tree file.
    The second parameter specifies the traversal strategy; one of BFS, DFS, REC.
    
    """
    parser = argparse.ArgumentParser(description='PALADIN Validation')
    parser.add_argument('tree', metavar='paladin_tree', type=str, default=None,
                        help='Path to the PALADIN tree file to validate')
    parser.add_argument(dest='traversal', type=str, choices=['BFS', 'DFS', 'REC'],
                        help='Tree traversal strategy to follow during validation')
    args = parser.parse_args()

    paladin(args.tree, args.traversal)
