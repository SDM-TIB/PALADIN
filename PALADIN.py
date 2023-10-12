# -*- coding: utf-8 -*-
"""
PALADIN Tree Validator

@author: Antonio Jesús Díaz-Honrubia
@author: Philipp D. Rohde
"""

import abc
import argparse
import json
from enum import Enum
from functools import partial

import mysql.connector
from SPARQLWrapper import SPARQLWrapper, JSON

print_mode = True


class DataSource(object):
    """Abstract class representing a data source."""

    def __init__(self):
        self.connection = self._get_connection()

    @staticmethod
    def get_data_source(data):
        """Get an appropriate ``DataSource`` instance based on the information in the PALADIN tree."""
        if data['data_source'] == 'mysql':
            return MySQL(data)
        elif data['data_source'] == 'sparql-endpoint':
            return SPARQLEndpoint(data)
        else:
            raise NotImplementedError('Data source type ' + str(data['data_source']) + ' not supported.')

    @abc.abstractmethod
    def _get_connection(self):
        """Private method to establish the connection to the data source."""
        return

    @abc.abstractmethod
    def query(self, query: str) -> set:
        """Executes a query over the data source and returns the result as a set."""
        return set()


class MySQL(DataSource):
    """Implementation of a MySQL data source. Handles the connection as well as queries to the data."""

    def __init__(self, data):
        self.url = data['url']
        self.port = data['port']
        self.database = data['database']
        self.user = data['user']
        self.password = data['password']
        super().__init__()

    def __del__(self):
        """Close the connection to the database when the object is deleted."""
        self.close_connection()

    def _get_connection(self):
        db_conn = mysql.connector.connect(
            host=self.url,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        db_cursor = db_conn.cursor()
        return {"db_mngr": db_conn, "db_crs": db_cursor}

    def close_connection(self):
        """Closes the MySQL connection."""
        self.connection['db_crs'].close()

    def query(self, query: str) -> set:
        try:
            db_crs = self.connection['db_crs']
            db_crs.execute(query)
            return set([x[0] for x in db_crs.fetchall()])
        except mysql.connector.Error:
            print('Error in the SQL query')
        return set()


class SPARQLEndpoint(DataSource):
    """Implementation of a SPARQL endpoint. Handles the connection as well as queries to the data."""

    def __init__(self, data):
        self.url = data['url']
        super().__init__()

    def _get_connection(self):
        sparql_endpoint = SPARQLWrapper(self.url)
        sparql_endpoint.setReturnFormat(JSON)
        return sparql_endpoint

    def query(self, query: str) -> set:
        try:
            self.connection.setQuery(query)
            result = self.connection.queryAndConvert()
            proj_var = result['head']['vars'][0]
            return set([res[proj_var]['value'] for res in result['results']['bindings']])
        except:
            print('Error in the SPARQL query')
        return set()


def traverse_tree_recursion(tree, population, data_source: DataSource):
    """Traverse the tree in a recursive fashion."""
    if tree['target']:
        target = data_source.query(tree['target'])
        population = population.intersection(target)
    
    validated = population
    unvalidated = set()
    if print_mode:
        print(tree['name'])
        print('Target population: ' + str(len(population)))
    
    if tree['validation']:
        validated = data_source.query(tree['validation'])
        validated = validated.intersection(population)
        unvalidated = population - validated
        if print_mode:
            print('Validated: ' + str(len(validated)))
            print('Validation violated: ' + str(len(unvalidated)))

    if print_mode:
        print('---------------------------')
    
    if tree['left'] and len(unvalidated) > 0:
        traverse_tree_recursion(tree['left'], validated, data_source)
    if tree['right'] and len(validated) > 0:
        traverse_tree_recursion(tree['right'], unvalidated, data_source)
    

def traverse_tree_depth(tree, population, data_source: DataSource):
    """Traverse the tree with Depth-first search (DFS)."""
    stack = []
    stack.append((tree, population,))

    while stack:
        tree, population = stack.pop()

        if tree['target']:
            target = data_source.query(tree['target'])
            population = population.intersection(target)

        validated = population
        unvalidated = set()
        if print_mode:
            print(tree['name'])
            print('Target population: ' + str(len(population)))

        if tree['validation']:
            validated = data_source.query(tree['validation'])
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
        

def traverse_tree_width(tree, population, data_source: DataSource):
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
                target = data_source.query(tree['target'])
                population = population.intersection(target)

            validated = population
            unvalidated = set()
            if print_mode:
                print(tree['name'])
                print('Target population: ' + str(len(population)))
            
            if tree['validation']:
                validated = data_source.query(tree['validation'])
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

    def traverse(self, tree, population, data_source):
        """Calls the traversal function mapped to this instance of the enum."""
        return self.value(tree, population, data_source)


def paladin(tree_file, traversal_strategy):
    """Performs the validation of the given PALADIN tree following the specified traversal strategy."""
    traversal_strategy = TraversalStrategy[traversal_strategy]
    with open(tree_file, 'r') as tf:
        data = json.load(tf)
    data_source = DataSource.get_data_source(data)
    population = data_source.query(data['population'])
    traversal_strategy.traverse(data['tree'], population, data_source)
    del data_source


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
