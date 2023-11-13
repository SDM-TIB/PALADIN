[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

# <img src="images/logo.png" alt="logo of PALADIN" width="200">

PALADIN is a process-based data validator, i.e., it is capable of validating an entity at different stages of the process it undergoes.
PALADIN is _source-agnostic_, which means that data sources of different formats can be validated.

> [!NOTE]
> This repository contains the prototype implementation of PALADIN.
> The prototype is limited to RDF-based knowledge graphs accessible via SPARQL endpoints and relational databases in MySQL.


## PALADIN Architecture

<figure>
  <img src="images/architecture.png" alt="PALADIN Architecture">
  <figcaption><i>Fig. 1:</i> <b>PALADIN Architecture</b></figcaption>
</figure><br><br>

The PALADIN architecture (see Fig. 1) consists of two components, namely the _Schema Traversal Planner_ and the _Schema Traversal Engine_.
The input is a PALADIN schema (the constraints) and the population (entities from a datasource) which is validated.
The population can be sourced from different data formats, e.g., RDF-based knowledge graphs, relational databases, or NoSQL databases.
PALADIN returns for each shape in the PALADIN schema the set of valid and invalid entities.

__Shape Traversal Planner.__ This component generates an evaluation plan for the shapes in the PALADIN schema.
The process is guided by several features of the shapes including statistics related to the number of the shapes, the topology of the schema, the population size, and selectivity of the shapes.
If the PALADIN schema corresponds to a balanced tree, the schema will be evaluated following a _breadth-first_ strategy.
However, when the schema induces an imbalanced tree where the majority of the shapes are part of the left side of the tree, PALADIN follows a _depth-first_ strategy.
The experiments show that the selection of the traversal strategy impacts the continuous behavior of the engine.

__Shape Traversal Engine.__ This component is responsible for the evaluation of the PALADIN schema following the traversal strategy chosen by the _shape traversal planner_.
The engine follows a recursive evaluation model starting from the root shape of the schema.
The evaluation involves two steps:

1. __Target Population Generation.__ The target population is the intersection of the ancestor's population (either valid or invalid entities) and the shape's target query.
2. __Population Validation.__ The target population is split into the sets for valid and invalid entities based on the constraint query.

The output of the _shape traversal engine_ are the sets of valid and invalid entities for each shape in the PALADIN schema.
