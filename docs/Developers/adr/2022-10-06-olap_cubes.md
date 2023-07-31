# OLAP cubes

* Status: accepted
* Deciders: Equip de Dades
* Date: 2022-10-06

Technical Story:

OLAP cubes?

## Context and Problem Statement


## Decision Drivers <!-- optional -->

* [driver 1, e.g., a force, facing concern, …]
* [driver 2, e.g., a force, facing concern, …]
* … <!-- numbers of drivers can vary -->

## Considered Options

1. OLAP i algun vendor o nosé
2. datasets basats en dbt models via macros on-demand

## Decision Outcome

2\. datasets basats en dbt models

A revisar en el futur

### Positive Consequences <!-- optional -->

* [e.g., improvement of quality attribute satisfaction, follow-up decisions required, …]
* …

### Negative Consequences <!-- optional -->

* [e.g., compromising quality attribute, follow-up decisions required, …]
* …

## Pros and Cons of the Options <!-- optional -->

### OLAP cube

[example | description | pointer to more information | …] <!-- optional -->

* Good, because [argument a]
* Good, because [argument b]
* Bad, because [argument c]
* … <!-- numbers of pros and cons can vary -->

### datasets

Molta gent a dbt argumenta en contra de OLAP cubes i a favor de datasets

* modern datawarehouses no tenen limitacions d'espai, la limitació és el dev-time
* Necessita pre-calcular totes les combinacions de dimensions en la granularitat mínima [1](https://youtu.be/MdSMSbQxnO0?t=1588)
* Molt costós de dissenyar (requereix stars de kimball) [2](https://www.youtube.com/watch?v=3OcS2TMXELU)


## Links <!-- optional -->

* [Link type] [Link to ADR] <!-- example: Refined by [ADR-0005](0005-example.md) -->
* … <!-- numbers of links can vary -->
