# Time-series agregation code duplication prevention

* Status: proposed
* Deciders: Equip de Dades
* Date: 2022-10-06

Technical Story:

## Context and Problem Statement

Un dels motius de dbt és reduir code duplication per les agregacions (diaria, setmanal, mensual, trimestral, anual...), com ho podem fer a dbt?

As-is necessitem un model per cada agregació per cada taula final, que no ens evita la duplicació de codi tot perquè cada agregació necessita el seu model

## Decision Drivers <!-- optional -->

* [driver 1, e.g., a force, facing concern, …]
* [driver 2, e.g., a force, facing concern, …]
* … <!-- numbers of drivers can vary -->

## Considered Options

1. soft-linked macro on every data mart
2. just repeat models with macros for aggregations
3. use post-hooks to move tables [1](https://stackoverflow.com/questions/73460282/multiple-models-in-database-from-a-single-dbt-model)
4. [other stuff](https://youtu.be/MdSMSbQxnO0?t=1977)
5. [dbt metrics support!](https://docs.getdbt.com/docs/building-a-dbt-project/metrics)

## Decision Outcome

5. dbt metrics support (en discussió)


### Positive Consequences <!-- optional -->

* [e.g., improvement of quality attribute satisfaction, follow-up decisions required, …]
* …

### Negative Consequences <!-- optional -->

* [e.g., compromising quality attribute, follow-up decisions required, …]
* …

## Pros and Cons of the Options <!-- optional -->

### [option 1]

[example | description | pointer to more information | …] <!-- optional -->

* Good, because [argument a]
* Good, because [argument b]
* Bad, because [argument c]
* … <!-- numbers of pros and cons can vary -->

### [option 2]

[example | description | pointer to more information | …] <!-- optional -->

* Good, because [argument a]
* Good, because [argument b]
* Bad, because [argument c]
* … <!-- numbers of pros and cons can vary -->

### [option 3]

[example | description | pointer to more information | …] <!-- optional -->

* Good, because [argument a]
* Good, because [argument b]
* Bad, because [argument c]
* … <!-- numbers of pros and cons can vary -->

## Links <!-- optional -->

* [Link type] [Link to ADR] <!-- example: Refined by [ADR-0005](0005-example.md) -->
* … <!-- numbers of links can vary -->
