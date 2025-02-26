# analytical engineering with dbt

Enables SQL to be deployed using SWE best practices

- modularity
- portability
- version control
- CI/CD
- documentation

## setup

- dbt requires its own repo. add to other project as a submodule if needed
- or use a subdirectory

### connect with git clone

- dbt will present an SSH-RSA key
- go to repo -> top menu -> settings -> left menu -> deploy
- add the key to the github deploy keys
- enables dbt to pull/push from/to this repo
- [CI/CD](https://docs.getdbt.com/guides/custom-cicd-pipelines?step=1) with custom git providers
- github can provide built-in CI/CD pipelines

### project structure

- models
    - staging - initial building blocks from source data
        - type casting
        - null processing
    - interemdiate - stacking layers of logic with specific purpose, preparation for joins
        - regional?

    - marts - things people care about
        - aggregations
- dbt_project.yml
- tests
- seeds
- analyses

## Building dbt models

### materializations

- ephemeral - akin to CTE, available only for a single dbt run
- view - same as sql view; all models are views by default
- regular
- incremental - new data only

### sources

FROM clause of a dbt model

- data that's been loaded to the DWH to be used as sources
- config in /models yml
- *freshness*
- the jinja references `{{ }}` abstracts the environment away so that dbt can compile the SQL for any environment, e.g. different project with different database and schema can still be referenced with `staging`
- however the tables name must match the actual table names

### seeds

CSV inside `/seeds`

- version controlled
- data that doesn't change frequently

### macros

- based on jinja templating
- stored in `/macros`
- enables env vars interpolation
- control structures e.g. IF/ELSE, FOR-loops
- similar to user-defined functions that dynamically generates SQL
- can be grouped into a *package* to be reused across different project
    - *dbt package hub*
    - can be *imported* in `packages.yml` at same level as `dbt_project.yml`
    - popular:
        - dbt_utils - many generic tests and common utilities
        - dbt_expectations

### vars

arguments to be passed to `dbt build` as a dict, e.g.

```sh
dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
```
usage inside a model:

```sql
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```

### env var

- deploy -> environments -> variables. each addition resets the cloud IDE
- must prefix with `DBT_`
- secrets: `DBT_ENV_SECRET_`; masked from logs
- useable in profiles, packages, and all schema/model related `.yml`
- NOT `dbt_project.yml`
- invoke with `{{ env_var('var_name', default) }}`
