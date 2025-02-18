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
    - interemdiate - stacking layers of logic with specific purpose, preparation for joins
    - marts - things people care about
- dbt_project.yml
- tests
- seeds
- analyses
