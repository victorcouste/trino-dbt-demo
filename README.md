# trino-dbt-demo

Welcome to this Trino dbt project !

<img src="https://github.com/victorcouste/trino-dbt-demo/raw/main/logos.png" width="50%" height="50%">

The idea of this project is to demonstrate the power of two of the most successful Open Source data projects, dbt and Trino:

- [dbt](https://www.getdbt.com) (data build tool) enables analytics engineers to transform data in their warehouses by simply writing SQL select statements. dbt handles turning these SQL select statements into tables and views ([more details](https://docs.getdbt.com/docs/introduction)).

- [Trino](https://trino.io), formerly [PrestoSQL](https://trino.io/blog/2020/12/27/announcing-trino.html), is a fast distributed SQL query engine for big data analytics that helps you explore your data universe and mix multiple data sources ([more details](https://trino.io/docs/current/overview/use-cases.html)).

So why not use dbt and Trino as a powerful ETL tool ?

Trino connects to multiple and diverse data sources ([available connectors](https://trino.io/docs/current/connector.html)) via 1 connection and process SQL queries at scale and fast, while dbt handles these SQL transformation queries to create tables or views.


With this demonstration and simple project example, you will be able to:
- Start a Trino server.
- From Trino, connect to a Google BigQuery dataset and an on-premises PostgreSQL database.
- Via a dbt project, join a BigQuery table and a PostgreSQL table, and write the result into a on-premises PostgreSQL table.

---

## Requirements

Installations:

- **Trino** - [installation instruction](https://trino.io/docs/current/installation/deployment.html), you can go for a single machine for testing and demo.

- **dbt** - [installation instruction](https://docs.getdbt.com/dbt-cli/installation)

- **dbt-presto** - [installation instruction](https://docs.getdbt.com/reference/warehouse-profiles/presto-profile#installation-and-distribution), this is the Trino/Presto dbt Python plugin.

---

## Settings

### Trino

As explained [here](https://trino.io/docs/current/installation/deployment.html#configuring-trino), before your start Trino server you need to:
- Set your config and properties files in the **/etc** folder.
- Define your [catalogs](https://trino.io/docs/current/installation/deployment.html#catalog-properties) with data source connections.

For this demo project, you case use conf, properties and catalog files found in the [trino_etc](/trino_etc) folder. Just copy the contents of this **/trino_etc** folder in a **/etc** folder under your Trino server installation.

#### Configuration: ####

Because of the [PrestoSQL to Trino renaming](https://trino.io/blog/2020/12/27/announcing-trino.html), you need to force protocol header to be named Presto for the dbt-presto plugin to work well with last Trino versions. For that, the additional `protocol.v1.alternate-header-name=Presto` property need to be added in Trino conf file [config.properties](trino_etc/config.properties) ([documentation](https://trino.io/docs/current/admin/properties-general.html?highlight=alternate%20header#protocol-v1-alternate-header-name)).


```
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=5GB
query.max-memory-per-node=1GB
query.max-total-memory-per-node=2GB
discovery-server.enabled=true
discovery.uri=http://localhost:8080
protocol.v1.alternate-header-name=Presto
```

#### Catalogs: ####

You need to define data sources you want to connect via Trino.

For this demonstration we will connect to a Google BigQuery dataset and to an on-premises PostgreSQL database.

**PostgreSQL**

The [postgres.properties](https://github.com/victorcouste/trino-dbt-demo/blob/main/trino_etc/catalog/postgresql.properties) file have to be copied in your etc/catalog Trino folder. You need to set host name, database and credentials of your PostgreSQL database ([Trino doc](https://trino.io/docs/current/connector/postgresql.html)) like:
```
connector.name=postgresql
connection-url=jdbc:postgresql://example.net:5432/database
connection-user=root
connection-password=secret
allow-drop-table=true
```
Note that you need to add the `allow-drop-table=true` so dbt can delete table via Trino.

**BigQuery**

For BigQuery we will connect to the dbt public project dbt-tutorial. The [bigquery.properties](https://github.com/victorcouste/trino-dbt-demo/blob/main/trino_etc/catalog/bigquery.properties) file have to be copied in your etc/catalog Trino folder and you need to set **bigquery.credentials-file** or **bigquery.credentials-key**
([Trino doc](https://trino.io/docs/current/connector/bigquery.html)) with your Google Cloud project key. 

To get this project JSON key file, you can read the [Google documentation](https://cloud.google.com/docs/authentication/getting-started
) or an explanation in [dbt documentation](https://docs.getdbt.com/tutorial/setting-up#create-a-bigquery-project) on BigQuery project and json key file creation.

```
connector.name=bigquery
bigquery.project-id=dbt-tutorial
bigquery.credentials-file=/your_folder/google-serviceaccount.json
```

### dbt

#### Profile:

You need to copy the [trino_profile.yml](https://github.com/victorcouste/trino-dbt-demo/blob/main/trino_profile.yml) file as profiles.yml in your home .dbt folder **~/.dbt/profiles.yml**.

Documentation on [dbt profile](https://docs.getdbt.com/dbt-cli/configure-your-profile) file and on [Presto/Trino profile](https://docs.getdbt.com/reference/warehouse-profiles/presto-profile).

The default catalog is PostgreSQL as we will write in PostgreSQL.

```
trino:
  target: dev
  outputs:
    dev:
      type: presto
      method: none  # optional, one of {none | ldap | kerberos}
      user: admin
      password:  # required if method is ldap or kerberos
      catalog: postgresql
      host: localhost
      port: 8080
      schema: public
      threads: 1
```

#### Project:

In the [dbt project file](https://docs.getdbt.com/reference/dbt_project.yml), [dbt_project.yml](https://github.com/victorcouste/trino-dbt-demo/blob/main/dbt_project.yml), we:
- Use the trino profile
- Define variables for BigQuery catalog **bigquery_catalog** and schema (dataset) **bigquery_schema**
- Set default PostgreSQL catalog and schema for output (under models)

```
name: 'trino_project'
version: '1.0.0'
config-version: 2

profile: 'trino'

source-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
data-paths: ["data"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
    - "target"
    - "dbt_modules"

vars:
  bigquery_catalog: bigquery
  bigquery_schema: data_prep

models:
  trino_project:
      materialized: table
      catalog: postgresql
      schema: public
```

We need also to define a [dbt Macro](https://docs.getdbt.com/docs/building-a-dbt-project/jinja-macros#macros) to change the way dbt generate and use a new schema (the change between BigQuery and PostgreSQL) in a model. Macro file [generate_schema_name.sql](https://github.com/victorcouste/trino-dbt-demo/blob/main/macros/generate_schema_name.sql).

```
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
```

#### Model:

In [customers.sql](https://github.com/victorcouste/trino-dbt-demo/blob/main/models/customers.sql) model, you can see that we:
1. Compute the first and last order date and the number of orders per customer from the **jaffle_shop_orders** BigQuery table.
2. Join previous result with the PostgreSQL **jaffle_shop_customers** table.
3. Write the result in a new **customers** PostgreSQL table.


---

## Use the project

[Start the Trino server](https://trino.io/docs/current/installation/deployment.html#running-trino) with `./bin/launcher start`

Trino will listen by default on 8080 port.

For dbt, run the following commands:
- `dbt --version` to check if dbt is well installed with presto-dbt plugin.
- `dbt debug` to check dbt project and connectivity to Trino.
- `dbt seed` to load the [jaffle_shop_customers.csv](/data/jaffle_shop_customers.csv) file in PostgreSQL.
- `dbt compile`to compile (but does not run) the model
- `dbt run` to run the [customers](https://github.com/victorcouste/trino-dbt-demo/blob/main/models/customers.sql) model, do the join with the BigQuery table aggregated and create the **customers** PostgreSQL table.
- `dbt test` to test data quality on 2 columns of the customers table.
- `dbt docs generate` to generate your project documentation.
- `dbt docs serve --port 8001` to starts a webserver on port 8001 to serve your documentation locally.


You can open Trino Web UI started on 8080 port (http://localhost:8080) to check and monitor SQL queries run by dbt.

---

## Extend the project


For input datasets, you can imagine to use the Google BigQuery public dataset [bigquery-public-data](https://cloud.google.com/bigquery/public-data).

You can also change Trino catalog and schema directly in SQL model files with:

```
{{
    config(
        catalog="bigquery",
        schema="data_prep"
     )
}}
```

Finally, you can connect to another existing Trino or [Starburst Enterprise](https://www.starburst.io/platform/starburst-enterprise) (Trino enterprise edition) deployment or cluster for more scalability, security and performance.

Have fun!
