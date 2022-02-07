from curses.ascii import ESC
from dagster import get_dagster_logger, job, op, resource, ScheduleDefinition, DagsterType, Field
from lib.amundsen_extractor import ESConnection, PostgresConnection, Neo4JConnection
from lib import amundsen_extractor
@resource(config_schema={
  "user": str,
  "password": str,
  "db": str,
  "host": Field(str, is_required=False, default_value="localhost"),
  "port": Field(int, is_required=False, default_value=5432)
})
def postgres_resource(init_context):
  user = init_context.resource_config["user"]
  password = init_context.resource_config["password"]
  db = init_context.resource_config["db"]
  host = init_context.resource_config["host"]
  port = init_context.resource_config["port"]

  return PostgresConnection(user, password, db, host, port)

@resource(config_schema={
  "host": str,
})
def es_resource(init_context):
  host = init_context.resource_config["host"]

  return ESConnection(host)

@resource(config_schema={
  "user": str,
  "password": str,
  "endpoint": str,
})
def neo4j_resource(init_context):
  user = init_context.resource_config["user"]
  password = init_context.resource_config["password"]
  endpoint = init_context.resource_config["endpoint"]

  return Neo4JConnection(endpoint, user, password)

@op(required_resource_keys={"postgres", "neo4j", "es"})
def amundsen_postgres(context):
  postgres:PostgresConnection = context.resources.postgres
  neo4j:Neo4JConnection = context.resources.neo4j
  es:ESConnection = context.resources.es

  amundsen_extractor.run_job(postgres, neo4j, es)

@op
def amundsen_glue():
  pass

@job(resource_defs={
  "postgres": postgres_resource,
  "neo4j": neo4j_resource,
  "es": es_resource,
})
def amundsen_job():
    amundsen_postgres()
    amundsen_glue()

basic_schedule = ScheduleDefinition(job=amundsen_job, cron_schedule="0 0 * * *")