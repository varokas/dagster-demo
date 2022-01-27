import csv
from typing import Dict, List

import requests
from dagster import get_dagster_logger, job, op, DagsterType

def is_list_of_dicts(_, value):
    return isinstance(value, list) and all(
        isinstance(element, dict) for element in value
    )

SimpleDataFrame = DagsterType(
    name="SimpleDataFrame",
    type_check_fn=is_list_of_dicts,
    description="A naive representation of a data frame, e.g., as returned by csv.DictReader.",
)

@op
def download_cereals() -> SimpleDataFrame:
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


@op
def find_highest_calorie_cereal(cereals: SimpleDataFrame) -> str:
    sorted_cereals = list(
        sorted(cereals, key=lambda cereal: cereal["calories"])
    )
    return sorted_cereals[-1]["name"]


@op
def find_highest_protein_cereal(cereals: SimpleDataFrame) -> str:
    sorted_cereals = list(
        sorted(cereals, key=lambda cereal: cereal["protein"])
    )
    return sorted_cereals[-1]["name"]


@op
def display_results(most_calories: str, most_protein: str):
    logger = get_dagster_logger()
    logger.info(f"Most caloric cereal: {most_calories}")
    logger.info(f"Most protein-rich cereal: {most_protein}")


@job
def diamond():
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )