"""
Astronaut ETL Example DAG

Queries the list of astronauts currently in space from the Open Notify API
and prints each astronaut's name and the craft they are flying on.

There are two tasks: one to fetch and store the API results, and another to
print them. Both use Airflow's TaskFlow API, which turns plain Python functions
into Airflow tasks and infers dependencies automatically.

The print task uses dynamic task mapping to spin up one copy per astronaut
returned by the API, so the DAG adjusts itself each run without any manual
configuration.

For a full walkthrough see the Astronomer getting-started guide:
https://www.astronomer.io/docs/learn/get-started-with-airflow
"""

from airflow.sdk.definitions.asset import Asset
from airflow.decorators import dag, task
from pendulum import datetime
import requests


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():

    @task(
        # Emitting this asset lets downstream DAGs schedule off this task's completion.
        outlets=[Asset("current_astronauts")]
    )
    def get_astronauts(**context) -> list[dict]:
        """
        Retrieve the list of people currently in space from the Open Notify API.
        Falls back to a hardcoded list when the API is unreachable.
        Pushes the total headcount to XCom for observability.
        """
        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            number_of_people_in_space = r.json()["number"]
            list_of_people_in_space = r.json()["people"]
        except Exception:
            print("API currently unavailable — falling back to hardcoded data.")
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS",      "name": "Oleg Kononenko"},
                {"craft": "ISS",      "name": "Nikolai Chub"},
                {"craft": "ISS",      "name": "Tracy Caldwell Dyson"},
                {"craft": "ISS",      "name": "Matthew Dominick"},
                {"craft": "ISS",      "name": "Michael Barratt"},
                {"craft": "ISS",      "name": "Jeanette Epps"},
                {"craft": "ISS",      "name": "Alexander Grebenkin"},
                {"craft": "ISS",      "name": "Butch Wilmore"},
                {"craft": "ISS",      "name": "Sunita Williams"},
                {"craft": "Tiangong", "name": "Li Guangsu"},
                {"craft": "Tiangong", "name": "Li Cong"},
                {"craft": "Tiangong", "name": "Ye Guangfu"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """Print a greeting for each astronaut along with their current spacecraft."""
        craft = person_in_space["craft"]
        name = person_in_space["name"]
        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    # Dynamic task mapping: one task instance per astronaut returned by the API
    print_astronaut_craft.partial(greeting="Hello!").expand(
        person_in_space=get_astronauts()
    )


example_astronauts()
