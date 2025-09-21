#!/usr/bin/env python3

import httpx
import psycopg2
from prefect import flow, get_run_logger, task

@task
def retrieve_from_api(
    base_url: str,
    path: str,
    secure: bool
) -> dict:
    
    logger = get_run_logger()

    if secure:
        url = f"https://{base_url}{path}"
    else:
        url = f"http://{base_url}{path}"


    # url: str = base_url
    response = httpx.get(url)
    response.raise_for_status()
    inventory_status = response.json()
    logger.info(inventory_status)
    return inventory_status

@task
def clean_stats_data(
    inventory_status: dict
) -> dict:
    return {
        "sold": inventory_status.get("sold", 0) + inventory_status.get("Sold", 0),
        "pending": inventory_status.get("pending", 0) + inventory_status.get("Pending", 0),
        "available": inventory_status.get("available", 0) + inventory_status.get("Available", 0),
        "unavailable": inventory_status.get("unavailable", 0) + inventory_status.get("Unavailable", 0),
    }

@task
def insert_into_db(
    inventory_stats: dict,
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_password: str
):
    with psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    ) as conn:
        
        insert_query = """
            INSERT INTO inventory_history (
                fetch_timestamp,
                sold,
                pending,
                available,
                unavailable
            )
            VALUES (now(), %(sold)s, %(pending)s, %(available)s, %(unavailable)s)
        """
        with conn.cursor() as cur:
            cur.execute(insert_query, inventory_stats)
            conn.commit()
    get_run_logger().info("Done!.")


def clean_stats(inventory_stats: dict):
    return {
        "sold": inventory_stats.get("sold", 0) + inventory_stats.get("Sold", 0),
        "pending": inventory_stats.get("pending", 0) + inventory_stats.get("Pending", 0),
        "available": inventory_stats.get("available", 0) + inventory_stats.get("Available", 0),
        "unavailable": inventory_stats.get("unavailable", 0) + inventory_stats.get("Unavailable", 0),
    }

@flow
def get_petstore_inventory(    
    base_url: str = "petstore.swagger.io",
    path: str = "/v2/store/inventory",
    secure: bool = True,
    db_host: str = "localhost",
    db_port: int = 5432,
    db_name: str = "petstore",
    db_user: str = "devuser",
    db_password: str = "password"
):
    inventory_stats = retrieve_from_api(base_url, path, secure)
    inventory_stats = clean_stats(inventory_stats)
    insert_into_db(
        inventory_stats,
        db_host,
        db_port,
        db_name,
        db_user,
        db_password
    )

def main():
    # print("hello world")
    get_petstore_inventory.serve("petstore-collection-deployment")


if __name__ == "__main__":
    main()