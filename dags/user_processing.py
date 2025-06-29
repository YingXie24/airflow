from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook



@dag
def user_processing(): 
    
    # Task 1: Connect to PostgreSQL and create a table
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
    )

    # Task 2: Check if the API exists and fetch content
    @task.sensor(poke_interval=30, timeout=300)
    def check_API_exists() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(f"Status code: {response.status_code}")

        if response.status_code == 200:
            condition = True
            fake_user = response.json()

        else:
            condition = False
            fake_user = None

        return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    
    fake_user = check_API_exists()

    # Task 3: Parsing the JSON data pulled from the API sensor
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
    } 

    user_info = extract_user(fake_user)

    # Task 4: Save user info into a csv file
    @task
    def process_user(user_info):
        import csv
        from datetime import datetime

        file_path = "/tmp/user_info.csv"

        user_info = {
            "id": "123",
            "firstname": "Ying",
            "lastname": "Xie",
            "email": "yingxie@gmail.com",
        } 

        user_info['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        with open(file_path, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=user_info.keys())

            writer.writeheader()
            writer.writerow(user_info)


    process_user(user_info)

    # Task 5: Load the data into PostgreSQL
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")  
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )

    store_user()




user_processing()