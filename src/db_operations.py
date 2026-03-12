import psycopg2
import json
import os

# Replace with your postgres endpoint
USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("DB_PASSWORD")
DB_URL = os.getenv("DB_URL")
DB_NAME = os.getenv("DB_NAME")
POSTGRES_ENDPOINT = f"postgres://{USERNAME}:{PASSWORD}@{DB_URL}/{DB_NAME}"

def create_table(endpoint):
    try:
        # Connect to Postgres using the endpoint
        conn = psycopg2.connect(endpoint)
        print("Connected to Postgres successfully.")
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS DRUGS_LABELLING_INFORMATION (
            drug_name TEXT,
            active_ingredients TEXT[],
            dosage_and_administration TEXT,
            route_of_administration TEXT[],
            dosage_forms_and_strengths TEXT,
            manufacturer TEXT[],
            indications TEXT,
            warnings_and_precautions TEXT,
            storage_and_handling TEXT,
            adverse_reactions TEXT,
            country TEXT,
            PRIMARY KEY (drug_name, country)
        );
        """

        cursor.execute(create_table_query)
        conn.commit()

        print("Table DRUGS_LABELLING_INFORMATION created successfully.")

    except Exception as e:
        print("Error creating table:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def upsert_records_from_json(endpoint, json_file):
    conn = psycopg2.connect(endpoint)
    cursor = conn.cursor()

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        records = list(data.values())

    query = """
    INSERT INTO DRUGS_LABELLING_INFORMATION (
        drug_name,
        active_ingredients,
        dosage_and_administration,
        route_of_administration,
        dosage_forms_and_strengths,
        manufacturer,
        indications,
        warnings_and_precautions,
        storage_and_handling,
        adverse_reactions,
        country
    )
    VALUES (
        UPPER(%(drug_name)s),
        %(active_ingredients)s,
        %(dosage_and_administration)s,
        %(route_of_administration)s,
        %(dosage_forms_and_strengths)s,
        %(manufacturer)s,
        %(indications)s,
        %(warnings_and_precautions)s,
        %(storage_and_handling)s,
        %(adverse_reactions)s,
        %(country)s
    )
    ON CONFLICT (drug_name, country)
    DO UPDATE SET
        drug_name = UPPER(EXCLUDED.drug_name),
        active_ingredients = EXCLUDED.active_ingredients,
        dosage_and_administration = EXCLUDED.dosage_and_administration,
        route_of_administration = EXCLUDED.route_of_administration,
        dosage_forms_and_strengths = EXCLUDED.dosage_forms_and_strengths,
        manufacturer = EXCLUDED.manufacturer,
        indications = EXCLUDED.indications,
        warnings_and_precautions = EXCLUDED.warnings_and_precautions,
        storage_and_handling = EXCLUDED.storage_and_handling,
        adverse_reactions = EXCLUDED.adverse_reactions;
    """

    for record in records:
        cursor.execute(query, record)

    conn.commit()
    cursor.close()
    conn.close()

    print("Records inserted/updated successfully.")