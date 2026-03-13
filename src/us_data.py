import os
import json
import requests
from tqdm import tqdm
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
from dotenv import load_dotenv
import time
import boto3
import logging
import logging_config

logger = logging.getLogger(__name__)

load_dotenv()

from db_operations import upsert_records_from_json, POSTGRES_ENDPOINT

if not os.getenv("AWS_PROFILE"):
    session = boto3.Session()
else:
    session = boto3.Session(profile_name=os.getenv("AWS_PROFILE"))

@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60), retry=retry_if_exception_type(requests.RequestException), reraise=True)
def get_us_data(brand_name):
    url = "https://api.fda.gov/drug/label.json"
    params = {
        "search": f"openfda.brand_name:{brand_name}",
        "sort": "effective_time:desc",
        "limit": 10
    }
    r = requests.get(url, params=params)
    try:
        r.raise_for_status()  # Check if the request was successful
    except requests.RequestException as e:
        raise e
    try:
        data = r.json()
    except ValueError as e:
        raise ValueError(f"Error parsing JSON response for brand name: {brand_name}") from e
    
    for c in data.get("results", []):
        logger.info(f"Found brand name in results: {c.get('openfda', {}).get('brand_name', [])}")

    if "results" in data and len(data["results"]) > 0:
        for item in data["results"]:
            if "openfda" in item and "brand_name" in item["openfda"]:
                for added_brand_name in item["openfda"]["brand_name"]:
                    if added_brand_name.lower() == brand_name.lower():
                        return item
        raise ValueError(f"No matching brand name found in results: {brand_name}")
    else:
        raise ValueError(f"No data found for brand name: {brand_name}")
    
def create_mapping_us(doc, given_drug_name=None):
    """
    Create a mapping of the drug label information.
    Information required:
    - drug_name
    - active_ingredients
    - dosage_and_administration
    - route_of_administration
    - dosage_forms_and_strengths
    - manufacturer
    - indications
    - warnings_and_precautions
    - storage_and_handling
    - adverse_reactions
    - country
    """
    mapping = {}

    # Add drug_name to mapping
    try:
        if given_drug_name:
            for brand_name in doc["openfda"]["brand_name"]:
                if brand_name.lower() == given_drug_name.lower():
                    mapping["drug_name"] = brand_name
                    break
        else:
            if len(doc["openfda"]["brand_name"]) > 0:
                raise ValueError("Multiple brand names found in the document. Please provide a specific brand name.")
    except KeyError:
        raise KeyError("Brand name not found in the document.")
    except ValueError as ve:
        raise ValueError(str(ve))
    
    # Add active_ingredients to mapping
    ok_flag = 0
    mapping["active_ingredients"] = []
    try:
        mapping["active_ingredients"] += doc["active_ingredient"] if type(doc["active_ingredient"]) == str else " ".join(doc["active_ingredient"])
        ok_flag = 1
    except KeyError:
        logger.info(f"Warning: Active ingredients not found for {mapping['drug_name']}.")
    try:
        mapping["active_ingredients"] += doc["openfda"]["substance_name"]
        ok_flag = 1
    except KeyError:
        logger.info(f"Warning: Substance name not found for {mapping['drug_name']}.")

    if not ok_flag:
        raise KeyError("No active ingredients found for the drug.")
    
    # Add dosage_and_administration to mapping
    try:
        mapping["dosage_and_administration"] = doc["dosage_and_administration"] if type(doc["dosage_and_administration"]) == str else "\n".join(doc["dosage_and_administration"])
    except KeyError:
        raise KeyError("Dosage and administration information not found in the document.")
    
    # Add route_of_administration to mapping
    try:
        mapping["route_of_administration"] = doc["openfda"]["route"]
    except KeyError:
        raise KeyError("Route of administration not found in the document.")
    
    # Add dosage_forms_and_strengths to mapping
    try:
        mapping["dosage_forms_and_strengths"] = doc["dosage_forms_and_strengths"] if type(doc["dosage_forms_and_strengths"]) == str else "\n".join(doc["dosage_forms_and_strengths"])
    except KeyError:
        raise KeyError("Dosage forms and strengths not found in the document.")
    
    # Add manufacturer to mapping
    try:
        mapping["manufacturer"] = doc["openfda"]["manufacturer_name"]
    except KeyError:
        mapping["manufacturer"] = []
        logger.info(f"Warning: Manufacturer name not found for {mapping['drug_name']}.")

    # Add indications to mapping
    try:
        mapping["indications"] = doc["indications_and_usage"] if type(doc["indications_and_usage"]) == str else "\n".join(doc["indications_and_usage"])
    except KeyError:
        raise KeyError("Indications and usage information not found in the document.")

    # Add warnings and precautions to mapping
    ok_flag = 0
    mapping["warnings_and_precautions"] = ""
    try:
        mapping["warnings_and_precautions"] += doc["warnings"] if type(doc["warnings"]) == str else "\n".join(doc["warnings"])
        ok_flag = 1
    except KeyError:
        logger.info(f"Warning: Warnings not found for {mapping['drug_name']}.")
    try:
        mapping["warnings_and_precautions"] += "\n" + (doc["precautions"] if type(doc["precautions"]) == str else "\n".join(doc["precautions"]))
        ok_flag = 1
    except KeyError:
        logger.info(f"Warning: Precautions not found for {mapping['drug_name']}.")
    try:
        mapping["warnings_and_precautions"] += "\n" + (doc["warnings_and_cautions"] if type(doc["warnings_and_cautions"]) == str else "\n".join(doc["warnings_and_cautions"]))
        ok_flag = 1
    except KeyError:
        logger.info(f"Warning: Warnings and cautions not found for {mapping['drug_name']}.")
    if not ok_flag:
        raise KeyError("No warnings and precautions information found for the drug.")
    
    # Add storage_and_handling to mapping
    try:
        mapping["storage_and_handling"] = doc["storage_and_handling"] if type(doc["storage_and_handling"]) == str else "\n".join(doc["storage_and_handling"])
    except KeyError:
        mapping["storage_and_handling"] = ""
        logger.info(f"Warning: Storage and handling information not found for {mapping['drug_name']}.")
    
    # Add adverse_reactions to mapping
    try:
        mapping["adverse_reactions"] = doc["adverse_reactions"] if type(doc["adverse_reactions"]) == str else "\n".join(doc["adverse_reactions"])
    except KeyError:
        raise KeyError("Adverse reactions information not found in the document.")
    
    # Add country to mapping
    mapping["country"] = "US"

    return mapping

def main():
    file_name = f"drug_labels_us_top_25_{int(time.time())}.json"
    df = pd.read_csv("top_25_drugs.csv", sep="\t")

    top_25_drugs = df["Drug"].to_list()
    logger.info(f"Starting pipeline for {len(top_25_drugs)} drugs.")

    for brand_name in tqdm(top_25_drugs):
        try:
            current_doc = create_mapping_us(get_us_data(brand_name), given_drug_name=brand_name)
        except Exception as e:
            logger.info(f"Error processing brand name {brand_name}: {str(e)}")
            continue
        try:
            if os.path.exists(file_name):
                logger.info("File already exists. Appending to JSON file.")
                with open(file_name, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                if f"{current_doc['drug_name'].upper()}_{current_doc['country']}" in existing_data:
                    logger.info(f"{current_doc['drug_name'].upper()}_{current_doc['country']} already exists in the JSON file. Replacing.")
                existing_data[f"{current_doc['drug_name'].upper()}_{current_doc['country']}"] = current_doc
            else:
                existing_data = {f"{current_doc['drug_name'].upper()}_{current_doc['country']}": current_doc}

            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(existing_data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.info(f"Error processing brand name {brand_name}: {str(e)}")
        time.sleep(2)

    ## Save Data of JSON file to S3 bucket
    s3_client = session.client("s3", region_name=os.getenv("AWS_REGION"))
    try:
        logger.info(f"Uploading {file_name} to S3 path {os.getenv('BUCKET_NAME')}/{os.getenv('DATADUMP_PREFIX')}{file_name}...")
        logger.info(f"File size: {os.path.getsize(file_name)} bytes")
        s3_client.upload_file(file_name, os.getenv("BUCKET_NAME"), os.getenv("DATADUMP_PREFIX") + file_name)
        logger.info(f"Successfully uploaded {file_name} to S3 bucket {os.getenv('BUCKET_NAME')}.")
    except Exception as e:
        logger.info(f"Error uploading {file_name} to S3: {str(e)}")
        return

    ## Load Data of JSON file from S3 bucket
    new_file_name = "downloaded_" + file_name
    try:
        s3_client.download_file(os.getenv("BUCKET_NAME"), os.getenv("DATADUMP_PREFIX") + file_name, new_file_name)
        logger.info(f"Successfully downloaded {file_name} from S3 bucket {os.getenv('BUCKET_NAME')}.")
    except Exception as e:
        logger.info(f"Error downloading {file_name} from S3: {str(e)}")
        return
    ## After processing all drugs, upsert the records to the database
    with open(new_file_name, "r", encoding="utf-8") as f:
        existing_data = json.load(f)
    try:
        logger.info(f"Number of records to upsert: {len(existing_data)}")
    except Exception as e:
        logger.info(f"Error counting records to upsert: {str(e)}")
    if os.getenv("UPDATE_PG_DATABASE", "false").lower() != "true":
        logger.info("Skipping database update as UPDATE_PG_DATABASE is set to false.")
    else:
        try:         
            upsert_records_from_json(POSTGRES_ENDPOINT, new_file_name)
        except Exception as e:
            logger.info(f"Error upserting records to the database: {str(e)}")

if __name__ == "__main__":
    main()
    
