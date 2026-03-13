import json
from pathlib import Path
from landingai_ade import LandingAIADE
from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import ClientError
import time
import pandas as pd
from tqdm import tqdm
import logging
import logging_config

logger = logging.getLogger(__name__)
from scrape import download_drug_pdf, remove_all_pdf, save_pdf_in_s3

load_dotenv()  # Load environment variables from .env file

from db_operations import upsert_records_from_json, POSTGRES_ENDPOINT

if not os.getenv("AWS_PROFILE"):
    session = boto3.Session()
else:
    session = boto3.Session(profile_name=os.getenv("AWS_PROFILE"))
BUCKET_NAME = os.getenv("BUCKET_NAME")
PREFIX = os.getenv("PREFIX")
AUTOMATED_UPLOAD_PREFIX = os.getenv("AUTOMATED_UPLOAD_PREFIX").replace("TIMESTAMP", str(int(time.time())))

# Define your extraction schema as a dictionary
schema_dict = {
  "type": "object",
  "properties": {
    "active_ingredients": {
      "type": "array",
      "description": "A list of the active, medicinal ingredients in the drug product.",
      "items": {
        "type": "string"
      }
    },
    "dosage_and_administration": {
      "type": "string",
      "description": "Information about the drug product’s dosage and administration recommendations, including starting dose, dose range, titration regimens, and any other clinically significant information that affects dosing recommendations.",
      "nullable": True
    },
    "route_of_administration": {
      "type": "array",
      "description": "The route of administration of the drug product.",
      "items": {
        "type": "string",
        "nullable": True
      }
    },
    "dosage_forms_and_strengths": {
      "type": "string",
      "description": "Information about all available dosage forms and strengths for the drug product to which the labeling applies. This field may contain descriptions of product appearance.",
      "nullable": True
    },
    "manufacturer": {
      "type": "array",
      "description": "Name of manufacturer or company that makes this drug product, corresponding to the labeler code segment of the NDC.",
      "items": {
        "type": "string",
        "nullable": True
      }
    },
    "indications": {
      "type": "string",
      "description": "A statement of each of the drug product's indications for use, such as for the treatment, prevention, mitigation, cure, or diagnosis of a disease or condition, or of a manifestation of a recognized disease or condition, or for the relief of symptoms associated with a recognized disease or condition. This field may also describe any relevant limitations of use.",
      "nullable": True
    },
    "warnings_and_precautions": {
      "type": "string",
      "description": "Information about serious adverse reactions and potential safety hazards, including limitations in use imposed by those hazards and steps that should be taken if they occur. Information about any special care to be exercised for safe and effective use of the drug.",
      "nullable": True
    },
    "storage_and_handling": {
      "type": "string",
      "description": "Information about safe storage and handling of the drug product.",
      "nullable": True
    },
    "adverse_reactions": {
      "type": "string",
      "description": "Information about undesirable effects, reasonably associated with use of the drug, that may occur as part of the pharmacological action of the drug or may be unpredictable in its occurrence. Adverse reactions include those that occur with the drug, and if applicable, with drugs in the same pharmacologically active and chemically related class. There is considerable variation in the listing of adverse reactions. They may be categorized by organ system, by severity of reaction, by frequency, by toxicological mechanism, or by a combination of these.",
      "nullable": True
    }
  },
  "required": [
    "active_ingredients",
    "dosage_and_administration",
    "route_of_administration",
    "dosage_forms_and_strengths",
    "manufacturer",
    "indications",
    "warnings_and_precautions",
    "storage_and_handling",
    "adverse_reactions"
  ]
}


def find_and_save_s3_document(bucket: str, prefix: str, brand_name: str) -> str:
    """
    Lists objects in an S3 prefix, checks if brand_name is present in object keys,
    saves the matched document locally, and returns the local path.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix to list objects from
        brand_name: String to search for in object keys

    Returns:
        Local file path as 'docs/{filename}'

    Raises:
        ValueError: If number of matches is not exactly 1
        ClientError: If S3 operations fail
    """
    s3_client = session.client("s3", region_name="ap-south-1")

    # List objects in the prefix
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get("Contents", [])

    # Handle pagination
    while response.get("IsTruncated"):
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=response["NextContinuationToken"],
        )
        objects.extend(response.get("Contents", []))

    # Filter objects containing brand_name
    matched_objects = [obj for obj in objects if brand_name.lower() in obj["Key"].lower()]

    # Validate exactly one match
    if len(matched_objects) == 0:
        raise ValueError(f"No objects found containing brand_name '{brand_name}' in s3://{bucket}/{prefix}")
    elif len(matched_objects) > 1:
        matched_keys = [obj["Key"] for obj in matched_objects]
        raise ValueError(
            f"Expected exactly 1 match for brand_name '{brand_name}', "
            f"but found {len(matched_objects)}: {matched_keys}"
        )

    # Extract matched object key and filename
    matched_key = matched_objects[0]["Key"]
    filename = os.path.basename(matched_key)

    # Ensure docs/ directory exists
    os.makedirs("docs", exist_ok=True)

    # Download the file locally
    local_path = f"docs/{filename}"
    s3_client.download_file(bucket, matched_key, local_path)

    return local_path


def create_mapping_eu(brand_name):

    file_path = find_and_save_s3_document(bucket=BUCKET_NAME, prefix=PREFIX, brand_name=brand_name)
    # Initialize the client
    client = LandingAIADE(apikey=os.getenv("LANDINGAI_API_KEY"))

    # First, parse the document to get markdown
    if not file_path or not os.path.exists(file_path):
        raise ValueError("Failed to download the document.")
    parse_response = client.parse(
        document=Path(file_path),
        model="dpt-2-latest"
    )

    # Convert schema dictionary to JSON string
    schema_json = json.dumps(schema_dict)

    # Extract structured data using the schema
    extract_response = client.extract(
        schema=schema_json,
        markdown=parse_response.markdown,
        model="extract-latest"
    )

    # Access the extracted data
    extracted_data = extract_response.extraction
    extracted_data["drug_name"] = brand_name
    extracted_data["country"] = "EU"

    return extracted_data

def main():
    file_name = f"drug_labels_eu_top_25_{int(time.time())}.json"
    df = pd.read_csv("top_25_drugs.csv", sep="\t")
    s3_client = session.client("s3", region_name=os.getenv("AWS_REGION"))

    top_25_drugs = df["Drug"].to_list()[24:25]
    logger.info(f"Starting pipeline for {len(top_25_drugs)} drugs.")

    for brand_name in tqdm(top_25_drugs):
        try:
            if os.getenv("EU_AUTOMATED_RUN", "false").lower() != "true":
                logger.info("Skipping automated run as EU_AUTOMATED_RUN is set to false.")
                
            else:
                # Download PDF and save shoertened version to S3 bucket
                pdf_filename = download_drug_pdf(brand_name)
                save_pdf_in_s3(s3_client, BUCKET_NAME, AUTOMATED_UPLOAD_PREFIX, pdf_filename)
                remove_all_pdf()
                PREFIX = AUTOMATED_UPLOAD_PREFIX
            current_doc = create_mapping_eu(brand_name)
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
        time.sleep(30)

    ## Save Data of JSON file to S3 bucket
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

main()

