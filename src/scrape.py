import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PyPDF2 import PdfReader, PdfWriter
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
import os
import glob
import logging
import logging_config

logger = logging.getLogger(__name__)

EXCEL_URL = "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"

@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(min=10, max=60),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def download_excel():
    r = requests.get(EXCEL_URL)
    r.raise_for_status()
    with open("medicines.xlsx", "wb") as f:
        f.write(r.content)


def get_medicine_url(drug_name):
    df = pd.read_excel("medicines.xlsx", sheet_name="Medicine", header=5)

    header = df.iloc[2]
    df = df.iloc[3:].copy()
    df.columns = header

    row = df[df["Name of medicine"].str.lower() == drug_name.lower()].iloc[0]
    return row["Medicine URL"]

@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(min=10, max=60),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def find_english_pdf(medicine_url):
    logger.info("Fetching medicine page: %s", medicine_url)
    r = requests.get(medicine_url + "#product-info")
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.find_all("a", href=True):
        if "product-information_en.pdf" in a["href"]:
            logger.info("Found English PDF link: %s", a["href"])
            return urljoin("https://www.ema.europa.eu", a["href"])

    return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(min=10, max=60),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def download_pdf(url, filename):
    r = requests.get(url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)

def save_pdf_in_s3(s3_client, bucket_name, prefix, filename):
    s3_client.upload_file(filename, bucket_name, prefix + filename)
    logger.info(f"Uploaded {filename} to s3://{bucket_name}/{prefix}{filename}")

def shorten_pdf(input_pdf, output_pdf):
    reader = PdfReader(input_pdf)
    writer = PdfWriter()

    # Always keep page 2
    writer.add_page(reader.pages[1])

    start_idx = None
    end_idx = None

    for i, page in enumerate(reader.pages):
        text = (page.extract_text() or "").replace(str(i+1), "", 1).strip()

        if i == 61:
            logger.info(f"Page {i+1} text: {text}...")  # Print the characters of the page text for debugging
        if text == "B. PACKAGE LEAFLET":
            start_idx = i + 1
            continue

        if start_idx is not None and "This leaflet was last revised in" in text:
            end_idx = i
            break

    if start_idx is None or end_idx is None:
        raise ValueError("Could not locate leaflet section")

    for i in range(start_idx, end_idx + 1):
        writer.add_page(reader.pages[i])

    with open(output_pdf, "wb") as f:
        writer.write(f)

def download_drug_pdf(drug_name):
    download_excel()
    medicine_url = get_medicine_url(drug_name)
    pdf_url = find_english_pdf(medicine_url)

    if pdf_url:
        filename = drug_name.replace(" ", "_") + ".pdf"
        filename = filename.lower()
        download_pdf(pdf_url, filename)
        new_filename = "shortened_" + filename
        shorten_pdf(filename, new_filename)
        return new_filename
    else:
        raise Exception("PDF not found for drug: " + drug_name)

def remove_all_pdf():
    """Utility function to remove all PDF files after processing."""
    pdf_files = glob.glob("*.pdf")
    for filename in pdf_files:
        os.remove(filename)
        logger.info(f"Removed file: {filename}")

if __name__ == "__main__":
    shorten_pdf("ozempic.pdf", "shortened_ozempic.pdf")
