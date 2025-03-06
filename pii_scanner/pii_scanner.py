import os
import pandas as pd
import json
import requests
import logging
import time
from tqdm import tqdm
from dotenv import load_dotenv

# Load API key and configuration from .env file
load_dotenv()

OLLAMA_URL = "http://localhost:11434/api/generate"  # Adjust if needed
OLLAMA_MODEL = "llama3"  # Ensure this model is available
DATA_FOLDER = "Data"
CHUNK_SIZE = 1000  # Increased chunk size

# Configure logging
logging.basicConfig(filename="pii_scanner.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# PII Categories
PII_DEFINITIONS = """
Personally Identifiable Information (PII) includes:
- Full names
- Addresses (street, city, postal code)
- Phone numbers
- Email addresses
- Social security numbers (SSNs) or national ID numbers
- Credit card numbers
- Bank account numbers
- Dates of birth
- Any sensitive data that uniquely identifies an individual
"""

# Function to check if Ollama server is running
def check_ollama_server():
    try:
        response = requests.get("http://localhost:11434", timeout=10)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

# Function to extract non-null data as a list of values
def extract_non_null_data(chunk):
    non_null_data = []
    for _, row in chunk.iterrows():
        for value in row:
            if pd.notna(value) and value not in [None, "", "nan", "NaN", "NAN"]:  # Exclude nulls, NaNs, and empty strings
                non_null_data.append(str(value).strip())  # Convert everything to string and strip whitespace
    return non_null_data

# Function to send data to the LLM
def scan_chunk_with_llm(chunk, retries=5, delay=5):
    for attempt in range(retries):
        try:
            # Extract non-null data as a list of values
            non_null_data = extract_non_null_data(chunk)
            dataset_text = " ".join(non_null_data)  # Convert list to a single string

            prompt = f"""
            Detect any Personally Identifiable Information (PII) as per the PII Categories in the dataset. 
            If PII is found, list it.
            If no PII is found, respond with: 'No PII detected.'
            Do not describe or analyze the dataset.

            PII Categories:
            {PII_DEFINITIONS}

            Dataset:
            {dataset_text}
            """

            payload = {
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False
            }

            headers = {"Content-Type": "application/json"}
            logging.info(f"Sending request to Ollama server: {payload}")
            response = requests.post(OLLAMA_URL, headers=headers, json=payload, timeout=300)  # Increased timeout

            logging.info(f"Response status code: {response.status_code}")
            if response.status_code == 200:
                return json.loads(response.text).get("response", "")
            else:
                logging.error(f"Error from LLM: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))  # Exponential backoff
            else:
                return None

# Function to process files
def process_file(file_path):
    try:
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == ".csv":
            df = pd.read_csv(file_path, encoding="latin1", low_memory=False, skiprows=1)  # Skip header row
        elif file_extension in [".xls", ".xlsx"]:
            df = pd.read_excel(file_path, engine="openpyxl", skiprows=1)  # Skip header row
        else:
            logging.warning(f"Unsupported file format: {file_path}")
            return

        logging.info(f"Processing file: {file_path} - {df.shape[0]} rows")

        # Print the first chunk for debugging
        first_chunk = df.iloc[0 : CHUNK_SIZE]
        non_null_data = extract_non_null_data(first_chunk)
        dataset_text = " ".join(non_null_data)  # Convert list to a single string

        prompt = f"""
        Detect any Personally Identifiable Information (PII) as per the PII Categories in the dataset. 
        If PII is found, list it.
        If no PII is found, respond with: 'No PII detected.'
        Do not describe or analyze the dataset.

        PII Categories:
        {PII_DEFINITIONS}

        Dataset:
        {dataset_text}
        """

        print("\n" + "=" * 80)
        print(f"Prompt for Rows 0 to {CHUNK_SIZE - 1}:")
        print(prompt)
        print("=" * 80 + "\n")

        # Process all chunks and send them to the LLM
        for i in tqdm(range(0, len(df), CHUNK_SIZE), desc=f"Processing {file_path}"):
            chunk = df.iloc[i : i + CHUNK_SIZE]
            result = scan_chunk_with_llm(chunk)

            if result:
                if "No PII detected" in result:
                    print(f"No PII detected in {file_path} (Rows {i}-{i + CHUNK_SIZE - 1}).")
                    logging.info(f"No PII detected in {file_path} (Rows {i}-{i + CHUNK_SIZE - 1}).")
                else:
                    print(f"\n🚀 Detected PII in {file_path} (Rows {i}-{i + CHUNK_SIZE - 1}):\n", result)
                    logging.info(f"PII detected in {file_path} (Rows {i}-{i + CHUNK_SIZE - 1}): {result}")

    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")

# Main function to scan all files in the Data folder
def main():
    if not os.path.exists(DATA_FOLDER):
        logging.error(f"Data folder '{DATA_FOLDER}' not found!")
        return

    # Check if Ollama server is running
    if not check_ollama_server():
        logging.error("Ollama server is not running or inaccessible!")
        print("Ollama server is not running or inaccessible!")
        return

    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith((".csv", ".xls", ".xlsx"))]
    
    if not files:
        logging.info("No datasets found in the Data folder.")
        print("No datasets found.")
        return

    for file in files:
        process_file(os.path.join(DATA_FOLDER, file))

if __name__ == "__main__":
    main()