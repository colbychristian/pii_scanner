import os
import pandas as pd

# Configuration
DATA_FOLDER = "Data"
CHUNK_SIZE = 20  # Chunk size for testing

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

# Function to process files and print the prompt and data
def process_file(file_path):
    try:
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == ".csv":
            df = pd.read_csv(file_path, encoding="latin1", low_memory=False)
        elif file_extension in [".xls", ".xlsx"]:
            df = pd.read_excel(file_path, engine="openpyxl")
        else:
            print(f"Unsupported file format: {file_path}")
            return

        print(f"Processing file: {file_path} - {df.shape[0]} rows")

        # Process in chunks and print the prompt and data
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE]
            chunk_data = chunk.to_string(index=False)  # Convert chunk to plain string

            prompt = f"""
            Detect any Personally Identifiable Information (PII) within the dataset. 
            If PII is found, return it as a list.
            If no PII is found, respond with: 'No PII detected.'
            Do not return a description of the dataset.

            PII Categories:
            {PII_DEFINITIONS}

            Dataset:
            {chunk_data}
            """

            print("\n" + "=" * 80)
            print(f"Prompt for Rows {i} to {i + CHUNK_SIZE - 1}:")
            print(prompt)
            print("=" * 80 + "\n")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Main function to scan all files in the Data folder
def main():
    if not os.path.exists(DATA_FOLDER):
        print(f"Data folder '{DATA_FOLDER}' not found!")
        return

    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith((".csv", ".xls", ".xlsx"))]
    
    if not files:
        print("No datasets found in the Data folder.")
        return

    for file in files:
        process_file(os.path.join(DATA_FOLDER, file))

if __name__ == "__main__":
    main()