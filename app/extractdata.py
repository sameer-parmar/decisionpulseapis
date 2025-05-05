import pandas as pd
# Import the OpenAI library, which will be used to interact with the DeepSeek API
try:
    from openai import OpenAI
except ImportError:
    print("Error: The 'openai' library is not found.")
    print("Please install it using: pip install openai")
    OpenAI = None # Set to None if the library isn't available


import json
import os
import math

# --- Configuration ---
# Your DeepSeek API key.
# It's recommended to store API keys securely, e.g., in environment variables.
API_KEY = os.environ.get("DEEPSEEK_API_KEY", "YOUR_DEEPSEEK_API_KEY")
if API_KEY == "YOUR_DEEPSEEK_API_KEY":
    print("WARNING: Replace 'YOUR_DEEPSEEK_API_KEY' with your actual DeepSeek API key or set the DEEPSEEK_API_KEY environment variable.")
    # Consider exiting or raising an error here in production

# Configure the OpenAI client to point to the DeepSeek API
deepseek_client = None
if OpenAI and API_KEY != "YOUR_DEEPSEEK_API_KEY":
    try:
        deepseek_client = OpenAI(api_key=API_KEY, base_url="https://api.deepseek.com")
        # The model name you want to use (e.g., "deepseek-chat" or another "top model" name)
        DEEPSEEK_MODEL_NAME = 'deepseek-chat' # Or the specific model name you need
    except Exception as e:
        print(f"Error initializing DeepSeek client using OpenAI SDK: {e}")
        print("Please ensure your API key is correct and the base_url is accurate.")
        deepseek_client = None
elif OpenAI:
     print("DeepSeek API key not set. DeepSeek client not initialized.")


# --- Extraction Prompt Template for Chunks ---
# This prompt will be sent to the DeepSeek model via the chat completion endpoint.
# We include a system message to set the AI's role.
CHUNK_EXTRACTION_MESSAGES_TEMPLATE = [
    {"role": "system", "content": "You are an expert data extraction assistant. Your task is to carefully read the provided text and extract specific pieces of information about sales and inventory records. Format the extracted data as a JSON array of JSON objects."},
    {"role": "user", "content": """
Analyze the following text chunk, which contains unstructured sales and market data.
Identify and extract all distinct records related to sales or inventory performance.
A record typically includes:
- sales_revenue (numerical value, e.g., 4350000000 for "USD 4.35 Billion")
- sales_units (numerical value)
- sales_date (standardized format, e.g., "YYYY-MM-DD" or "YYYY-QX")
- sales_region (e.g., "USA", "Australia", "South region")
- product_segment (e.g., "SUVs", "Passenger Cars", "Mini", "Technology Retail")
- inventory_level (a numerical value if present)
- Any associated Source Information (source_country, source_origin, source_table mentioned nearby)

For each identified record, extract the available information and format it as a JSON object.
Collect all found JSON objects into a single JSON array.
If no records are found in the chunk, return an empty JSON array [].
Do NOT include any introductory or concluding text, only the JSON array.

Text Chunk:
---
{text_chunk}
---

JSON Array Output:
"""}
]


# Define a rough chunk size (number of lines).
# This needs tuning based on the DeepSeek model's context window limits and performance.
# Start small, test with your data, and increase if possible while monitoring accuracy and cost.
LINES_PER_CHUNK = 20 # Example size, adjust as needed

def extract_records_from_chunk_with_deepseek(text_chunk, client, model_name):
    """
    Sends a text chunk to a DeepSeek model (via OpenAI SDK) for extraction of multiple records.

    Args:
        text_chunk (str): A large string containing multiple lines/rows from the CSV.
        client: The initialized OpenAI client object configured for DeepSeek.
        model_name (str): The name of the DeepSeek model to use.

    Returns:
        list or None: A list of dictionaries (records) if successful, None otherwise.
    """
    if client is None:
        return None

    # Prepare the messages for the chat completion API call
    messages = [
        {"role": msg["role"], "content": msg["content"].format(text_chunk=text_chunk) if "{text_chunk}" in msg["content"] else msg["content"]}
        for msg in CHUNK_EXTRACTION_MESSAGES_TEMPLATE
    ]


    try:
        # --- THIS IS THE PART USING THE OPENAI SDK TO CALL DEEPSEEK ---
        response = client.chat.completions.create(
            model=model_name,
            messages=messages,
            # Use response_format if the model supports it for reliable JSON output
            # Check DeepSeek's documentation if their models support this parameter
            # response_format={"type": "json_object"},
            temperature=0 # Use low temperature for extraction tasks
        )

        # Assuming the response structure is standard chat completion response
        if not response or not response.choices:
             print(f"API response is empty or invalid for chunk: {text_chunk[:100]}...")
             return None

        # The model's text output should contain the JSON string
        json_string = response.choices[0].message.content.strip()

        # Attempt to parse the JSON string returned by the API
        try:
            extracted_data_list = json.loads(json_string)
            # Ensure it's a list as expected from the prompt
            if isinstance(extracted_data_list, list):
                return extracted_data_list
            else:
                 print(f"API did not return a JSON list for chunk: {text_chunk[:100]}... Response text: {json_string[:100]}...")
                 # If it's not a list but is valid JSON (e.g., a single object), you might decide how to handle it
                 return None # Or return [extracted_data_list] if a single object is expected sometimes
        except json.JSONDecodeError:
            print(f"Failed to parse JSON from API response for chunk: {text_chunk[:100]}... Response text: {json_string[:100]}...")
            return None # Or try more robust JSON parsing/error recovery


    except Exception as e:
        print(f"DeepSeek API call failed for chunk: {text_chunk[:100]}... Error: {e}")
        return None


def process_csv_in_chunks_with_deepseek(file_path):
    """
    Reads an unstructured sales CSV in chunks, uses DeepSeek API (via OpenAI SDK)
    to extract multiple records from each chunk, and returns a list of extracted
    data dictionaries.

    Args:
        file_path (str): The path to the unstructured CSV file.

    Returns:
        list: A list of dictionaries, where each dictionary is an extracted record.
    """
    all_extracted_records = []

    if deepseek_client is None:
        print("DeepSeek client not initialized. Cannot process data.")
        return []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Filter out potentially empty or pure header lines if necessary
        # Keep lines that contain some characters, potentially refine this filter
        lines = [line for line in lines if line.strip()]
        # Optional: Filter out lines that look like the header row if it might appear multiple times
        # lines = [line for line in lines if not line.startswith('sales_revenue,sales_units')]


        num_lines = len(lines)
        if num_lines == 0:
            print("No significant lines found in the file after basic filtering.")
            return []

        num_chunks = math.ceil(num_lines / LINES_PER_CHUNK)

        print(f"Total lines to process: {num_lines}")
        print(f"Processing in {num_chunks} chunks of approx {LINES_PER_CHUNK} lines...")

        for i in range(num_chunks):
            start_index = i * LINES_PER_CHUNK
            end_index = min((i + 1) * LINES_PER_CHUNK, num_lines)
            current_chunk_lines = lines[start_index:end_index]
            text_chunk = "".join(current_chunk_lines) # Join lines into a single string chunk

            if not text_chunk.strip(): # Skip completely empty chunks
                continue

            print(f"\nProcessing Chunk {i+1}/{num_chunks} (Lines {start_index+1}-{end_index})...") # Use 1-based indexing for display

            # Use the function that calls the DeepSeek API
            extracted_records_in_chunk = extract_records_from_chunk_with_deepseek(
                text_chunk,
                deepseek_client,
                DEEPSEEK_MODEL_NAME # Pass the model name
            )

            if extracted_records_in_chunk:
                all_extracted_records.extend(extracted_records_in_chunk) # Add found records to the main list
                print(f"Extracted {len(extracted_records_in_chunk)} records from chunk {i+1}")


    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred during file reading or chunk processing: {e}")
        return []

    return all_extracted_records

# --- How to use the script ---
file_name = 'your_unstructured_data.csv' # Replace with the actual name of your file

# Ensure your DeepSeek API key is set (e.g., via environment variable DEEPSEEK_API_KEY)
# or replace "YOUR_DEEPSEEK_API_KEY" directly in the script (less secure).

extracted_data_records = process_csv_in_chunks_with_deepseek(file_name)

# Now 'extracted_data_records' is a list of dictionaries, where each dictionary is a structured record
if extracted_data_records:
    structured_df = pd.DataFrame(extracted_data_records)
    print("\n--- Extracted Data (First 5 records) ---")
    print(structured_df.head())
    print(f"\nTotal records extracted: {len(structured_df)}")

    # --- Next Steps: Data Cleaning and KPI Calculation ---
    # The extracted data is now in a DataFrame, but values might still be strings.
    # You need to clean columns (e.g., convert 'sales_revenue' to numbers, 'sales_date' to datetime).

    # Example Cleaning (requires careful handling of potential None/missing values from extraction):
    # structured_df['sales_revenue'] = pd.to_numeric(structured_df['sales_revenue'], errors='coerce')
    # structured_df['sales_units'] = pd.to_numeric(structured_df['sales_units'], errors='coerce')
    # Try converting date (needs robust parsing based on your date formats)
    # structured_df['sales_date'] = pd.to_datetime(structured_df['sales_date'], errors='coerce')


    # Once columns are cleaned and correctly typed, you can calculate KPIs:
    # total_revenue = structured_df['sales_revenue'].sum()
    # sales_by_region = structured_df.groupby('sales_region')['sales_revenue'].sum()
    # (Requires date cleaning first) YOY_growth = ...


else:
    print("\nNo records were extracted.")