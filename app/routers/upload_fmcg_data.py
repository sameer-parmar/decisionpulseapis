# routes/upload.py (or wherever your router/task lives)

from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks # , Depends # If using Depends
from sqlalchemy.orm import Session
import csv
import io
from app.database import SessionLocal #, get_db # Assuming SessionLocal is your factory
# Import all needed models
from app.models.datapoints import DataPoint, Country, Category, Brand # ADDED Brand

router = APIRouter()

# --- Helper for DB Lookups (get_or_create_lookup - unchanged) ---
def get_or_create_lookup(db: Session, model, name_field, name_value, cache: dict):
    """Looks up or creates a Country/Category/Brand, using a simple cache."""
    if not name_value: # Handle empty names early
        print(f"Warning: Attempted lookup with empty name for {model.__name__}.")
        return None
    # Normalize cache key slightly if needed (e.g., lowercasing names for lookup)
    # For now, assumes exact match from CSV matches DB casing.
    cache_key = (model.__tablename__, name_value)
    if cache_key in cache:
        return cache[cache_key]

    instance = db.query(model).filter(getattr(model, name_field) == name_value).first()
    if not instance:
        print(f"Info: '{name_value}' not found for {model.__name__}. Creating new entry.")
        instance = model(**{name_field: name_value})
        try:
            db.add(instance)
            # Flush to get ID if needed before commit, Commit to make it available
            # Committing immediately can be safer in background tasks to avoid downstream errors
            # on data that wasn't actually saved yet.
            db.commit()
            print(f"Info: Successfully created new {model.__name__}: {name_value} (ID: {instance.id})")
            cache[cache_key] = instance # Add to cache *after* successful creation and commit
        except Exception as e:
             db.rollback()
             print(f"Error: Failed to create {model.__name__} '{name_value}': {e}. Skipping related row.")
             # Optionally log the full error e
             return None # Failed to create
    else:
         cache[cache_key] = instance # Add existing to cache
    return instance


def process_csv_data(csv_content: str, db: Session):
    """Processes CSV data in the background."""
    print("Background task: Starting CSV processing...")
    inserted_count = 0
    skipped_count = 0
    processed_count = 0
    batch = []
    batch_size = 100 # Adjust as needed

    # Required headers - same as before, the column name in CSV is still 'Brand'
    required_headers = {'Country', 'Year', 'Brand', 'Metric', 'Value', 'Source URL', 'Category'}
    optional_headers = {'Summary', 'Insight'}
    all_expected_headers = required_headers.union(optional_headers)

    # Cache for Lookups
    lookup_cache = {}

    try:
        csvfile = io.StringIO(csv_content)
        reader = csv.DictReader(csvfile)

        # --- Header Check (remains the same logic) ---
        if not reader.fieldnames:
             print("Background task Error: CSV file has no headers.")
             db.close()
             return
        found_headers_lower = {str(h).strip().lower() for h in reader.fieldnames if h}
        required_headers_lower = {h.lower() for h in required_headers}
        missing_req = required_headers_lower - found_headers_lower
        if missing_req:
             original_missing = [h for h in required_headers if h.lower() in missing_req]
             print(f"Background task Error: Missing required CSV headers: {original_missing}. Aborting.")
             db.close()
             return
        print("Background task: Headers validated.")
        # --- End Header Check ---

        for row_num, row in enumerate(reader, start=1):
            processed_count += 1
            cleaned_row = {str(k).strip() if k else None: str(v).strip() if v else '' for k, v in row.items()}

            # Use the parsing method (which now returns brand_name)
            parsed_data = DataPoint.parse_csv_row(cleaned_row, all_expected_headers, required_headers)

            if parsed_data is None:
                skipped_count += 1
                continue

            try:
                # --- Perform DB Lookups (including Brand) ---
                country_name = parsed_data['country_name']
                category_name = parsed_data['category_name']
                brand_name = parsed_data['brand_name'] # Get the brand name

                # Use the generic helper for all lookups
                country_obj = get_or_create_lookup(db, Country, 'name', country_name, lookup_cache)
                category_obj = get_or_create_lookup(db, Category, 'name', category_name, lookup_cache)
                brand_obj = get_or_create_lookup(db, Brand, 'name', brand_name, lookup_cache) # ADDED Brand lookup

                # --- Check if all lookups succeeded ---
                if not country_obj:
                     print(f"Background task Warning: Skipping row {row_num} due to missing/uncreatable Country '{country_name}'")
                     skipped_count += 1
                     continue # Skip to next row
                if not category_obj:
                     print(f"Background task Warning: Skipping row {row_num} due to missing/uncreatable Category '{category_name}'")
                     skipped_count += 1
                     continue # Skip to next row
                if not brand_obj: # ADDED check for brand
                     print(f"Background task Warning: Skipping row {row_num} due to missing/uncreatable Brand '{brand_name}'")
                     skipped_count += 1
                     continue # Skip to next row

                # --- Create DataPoint Instance ---
                data_point = DataPoint(
                    # Assign Foreign Key IDs
                    country=country_obj.id,
                    category=category_obj.id,
                    brand=brand_obj.id, # MODIFIED: Assign the Brand ID
                    # Assign other parsed data
                    source_url=parsed_data['source_url'],
                    insight=parsed_data['insight'] or "",
                    summary=parsed_data['summary'],
                    year=parsed_data['year'],
                    metric=parsed_data['metric'],
                    value=parsed_data['value'],
                )

                # Assign Metric Category
                data_point.assign_category()

                # Add to Batch
                batch.append(data_point)

                # Commit Batch logic remains the same
                if len(batch) >= batch_size:
                    # ... (commit batch or fallback to row-by-row) ...
                    try:
                        db.add_all(batch)
                        db.commit()
                        inserted_count += len(batch)
                        # print(f"Background task: Committed batch of {len(batch)} rows.") # Optional: Less verbose logging
                        batch = []
                    except Exception as e:
                        db.rollback()
                        print(f"Background task Error: Batch insert failed: {e}. Retrying row-by-row for this batch.")
                        for item_index, bp in enumerate(batch):
                            try:
                                db.add(bp)
                                db.commit()
                                inserted_count += 1
                            except Exception as individual_e:
                                db.rollback()
                                print(f"Background task Error: Row {row_num - len(batch) + item_index + 1} failed individual insert: {individual_e}. Skipping.")
                                skipped_count += 1
                        batch = []


            except Exception as e:
                 db.rollback()
                 print(f"Background task Error: Failed to process row {row_num} (lookup/instance creation): {e}. Skipping.")
                 # Consider logging traceback:
                 # import traceback
                 # traceback.print_exc()
                 skipped_count += 1

        # --- Insert final remaining batch (logic remains the same) ---
        if batch:
            print(f"Background task: Committing final batch of {len(batch)} rows.")
            # ... (commit final batch or fallback to row-by-row) ...
            try:
                db.add_all(batch)
                db.commit()
                inserted_count += len(batch)
            except Exception as e:
                db.rollback()
                print(f"Background task Error: Final batch insert failed: {e}. Retrying row-by-row.")
                for item_index, bp in enumerate(batch):
                     try:
                         db.add(bp)
                         db.commit()
                         inserted_count += 1
                     except Exception as individual_e:
                         db.rollback()
                         final_row_num_estimate = processed_count - len(batch) + item_index + 1 # Estimate row number
                         print(f"Background task Error: Row {final_row_num_estimate} (final batch) failed individual insert: {individual_e}. Skipping.")
                         skipped_count += 1

    # --- Exception Handling and Cleanup (remains the same) ---
    except csv.Error as e:
         print(f"Background task Error: CSV parsing error: {e}")
         db.rollback()
    except Exception as e:
        print(f"Background task Error: An unexpected error occurred during processing: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        print(f"Background task: Finished processing. Processed: {processed_count}, Inserted: {inserted_count}, Skipped: {skipped_count}")
        if db.is_active:
             db.close()
        print("Background task: Database session closed.")


# --- API Endpoint (upload_fmcg_csv - unchanged) ---
# The endpoint itself doesn't need modification as it just handles
# file upload and task scheduling. The CSV format expected still
# has a 'Brand' column.
@router.post("/upload-fmcg-csv/")
async def upload_fmcg_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    # ... (endpoint logic remains the same as in the previous version) ...
    """
    Uploads a CSV file with FMCG data and processes it in the background.

    **Expected CSV Columns (Header names are case-insensitive):**

    * **Required:** `Country`, `Year`, `Brand`, `Metric`, `Value`, `Source URL`, `Category`
    * **Optional:** `Summary`, `Insight`
    """
    if not file.filename or not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file (.csv).")

    try:
        contents = await file.read()
        try:
            csv_content = contents.decode('utf-8')
        except UnicodeDecodeError:
            print("Warning: Could not decode file as UTF-8, trying Latin-1.")
            try:
                csv_content = contents.decode('latin-1')
            except Exception as decode_err:
                 raise HTTPException(status_code=400, detail=f"Error decoding file: Neither UTF-8 nor Latin-1 worked. ({decode_err})")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading uploaded file: {e}")
    finally:
        await file.close()

    if not csv_content.strip():
           raise HTTPException(status_code=400, detail="CSV file appears to be empty or contains only whitespace.")

    print(f"Received file '{file.filename}'. Scheduling background processing.")
    background_tasks.add_task(process_csv_data, csv_content, SessionLocal())

    return {"message": f"File '{file.filename}' received. Processing started in the background."}