from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
import csv
import io
import os  # For file system operations
from app.database import SessionLocal
from app.models.datapoints import DataPoint, Country, Category, Brand
from typing import Optional, Union

router = APIRouter()

# --- Configuration for Upload Directory ---
UPLOAD_DIR = "uploaded_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)  # Ensure the directory exists


# --- Helper for DB Lookups (get_or_create_lookup - unchanged) ---
def get_or_create_lookup(db: Session, model, name_field, name_value, cache: dict):
    """Looks up or creates a Country/Category/Brand, using a simple cache."""
    if not name_value:
        print(f"Warning: Attempted lookup with empty name for {model.__name__}.")
        return None
    cache_key = (model.__tablename__, name_value)
    if cache_key in cache:
        return cache[cache_key]

    instance = db.query(model).filter(getattr(model, name_field) == name_value).first()
    if not instance:
        print(f"Info: '{name_value}' not found for {model.__name__}. Creating new entry.")
        instance = model(**{name_field: name_value})
        try:
            db.add(instance)
            db.commit()
            print(f"Info: Successfully created new {model.__name__}: {name_value} (ID: {instance.id})")
            cache[cache_key] = instance
        except Exception as e:
            db.rollback()
            print(f"Error: Failed to create {model.__name__} '{name_value}': {e}. Skipping related row.")
            return None
    else:
        cache[cache_key] = instance
    return instance


async def save_uploaded_file(file: UploadFile, destination: str) -> None:
    """Saves the uploaded file to the specified destination."""
    try:
        contents = await file.read()
        with open(destination, "wb") as f:
            f.write(contents)
    except Exception as e:
        print(f"Error saving file '{file.filename}' to '{destination}': {e}")
    finally:
        await file.close()


def process_csv_data(csv_content: str, db: Session):
    """Processes CSV data in the background, ensuring UUID FKs match."""
    print("Background task: Starting CSV processing...")
    inserted_count = 0
    skipped_count = 0
    processed_count = 0
    batch = []
    batch_size = 100

    required_headers = {'Country', 'Year', 'Brand', 'Metric', 'Value', 'Category', 'Unit'}  # Added 'Unit'
    optional_headers = {'Summary', 'Insight'}
    all_expected_headers = required_headers.union(optional_headers)

    lookup_cache = {}

    try:
        csvfile = io.StringIO(csv_content)
        reader = csv.DictReader(csvfile)

        # validate headers
        if not reader.fieldnames:
            print("Background task Error: CSV file has no headers.")
            db.close()
            return
        found = {h.strip().lower() for h in reader.fieldnames if h}
        missing = {h.lower() for h in required_headers} - found
        if missing:
            orig = [h for h in required_headers if h.lower() in missing]
            print(f"Background task Error: Missing required CSV headers: {orig}. Aborting.")
            db.close()
            return
        print("Background task: Headers validated.")

        for row_num, row in enumerate(reader, start=1):
            processed_count += 1
            cleaned = {k.strip(): v.strip() for k, v in row.items() if k}
            parsed = DataPoint.parse_csv_row(cleaned, all_expected_headers, required_headers)
            if parsed is None:
                skipped_count += 1
                continue

            # lookups (or creation) for Country, Category, Brand
            country_obj = get_or_create_lookup(db, Country, 'name', parsed['country_name'], lookup_cache)
            category_obj = get_or_create_lookup(db, Category, 'name', parsed['category_name'], lookup_cache)
            brand_obj = get_or_create_lookup(db, Brand, 'name', parsed['brand_name'], lookup_cache)

            if not (country_obj and category_obj and brand_obj):
                print(f"Background task Warning: Skipping row {row_num} due to missing lookup.")
                skipped_count += 1
                continue

            # —— Ensure the looked‑up Category/Brand carry the correct UUID FKs ——
            # Category.country → Country.id
            if category_obj.country is None:
                category_obj.country = country_obj.id
                db.add(category_obj)
                db.commit()

            # Brand.country → Country.id, Brand.category → Category.id
            updated = False
            if brand_obj.country is None:
                brand_obj.country = country_obj.id
                updated = True
            if brand_obj.category is None:
                brand_obj.category = category_obj.id
                updated = True
            if updated:
                db.add(brand_obj)
                db.commit()

            # build DataPoint with matching UUIDs
            dp = DataPoint(
                country=country_obj.id,
                category=category_obj.id,
                brand=brand_obj.id,
                source_url=parsed['source_url'],
                insight=parsed['insight'] or "",
                summary=parsed['summary'],
                year=parsed['year'],
                metric=parsed['metric'],
                value=parsed['value'],
                unit=parsed['unit'],  # Include the 'unit' field
            )
            dp.assign_category()
            batch.append(dp)

            # batch insert
            if len(batch) >= batch_size:
                try:
                    db.add_all(batch)
                    db.commit()
                    inserted_count += len(batch)
                    batch = []
                except Exception as e:
                    db.rollback()
                    print(f"Background task Error: Batch insert failed: {e}. Retrying individually.")
                    for idx, item in enumerate(batch):
                        try:
                            db.add(item)
                            db.commit()
                            inserted_count += 1
                        except Exception as ind_e:
                            db.rollback()
                            print(f"Background task Error: Row {row_num - len(batch) + idx + 1} failed: {ind_e}")
                            skipped_count += 1
                    batch = []

        # final batch
        if batch:
            print(f"Background task: Committing final batch of {len(batch)} rows.")
            try:
                db.add_all(batch)
                db.commit()
                inserted_count += len(batch)
            except Exception as e:
                db.rollback()
                print(f"Background task Error: Final batch failed: {e}. Retrying individually.")
                for idx, item in enumerate(batch):
                    try:
                        db.add(item)
                        db.commit()
                        inserted_count += 1
                    except Exception as ind_e:
                        db.rollback()
                        print(f"Background task Error: Final row {idx + 1} failed: {ind_e}")
                        skipped_count += 1

    except csv.Error as e:
        print(f"Background task Error: CSV parsing error: {e}")
        db.rollback()
    except Exception as e:
        print(f"Background task Error: Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        print(
            f"Background task: Done. Processed: {processed_count}, "
            f"Inserted: {inserted_count}, Skipped: {skipped_count}"
        )
        if db.is_active:
            db.close()
        print("Background task: DB session closed.")


@router.post("/upload-fmcg-csv/")
async def upload_fmcg_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    save_file: Optional[bool] = False  # New parameter to control file saving
):
    """
    Uploads a CSV file with FMCG data, optionally saves it, and processes it in the background.

    **Expected CSV Columns (Header names are case-insensitive):**

    * **Required:** `Country`, `Year`, `Brand`, `Metric`, `Value`, `Source URL`, `Category`, `Unit`
    * **Optional:** `Summary`, `Insight`

    **Optional Parameter:**

    * `save_file`: Set to `True` to save the uploaded CSV file to the server. Defaults to `False`.
    """
    if not file.filename or not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file (.csv).")

    file_path: Optional[str] = None
    if save_file:
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        await save_uploaded_file(file, file_path)
        print(f"File '{file.filename}' saved to '{file_path}'.")

    try:
        contents = await file.read()
        try:
            csv_content = contents.decode('utf-8')
        except UnicodeDecodeError:
            print("Warning: Could not decode file as UTF-8, trying Latin-1.")
            try:
                csv_content = contents.decode('latin-1')
            except Exception as decode_err:
                raise HTTPException(
                    status_code=400,
                    detail=f"Error decoding file: Neither UTF-8 nor Latin-1 worked. ({decode_err})")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading uploaded file: {e}")
    finally:
        await file.close()

    if not csv_content.strip():
        raise HTTPException(
            status_code=400,
            detail="CSV file appears to be empty or contains only whitespace.")

    print(f"Received file '{file.filename}'. Scheduling background processing.")
    background_tasks.add_task(process_csv_data, csv_content, SessionLocal())

    return {"message": f"File '{file.filename}' received. Processing started in the background.",
            "file_saved_to": file_path if save_file else None}
