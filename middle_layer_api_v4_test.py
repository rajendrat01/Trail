
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from pydantic import BaseModel, Field, ValidationError, model_validator, constr
from typing import List, Optional
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import datetime
import jwt
from enum import Enum
import pika
import json
from app.core.config import settings
from collections import defaultdict
import pandas as pd
from copy import deepcopy
import aiohttp
import asyncio
import math
import os
import pickle
import time

# Constants for NHAI One API
NHAI_ONE_API_URL = "https://datalakeg.nhai.gov.in/nhaiapi/api/MastersAPI/GetMaintenanceDefectMaster"
NHAI_ONE_API_TIMEOUT = 600  # 10 minutes for total request
NHAI_ONE_API_READ_TIMEOUT = 600  # 10 minutes for reading response

DUPLICATE_CHECK_ENABLED = False

# Enumerations
class TypeCarriageway(int, Enum):
    MAIN_CARRIAGEWAY = 1
    SERVICE_ROAD = 2
    SLIP_ROAD = 3
    LOOPS = 4
    RAMPS = 5


class DefectTypeID(int, Enum):
    POTHOLE = 1
    CRACK = 2
    ILLEGAL_MEDIAN_OPENING = 3
    DEFORMED_SHAPE_OR_MISPLACED_POSITION_OF_ROAD_SIGNS = 4


class AssetTypeID(int, Enum):
    FLEXIBLE_PAVEMENT = 1
    RIGID_PAVEMENTS = 2
    BRIDGE = 3
    ROAD_FURNITURE = 4


class Side(int, Enum):
    LHS = 0
    MEDIAN = 1
    RHS = 2
    BOTH_SIDES = 3


# Constants for duplicate detection
DEFAULT_DUPLICATE_DISTANCE_THRESHOLD = 10  # meters
DEFECT_TYPE_DISTANCE_THRESHOLDS = {
    DefectTypeID.POTHOLE: 10,  # 10 meters for potholes
    DefectTypeID.CRACK: 20,    # 20 meters for cracks
    DefectTypeID.ILLEGAL_MEDIAN_OPENING: 50,  # 50 meters for illegal median openings
    DefectTypeID.DEFORMED_SHAPE_OR_MISPLACED_POSITION_OF_ROAD_SIGNS: 50  # 50 meters for deformed shape or misplaced position of road signs
}

# Photo Model
class Photo(BaseModel):
    photo_base64: constr(strip_whitespace=True, min_length=1)  # Must be non-empty Base64 string


# Defect Model
class Defect(BaseModel):
    asset_type: Optional[AssetTypeID] = Field(None, description="Type of asset associated with the defect (optional)")
    upc: str = Field(..., description="Unique Project Code")
    defect_name: str = Field(..., description="Name of the defect")
    defect_type_id: DefectTypeID
    datetime_identification: datetime.datetime
    photos_list: List[Photo]
    side: Side
    lane: Optional[str] = None
    severity: Optional[str] = None
    type_carriageway: TypeCarriageway
    chainage_from: Optional[float] = None
    chainage_to: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    remarks: Optional[str] = None
    source: str
    external_source_id: str

    @model_validator(mode="after")
    def validate_mandatory_fields(cls, values):
        latitude = values.latitude
        longitude = values.longitude
        chainage_from = values.chainage_from
        chainage_to = values.chainage_to

        if not ((latitude and longitude) or (chainage_from and chainage_to)):
            raise ValueError(
                "Either both chainage_from/chainage_to or latitude/longitude must be provided."
            )
        if len(values.photos_list) == 0:
            raise ValueError("At least one photo must be included in photos_list.")

        return values


def calculate_haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on the earth (specified in decimal degrees)
    Returns distance in meters
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371000  # Radius of earth in meters
    return c * r


def get_nhai_defect_name(defect_type_id):
    """
    Convert our defect type ID to the corresponding NHAI API defect name.
    """
    mapping = {
        DefectTypeID.POTHOLE: "Pothole",
        DefectTypeID.CRACK: "Cracking",
        DefectTypeID.ILLEGAL_MEDIAN_OPENING: "Illegal Median Opening"
    }
    return mapping.get(defect_type_id, "")


# Cache file path (shared with cron job) - use config setting
CACHE_FILE_PATH = settings.nhai_defects_cache_path
CACHE_DURATION = 2100  # 35 minutes in seconds (slightly longer than 30-minute cron interval)

def get_cached_nhai_defects(upc: str = None):
    """
    Get cached NHAI One defects from file, optionally filtered by UPC.
    Returns empty list if cache is stale or empty.
    """
    try:
        print(f"DEBUG: Checking cache at path: {CACHE_FILE_PATH}")
        if not os.path.exists(CACHE_FILE_PATH):
            print("DEBUG: Cache file does not exist")
            return []
        
        print("DEBUG: Cache file exists, loading...")
        with open(CACHE_FILE_PATH, 'rb') as f:
            cache_data = pickle.load(f)
        
        print(f"DEBUG: Cache loaded, last_update: {cache_data.get('last_update')}")
        
        # Check if cache is stale
        if (cache_data["last_update"] is None or 
            (datetime.datetime.now() - cache_data["last_update"]).total_seconds() > CACHE_DURATION):
            print(f"DEBUG: Cache is stale. Age: {(datetime.datetime.now() - cache_data['last_update']).total_seconds() if cache_data['last_update'] else 'None'} seconds, threshold: {CACHE_DURATION} seconds")
            return []
        
        print("DEBUG: Cache is fresh, extracting data...")
        nhai_defects_cache = cache_data["cache"]
        
        if upc:
            defects = nhai_defects_cache.get(upc, [])
            print(f"DEBUG: Found {len(defects)} defects for UPC {upc}")
            return defects
        else:
            # Return all defects
            all_defects = []
            for upc_defects in nhai_defects_cache.values():
                all_defects.extend(upc_defects)
            print(f"DEBUG: Found {len(all_defects)} total defects")
            return all_defects
            
    except Exception as e:
        print(f"DEBUG: Error reading cache file: {str(e)}")
        return []


async def check_duplicate_defect_cached(defect):
    """
    Check if a defect is a duplicate using cached NHAI One data.
    This is much faster than real-time API calls.
    """
    print(f"Checking for duplicates using cached data for UPC: {defect.upc}")
    
    # Get cached defects for this UPC
    cached_defects = get_cached_nhai_defects(defect.upc)
    
    if not cached_defects:
        print("No cached data available for this UPC, skipping duplicate check")
        return []
    
    print(f"Found {len(cached_defects)} cached defects for UPC {defect.upc}")
    return filter_matching_defects(cached_defects, defect)


# Keep the original function for backward compatibility
async def check_duplicate_defect(defect):
    """
    Check if a defect is a duplicate by querying NHAI One system.
    Returns list of matching defects if found, empty list if no duplicates.
    """
    if not DUPLICATE_CHECK_ENABLED:
        return []
    
    # Use cached version if available, otherwise fall back to real-time API
    cached_result = await check_duplicate_defect_cached(defect)
    if cached_result:
        return cached_result
    
    # Fall back to real-time API if cache is empty
    print(f"Cache empty, falling back to real-time API call for UPC {defect.upc}")
    timeout = aiohttp.ClientTimeout(total=NHAI_ONE_API_TIMEOUT, sock_read=NHAI_ONE_API_READ_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print(f"Making request to NHAI One API to fetch all defects")
        try:
            # Call API without parameters to get all defects
            async with session.get(NHAI_ONE_API_URL) as response:
                print(f"NHAI One API response status: {response.status}")
                if response.status == 200:
                    try:
                        response_text = await response.text()
                        print(f"NHAI One API response received")
                        print(f"Response length: {len(response_text)} characters")
                        
                        # Try to parse the response as JSON
                        try:
                            nhai_defects = json.loads(response_text)
                            if isinstance(nhai_defects, str):
                                # If it's a string, try to parse it again (in case it's a JSON string)
                                nhai_defects = json.loads(nhai_defects)
                            return filter_matching_defects(nhai_defects, defect)
                        except json.JSONDecodeError as e:
                            print(f"Error parsing JSON response: {str(e)}")
                            return []
                            
                    except asyncio.TimeoutError:
                        print("Timeout while reading response body")
                        return []
                print(f"Non-200 response from NHAI One API: {response.status}")
                return []
        except asyncio.TimeoutError:
            print("Timeout while making request to NHAI One API")
            return []
        except Exception as e:
            # Log the error but don't fail the request
            print(f"Error checking duplicates: {str(e)}")
            print(f"Error type: {type(e)}")
            import traceback
            print(f"Full traceback: {traceback.format_exc()}")
            return []


def filter_matching_defects(nhai_defects, defect):
    """
    Filter defects from NHAI One to find matches based on location and characteristics.
    Uses Haversine distance for GPS coordinate matching.
    """
    print(f"\n=== Starting duplicate check ===")
    print(f"Input defect details:")
    print(f"- Location: lat={defect.latitude}, long={defect.longitude}")
    print(f"- Chainage: from={defect.chainage_from}, to={defect.chainage_to}")
    print(f"- UPC: {defect.upc}")
    print(f"- Defect Type ID: {defect.defect_type_id}")
    print(f"- Defect Name: {defect.defect_name}")
    print(f"- Asset Type: {defect.asset_type}")
    print(f"- Side: {defect.side}")
    print(f"- Carriageway: {defect.type_carriageway}")
    
    # Accept both dict-with-data and list
    if isinstance(nhai_defects, dict) and "data" in nhai_defects:
        nhai_defects = nhai_defects["data"]
    elif isinstance(nhai_defects, list):
        pass  # Already a list, do nothing
    else:
        print("Response does not contain 'data' array or is not a list")
        return []
        
    print(f"\nNumber of defects from NHAI One: {len(nhai_defects)}")
    
    # Get the NHAI defect name for comparison
    nhai_defect_name = get_nhai_defect_name(defect.defect_type_id)
    print(f"Looking for defect type: {nhai_defect_name}")
    
    # Get the distance threshold for this defect type
    distance_threshold = DEFECT_TYPE_DISTANCE_THRESHOLDS.get(defect.defect_type_id, DEFAULT_DUPLICATE_DISTANCE_THRESHOLD)
    print(f"Using distance threshold: {distance_threshold} meters")
    
    matching_defects = []
    for nhai_defect in nhai_defects:
        print(f"\n=== Checking NHAI defect ===")
        print(f"Defect ID: {nhai_defect.get('data_id')}")
        print(f"Defect details:")
        print(f"- Location: lat={nhai_defect.get('latitude')}, long={nhai_defect.get('longitude')}")
        print(f"- Chainage: from_km={nhai_defect.get('from_km')}, from_m={nhai_defect.get('from_m')}, to_km={nhai_defect.get('to_km')}, to_m={nhai_defect.get('to_m')}")
        print(f"- UPC: {nhai_defect.get('upc')}")
        print(f"- Defect Name: {nhai_defect.get('defect_name')}")
        print(f"- Asset: {nhai_defect.get('asset')}")
        print(f"- Side: {nhai_defect.get('side')}")
        print(f"- Carriageway: {nhai_defect.get('type_carriageway')}")
        
        # Check location match (either chainage or GPS)
        location_match = False
        
        # Check chainage match
        try:
            if (nhai_defect.get('from_chainage_km') is not None and 
                nhai_defect.get('to_chainage_km') is not None):
                if (float(nhai_defect['from_chainage_km']) == float(defect.chainage_from) and
                    float(nhai_defect['to_chainage_km']) == float(defect.chainage_to)):
                    location_match = True
                    print("✓ Location match by chainage")
                else:
                    print("✗ Location mismatch by chainage")
                    print(f"  Expected: from_chainage_km={defect.chainage_from}, to_chainage_km={defect.chainage_to}")
                    print(f"  Got: from_chainage_km={nhai_defect['from_chainage_km']}, to_chainage_km={nhai_defect['to_chainage_km']}")
            else:
                print("✗ Chainage fields missing in cache data")
        except (ValueError, TypeError) as e:
            print(f"Error in chainage matching: {str(e)}")
            
        # Check GPS match using Haversine distance
        if not location_match:  # Only check GPS if chainage didn't match
            try:
                nhai_lat = float(nhai_defect.get("latitude", 0))
                nhai_long = float(nhai_defect.get("longitude", 0))
                distance = calculate_haversine_distance(defect.latitude, defect.longitude, nhai_lat, nhai_long)
                print(f"Distance between points: {distance:.2f} meters")
                if distance <= distance_threshold:
                    location_match = True
                    print(f"✓ Location match by GPS (distance: {distance:.2f}m <= threshold: {distance_threshold}m)")
                else:
                    print(f"✗ Location mismatch by GPS (distance: {distance:.2f}m > threshold: {distance_threshold}m)")
            except (ValueError, TypeError) as e:
                print(f"Error calculating GPS distance: {str(e)}")
            
        if location_match:
            print("\nLocation matched, checking other characteristics")
            # Rest of the existing matching logic remains the same
            try:
                # Convert asset type to match the format in the API response
                asset_type_match = False
                if defect.asset_type:
                    asset_type_name = AssetTypeID(defect.asset_type.value).name.replace("_", " ").title()
                    asset_type_match = nhai_defect.get("asset", "").lower() == asset_type_name.lower()
                    print(f"Asset type match: {asset_type_match}")
                    print(f"  Expected: {asset_type_name.lower()}")
                    print(f"  Got: {nhai_defect.get('asset', '').lower()}")
                else:
                    asset_type_match = True  # If no asset type specified, consider it a match
                    print("No asset type specified, considering it a match")

                # Check all characteristics
                upc_match = nhai_defect.get("upc") == defect.upc
                defect_name_match = nhai_defect.get("defect_name", "").lower() == nhai_defect_name.lower()
                
                print(f"\nRequired characteristics:")
                print(f"UPC match: {upc_match}")
                print(f"  Expected: {defect.upc}")
                print(f"  Got: {nhai_defect.get('upc')}")
                print(f"Defect name match: {defect_name_match}")
                print(f"  Expected: {nhai_defect_name.lower()}")
                print(f"  Got: {nhai_defect.get('defect_name', '').lower()}")

                if (upc_match and asset_type_match and defect_name_match):
                    # Check optional fields if provided
                    # Carriageway check removed since field is not available in NHAI One API

                    side_match = True  # Default to True
                    print(f"\nSide check:")
                    print(f"Input defect side: {defect.side}")
                    print(f"NHAI defect side: {nhai_defect.get('side')}")
                    print(f"Initial side_match: {side_match}")
                    
                    if defect.side is not None:
                        nhai_side = nhai_defect.get("side")
                        print(f"NHAI side value: {nhai_side}")
                        if nhai_side is not None:
                            # Convert string side to numeric if needed
                            if isinstance(nhai_side, str):
                                side_map = {"LHS": 0, "MEDIAN": 1, "RHS": 2, "BOTH": 3}
                                nhai_side = side_map.get(nhai_side.upper())
                                print(f"Converted side to: {nhai_side}")
                            side_match = nhai_side == defect.side
                            print(f"Final side_match: {side_match}")

                    print(f"\nOptional characteristics:")
                    print(f"Carriageway match: Skipped (field not available in NHAI One API)")
                    print(f"Side match: {side_match}")
                    print(f"  Expected: {defect.side}")
                    print(f"  Got: {nhai_defect.get('side')}")

                    if side_match:  # Only check side match, carriageway is ignored
                        print("\n✓ All characteristics matched!")
                        matching_defects.append(nhai_defect)
                        print("Added to matching defects")
                        print("Found a match, no need to check further")
                        break  # Exit the loop once we find a match
                    else:
                        print("\n✗ Optional fields didn't match")
                else:
                    print("\n✗ Required characteristics didn't match")
            except Exception as e:
                print(f"Error checking characteristics: {str(e)}")
        else:
            print("\n✗ Location didn't match, skipping other checks")
    
    print(f"\n=== Duplicate check complete ===")
    print(f"Found {len(matching_defects)} matching defects")
    return matching_defects

# Database Configuration (Update with your RDS credentials)
DATABASE_URL = "postgresql+psycopg2://postgres:8jmNbGYFAoeTPxN6rT2k@defects-db.cn84w2qi6gyi.ap-south-1.rds.amazonaws.com/defects-db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# RabbitMQ configuration
RABBITMQ_URL = "amqps://rabbitmq:rabbitmq%40123@b-5e5ac81a-0b5b-4521-90c6-12eb2a2d3193.mq.ap-south-1.amazonaws.com:5671"
QUEUE_NAME = "defects_queue"

# Authentication configuration
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
SECRET_KEY = "key@nhai"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_password_hash(password):
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[datetime.timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.utcnow() + expires_delta
    else:
        expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def publish_to_queue(queue_name, message):
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)  # Ensure queue is durable

    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
    )
    connection.close()


def load_defect_asset_master():
    defect_master_df = pd.read_pickle(settings.defect_master_pickle_path)
    asset_master_df = pd.read_pickle(settings.asset_master_pickle_path)

    defect_dict = defaultdict(list)
    for k, v in zip(defect_master_df['DEFECTTYPE'], defect_master_df['DEFECT_ID']):
        defect_dict[k].append(v)

    defect_dict_idToType = dict(zip(defect_master_df['DEFECT_ID'], defect_master_df['DEFECTTYPE']))
    asset_dict = dict(zip(asset_master_df['ASSET_ID'], asset_master_df['ASSET']))
    defect_to_asset_dict = dict(zip(defect_master_df['DEFECT_ID'], defect_master_df['ASSET_ID']))

    print(defect_to_asset_dict)
    return defect_dict, asset_dict, defect_dict_idToType, defect_to_asset_dict


def generate_access_token_v2(data: dict):
    # You can include more user-specific data in the payload if necessary
    to_encode = data.copy()
    to_encode.update({"exp": datetime.datetime.utcnow() + datetime.timedelta(hours=1)})  # Set expiration time
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return {"access_token": encoded_jwt, "token_type": "bearer"}


def verify_password_v2(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user_v2(username: str, password: str):
    db = SessionLocal()
    try:
        result = db.execute(
            text("SELECT * FROM users_test WHERE username = :username AND is_active = TRUE"),
            {"username": username}
        )
        user = result.fetchone()
        if user and verify_password_v2(password, user.hashed_password):
            return user
        return None
    finally:
        db.close()


def get_current_user_v2(token: str = Depends(oauth2_scheme)) -> dict[str, str]:
    try:
        # Decode the JWT token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")

        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Get user from database instead of users_db
        db = SessionLocal()
        try:
            result = db.execute(
                text("SELECT username, source FROM users_test WHERE username = :username AND is_active = TRUE"),
                {"username": username}
            )
            user = result.fetchone()
            if user:
                # Convert SQLAlchemy Row to dict
                return {"username": user.username, "source": user.source}
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )
        finally:
            db.close()
            
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_current_user_id_v4(token: str = Depends(oauth2_scheme)) -> int:
    try:
        # Decode the JWT token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")

        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Get user from database instead of users_db
        db = SessionLocal()
        try:
            result = db.execute(
                text("SELECT id FROM users_test WHERE username = :username AND is_active = TRUE"),
                {"username": username}
            )
            user = result.fetchone()
            if user:
                return user.id
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )
        finally:
            db.close()
            
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_user_code_v4(db, user_id):
    result = db.execute(
        text("SELECT user_code FROM users_test WHERE id = :user_id"),
        {"user_id": user_id}
    )
    user = result.fetchone()
    return user.user_code if user else "0000000005"  # Default to ATMS code


def convert_asset_type_v2(current_asset_name, asset_dict):
    for key, value in asset_dict.items():
        if value.lower() == current_asset_name:
            return key  # Return the new ID as a string
    return None  # If no match is found


def convert_defect_type_v2(current_defect_name, defect_dict_idToType):
    for key, value in defect_dict_idToType.items():
        if key.lower().replace("(combined)", "").strip() == current_defect_name:
            print("check", value)
            return value  # Return the new ID as a string
        elif key.lower().strip() == current_defect_name.lower().replace("_", " ").strip():
            print("check", value)
            return value  # Return the new ID as a string
    return defect_dict_idToType.get("Other", [])  # If no match is found, return "Other", in "Misc", return 74, in "Rigid Pavements", return 31


def combine_defect_types_v2(defects_dict):
    # Create a copy of the original dictionary
    defects_dict_copy = deepcopy(defects_dict)

    # Combine defect types mentioning 'cracks'
    combined_cracks_defect = "Crack (Combined)"
    combined_cracks_ids = []

    # Combine defect types mentioning 'pothole'
    combined_pothole_defect = "Pothole (Combined)"
    combined_pothole_ids = []

    # List of defects to remove after combining
    defects_to_remove = []

    for defect, ids in defects_dict_copy.items():
        # Check for 'crack' in defect name
        if "crack" in defect.lower():
            combined_cracks_ids.extend(ids)
            defects_to_remove.append(defect)

        # Check for 'pothole' in defect name
        if "pothole" in defect.lower():
            combined_pothole_ids.extend(ids)
            defects_to_remove.append(defect)

    # Remove the individual defects that were merged into combined defects
    for defect in defects_to_remove:
        del defects_dict_copy[defect]

    # Add combined entries for cracks and potholes
    combined_cracks_ids = sorted(set(combined_cracks_ids))  # Remove duplicates and sort
    combined_pothole_ids = sorted(set(combined_pothole_ids))  # Remove duplicates and sort

    # Add combined entries for cracks and potholes
    defects_dict_copy[combined_cracks_defect] = combined_cracks_ids
    defects_dict_copy[combined_pothole_defect] = combined_pothole_ids

    return defects_dict_copy


def select_defect_for_asset_v2(asset_id, selected_defect_ids, defect_to_asset_dict):
    """
    Select a defect ID for a given asset ID from the selected defect IDs.

    :param asset_id: The ID of the asset type (e.g., 1, 2, 3)
    :param selected_defect_ids: List of selected defect IDs
    :param defect_to_asset_dict: Dictionary mapping defect IDs to asset IDs

    :return: The first defect ID corresponding to the asset, or 74 (Other) if no match is found
    """
    # Filter defect IDs by matching the asset ID
    for defect_id in selected_defect_ids:
        if defect_to_asset_dict.get(defect_id) == str(asset_id):
            return defect_id  # Return the first matching defect ID
    return '74' #Other in 'Misc' asset  # If no match is found


def extract_base64_v2_test(photo_base64):
    if photo_base64 and (photo_base64.startswith("data:image/jpeg;base64,") or photo_base64.startswith("data:image/png;base64,")):
        # Extract everything after the comma
        base64_data = photo_base64.split(",")[1]
        return base64_data
    return None


async def save_defect_v4_test(defect, token: str = Depends(oauth2_scheme)):
    start_time = time.time()
    
    db = SessionLocal()
    try:
        print(f"DEBUG: Starting save_defect_v4 at {time.time() - start_time:.2f}s")
        
        # Check for duplicates in NHAI One system using slow method for debugging
        duplicate_start = time.time()
        duplicate_defects = await check_duplicate_defect(defect)
        print(f"DEBUG: Duplicate check completed in {time.time() - duplicate_start:.2f}s")
        
        # Save defect to database (always)
        db_start = time.time()
        current_user_id = get_current_user_id_v4(token)
        user_code = get_user_code_v4(db, current_user_id)
        print(f"DEBUG: User lookup completed in {time.time() - db_start:.2f}s")

        defect_name = DefectTypeID(defect.defect_type_id).name

        # Calculate chainage values
        from_km = int(defect.chainage_from)
        from_m = round((defect.chainage_from - from_km) * 1000)
        to_km = int(defect.chainage_to)
        to_m = round((defect.chainage_to - to_km) * 1000)

        # First check if record exists
        check_query = text("""
            SELECT id FROM defects_new_non_atms_test 
            WHERE source = :source AND external_source_id = :external_source_id
        """)
        existing_record = db.execute(check_query, {
            "source": defect.source,
            "external_source_id": defect.external_source_id
        }).fetchone()
        
        if existing_record:
            raise HTTPException(
                status_code=400,
                detail=f"A defect with source '{defect.source}' and external_source_id '{defect.external_source_id}' already exists. Updates are not allowed."
            )

        # Insert data in defects_new_non_atms_test
        insert_start = time.time()
        query = text("""
            INSERT INTO defects_new_non_atms_test (
                asset_type, upc, defect_name, defect_type_id, datetime_identification, side,
                lane, severity, type_carriageway, chainage_from_km, chainage_from_m, 
                chainage_to_km, chainage_to_m, latitude, longitude,
                remarks, source, external_source_id, created_by, updated_at)
            VALUES (
                :asset_type, :upc, :defect_name, :defect_type_id, :datetime_identification, :side,
                :lane, :severity, :type_carriageway, :chainage_from_km, :chainage_from_m,
                :chainage_to_km, :chainage_to_m, :latitude, :longitude,
                :remarks, :source, :external_source_id, :created_by, NULL)
            RETURNING id
        """)
        
        result = db.execute(query, {
            "asset_type": defect.asset_type.value if defect.asset_type else None,
            "upc": defect.upc,
            "defect_name": defect_name,
            "defect_type_id": defect.defect_type_id,
            "datetime_identification": defect.datetime_identification,
            "side": defect.side,
            "lane": defect.lane,
            "severity": defect.severity,
            "type_carriageway": defect.type_carriageway,
            "chainage_from_km": from_km,
            "chainage_from_m": from_m,
            "chainage_to_km": to_km,
            "chainage_to_m": to_m,
            "latitude": defect.latitude,
            "longitude": defect.longitude,
            "remarks": defect.remarks,
            "source": defect.source,
            "external_source_id": defect.external_source_id,
            "created_by": current_user_id
        })

        defect_id = result.fetchone()[0]
        print(f"DEBUG: Defect insert completed in {time.time() - insert_start:.2f}s")

        # Insert photos
        photo_start = time.time()
        for photo in defect.photos_list:
           photo_query = text("""
           INSERT INTO photos_new_non_atms_test (defect_id, photo_base64)
           VALUES (:defect_id, :photo_base64)
           """)
           db.execute(photo_query, {"defect_id": defect_id, "photo_base64": photo.photo_base64})
        print(f"DEBUG: Photo inserts completed in {time.time() - photo_start:.2f}s")

        # If duplicates found, record them
        if duplicate_defects:
            duplicate_record_start = time.time()
            for original_defect in duplicate_defects:
                duplicate_query = text("""
                    INSERT INTO defect_duplicates 
                    (original_defect_id, duplicate_defect_id, duplicate_source, 
                     duplicate_datetime, duplicate_reason)
                    VALUES 
                    (:original_id, :duplicate_id, :source, :datetime, :reason)
                """)
                db.execute(duplicate_query, {
                    "original_id": original_defect.get("data_id"),  # Use data_id instead of unique_defect_id
                    "duplicate_id": defect_id,
                    "source": defect.source,
                    "datetime": defect.datetime_identification,
                    "reason": "Same location and defect type"
                })
            
            db.commit()
            print(f"DEBUG: Duplicate recording completed in {time.time() - duplicate_record_start:.2f}s")
            print(f"DEBUG: Total time: {time.time() - start_time:.2f}s")
            return {
                "status": "duplicate",
                "message": "A similar defect has already been reported at this location"
            }

        # If not duplicate, prepare and publish to queue
        queue_start = time.time()
        defect_dict, asset_dict, defect_dict_idToType, defect_to_asset_dict = load_defect_asset_master()
        combined_defects_dict = combine_defect_types_v2(defect_dict)
        print(f"DEBUG: Asset/defect master loaded in {time.time() - queue_start:.2f}s")
        
        if defect.asset_type:
            asset_type_name = AssetTypeID(defect.asset_type.value).name.replace("_", " ").lower()
        else:
            asset_type_name = None

        if defect.defect_type_id == DefectTypeID.ILLEGAL_MEDIAN_OPENING:
            defect_id_nhai_one = "74"  # Other
            asset_id_nhai_one = "13"  # Misc
        else:
            asset_id_nhai_one = convert_asset_type_v2(asset_type_name, asset_dict)
            defect_id_nhai_one_list = convert_defect_type_v2(defect_name.lower(), combined_defects_dict)
            defect_id_nhai_one = None
            if defect_id_nhai_one_list:
                defect_id_nhai_one = select_defect_for_asset_v2(asset_id_nhai_one, defect_id_nhai_one_list, defect_to_asset_dict)

        lane_nhai_one = "Lane 1"  # default lane
        if defect.lane:
            lane_nhai_one = defect.lane

        side_nhai_one = "LHS"  # default side
        if defect.side is not None:
            if defect.side == 0:
                side_nhai_one = "LHS"
            elif defect.side == 1:
                side_nhai_one = "Median"
            elif defect.side == 2:
                side_nhai_one = "RHS"

        type_carriageway_nhai_one = "Main Carriageway"
        if defect.type_carriageway is not None:
            try:
                type_carriageway_nhai_one = TypeCarriageway(defect.type_carriageway).name.replace("_", " ").title()
            except ValueError:
                type_carriageway_nhai_one = "Main Carriageway"

        message = {
            "DEFECT_TYPE_ID": defect_id_nhai_one,
            "ASSET_TYPE": asset_id_nhai_one,
            "UPC": defect.upc,
            "DATETIME_IDENTIFICATION": defect.datetime_identification.isoformat(),
            "SOURCE": defect.source,
            "USER_NAME": user_code,
            "IMAGE": extract_base64_v2(defect.photos_list[0].photo_base64) if len(defect.photos_list) > 0 else None,
            "IMAGE_2": extract_base64_v2(defect.photos_list[1].photo_base64) if len(defect.photos_list) > 1 else None,
            "INSPECTION_TYPE": "Random",
            "LANE": lane_nhai_one,
            "REMARKS": defect_name.replace("_", " ").strip(), # using the defect name as remarks as defect.remarks contains "Unknown Defect" in case of Illegal Median Opening defects
            "SEVERITY": defect.severity,
            "SIDE": side_nhai_one,
            "TYPE_CARRIAGEWAY": type_carriageway_nhai_one,
            "FROM_KM": from_km,
            "FROM_M": from_m,
            "TO_KM": to_km,
            "TO_M": to_m,
            "ID": defect_id,
            "LAT": defect.latitude,
            "LONG": defect.longitude
        }

        # Publish to queue only if not a duplicate
        publish_start = time.time()
        publish_to_queue(QUEUE_NAME, message)
        print(f"DEBUG: Queue publish completed in {time.time() - publish_start:.2f}s")
        
        db.commit()
        print(f"DEBUG: Total time: {time.time() - start_time:.2f}s")
        
        return {"status": "success", "message": f"Defect with ID {defect.external_source_id} has been saved."}
        
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        db.close()


def check_duplicate_defect_fast(defect):
    """
    Fast duplicate detection using cached data only.
    Returns list of matching defects if found, empty list if no duplicates.
    """
    print(f"DEBUG: Starting fast duplicate detection for UPC: {defect.upc}")
    
    # Get cached defects for this UPC
    cached_defects = get_cached_nhai_defects(defect.upc)
    
    if not cached_defects:
        print("DEBUG: No cached data available, returning empty list")
        return []
    
    print(f"DEBUG: Processing {len(cached_defects)} cached defects for duplicate check")
    result = filter_matching_defects_fast(cached_defects, defect)
    print(f"DEBUG: Fast duplicate detection found {len(result)} matches")
    return result


def filter_matching_defects_fast(nhai_defects, defect):
    """
    Fast version of filter_matching_defects with minimal logging and early exit.
    Updated to use actual cache field names.
    """
    # Accept both dict-with-data and list
    if isinstance(nhai_defects, dict) and "data" in nhai_defects:
        nhai_defects = nhai_defects["data"]
    elif isinstance(nhai_defects, list):
        pass  # Already a list, do nothing
    else:
        return []
    
    # Get the NHAI defect name for comparison
    nhai_defect_name = get_nhai_defect_name(defect.defect_type_id)
    
    # Get the distance threshold for this defect type
    distance_threshold = DEFECT_TYPE_DISTANCE_THRESHOLDS.get(defect.defect_type_id, DEFAULT_DUPLICATE_DISTANCE_THRESHOLD)
    
    matching_defects = []
    for nhai_defect in nhai_defects:
        # Check location match (either chainage or GPS)
        location_match = False
        
        # Check chainage match using actual cache fields
        try:
            if (nhai_defect.get('from_chainage_km') is not None and 
                nhai_defect.get('to_chainage_km') is not None):
                if (float(nhai_defect['from_chainage_km']) == float(defect.chainage_from) and
                    float(nhai_defect['to_chainage_km']) == float(defect.chainage_to)):
                    location_match = True
        except (ValueError, TypeError):
            pass
            
        # Check GPS match using Haversine distance if chainage didn't match
        if not location_match:
            try:
                nhai_lat = float(nhai_defect.get("latitude", 0))
                nhai_long = float(nhai_defect.get("longitude", 0))
                distance = calculate_haversine_distance(defect.latitude, defect.longitude, nhai_lat, nhai_long)
                if distance <= distance_threshold:
                    location_match = True
            except (ValueError, TypeError):
                pass
            
        if location_match:
            # Check required characteristics
            upc_match = nhai_defect.get("upc") == defect.upc
            defect_name_match = nhai_defect.get("defect_name", "").lower() == nhai_defect_name.lower()
            
            # Check asset type match
            asset_type_match = True
            if defect.asset_type:
                asset_type_name = AssetTypeID(defect.asset_type.value).name.replace("_", " ").title()
                asset_type_match = nhai_defect.get("asset", "").lower() == asset_type_name.lower()

            if (upc_match and asset_type_match and defect_name_match):
                # Check optional fields if provided
                # Carriageway check removed since field is not available in NHAI One API

                side_match = True  # Default to True
                print(f"\nSide check:")
                print(f"Input defect side: {defect.side}")
                print(f"NHAI defect side: {nhai_defect.get('side')}")
                print(f"Initial side_match: {side_match}")
                
                if defect.side is not None:
                    nhai_side = nhai_defect.get("side")
                    if nhai_side is not None:
                        if isinstance(nhai_side, str):
                            side_map = {"LHS": 0, "MEDIAN": 1, "RHS": 2, "BOTH": 3}
                            nhai_side = side_map.get(nhai_side.upper())
                        side_match = nhai_side == defect.side
                        print(f"Final side_match: {side_match}")

                print(f"\nOptional characteristics:")
                print(f"Carriageway match: Skipped (field not available in NHAI One API)")
                print(f"Side match: {side_match}")
                print(f"  Expected: {defect.side}")
                print(f"  Got: {nhai_defect.get('side')}")

                if side_match:  # Only check side match, carriageway is ignored
                    matching_defects.append(nhai_defect)
                    # Early exit - we found a match, no need to check further
                    break
    
    return matching_defects

