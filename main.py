# Importing Necessary modules
from fastapi import FastAPI, Depends, Query, HTTPException, status, Request, Form
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from .auth import verify_credentials, create_access_token, get_current_user_from_token
import uvicorn
import json
import requests
import base64
import gzip
from pydantic import create_model, BaseModel
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import math
from app.v1.endpoints.lat_long_to_upc_api import *
from app.v1.endpoints.utility_shifting_api import *
from app.v1.endpoints.toilet_model_api import *
from app.v1.endpoints.defect_others_model import *
from app.v1.endpoints.mobineers_toilet_apis_for_powerbi import *
from app.v1.endpoints.eoffice_api import *
from app.v1.endpoints.la_api import *
from app.v1.endpoints.access_permissions_api import *
from app.v1.endpoints.canara_bank_api import *
from app.v1.endpoints.basic_data_details_api import *
from app.v1.endpoints.ams_geofencing_api import *
from app.v1.endpoints.construction_achievement_api import *
from app.v1.endpoints.middle_layer_api import *
from app.v1.endpoints.middle_layer_api_v2 import *
from app.v1.endpoints.middle_layer_api_v3 import *
from app.v1.endpoints.middle_layer_api_v4 import *
from app.v1.endpoints.middle_layer_api_v4_test import *
from app.v1.endpoints.middle_layer_api_v5 import *
from app.v1.endpoints.toll_revenue_api import *
from app.v1.endpoints.toilet_api_rmy import *
from app.v1.endpoints.wayside_amenities import *
from app.v1.endpoints.road_closures_api_pkl import *
from app.v1.endpoints.mock_vahan_api import *
import logging
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select
from typing import List, Optional
import asyncio
from uuid import uuid4


logging.basicConfig(level=logging.DEBUG)

# Declaring our FastAPI instance
app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)

templates = Jinja2Templates(directory="/home/ec2-user/APIs_Hosting/app/v1/templates")

gdf = read_shapefile()
district_gdf = read_district_shapefile()


@app.get('/lat_long_to_upc')
def lat_long_to_upc(latitude: float = Query(..., description="Latitude of the point"), longitude: float = Query(..., description="Longitude of the point"), complaint_type: str = Query(..., description="Type of the complaint")):
	point = Point(longitude, latitude)
	point_transformed = gpd.GeoSeries([point], crs='EPSG:4326').to_crs(epsg=24378).iloc[0]
	logging.debug("Point transformed successfully")

	merged_df = merge_shapefile_project_info_df(gdf)
	merged_df['distance'] = merged_df.geometry.distance(point_transformed)
	logging.debug("Shapefile merged successfully")


	df_within_radius = get_results_within_radius(merged_df)
	logging.debug("results within 250m")


	if df_within_radius.empty: # assuming that this lat, long is going to link to a MoRTH UPC, we would have to return the PIU, District, State in this case
		logging.debug("df_within_radius empty")
		print("df within radius empty")
		district_gdf['distance'] = district_gdf.geometry.distance(point_transformed)
		min_distance = district_gdf['distance'].min()
		close_gdf = district_gdf[(district_gdf['distance'] == min_distance)]
		upc_list = return_list_of_list(close_gdf)
		return JSONResponse(status_code=200, content={"data": upc_list})


	else:
		logging.debug(complaint_type)
		if complaint_type == "UNAUTHORISED OCCUPATION": #Encroachment - get any UPC, get PD
			all_upc_list = return_list_of_list(df_within_radius)
			return JSONResponse(status_code=200, content={"data": all_upc_list})
		elif (complaint_type == "SAFETY HAZARDS") or (complaint_type == "POOR WORKMANSHIP ON CONSTRUCTION"): #Safety Hazards & Poor Workmanship on construction - get latest LOA UPC, get AE
			latest_loa_upc_df = get_min_dist_latest_loa_upc(df_within_radius) #get_latest_loa_upc(df_within_radius)
			latest_loa_upc_list = return_list_of_list(latest_loa_upc_df)
			return JSONResponse(status_code=200, content={"data": latest_loa_upc_list})
		elif (complaint_type == "POTHOLES AND OTHER MAINTENANCE ISSUES"):
			latest_loa_operation_maintenance_upc_df = get_min_dist_latest_loa_operation_maintenance(df_within_radius) #get_latest_loa_operation_maintenance(df_within_radius)
			latest_loa_operation_maintenance_upc_list = return_list_of_list(latest_loa_operation_maintenance_upc_df)
			return JSONResponse(status_code=200, content={"data": latest_loa_operation_maintenance_upc_list})
	return {"status": "Invalid complaint type"}




@app.get('/lat_long_to_upc_test')
def lat_long_to_upc_test(latitude: float = Query(..., description="Latitude of the point"), longitude: float = Query(..., description="Longitude of the point")):

        point = Point(longitude, latitude)
        point_transformed = gpd.GeoSeries([point], crs='EPSG:4326').to_crs(epsg=24378).iloc[0]

        merged_df = merge_shapefile_project_info_df(gdf)
        merged_df['distance'] = merged_df.geometry.distance(point_transformed)

        df_within_radius = get_results_within_radius(merged_df)


        if df_within_radius.empty:
                print("df within radius empty")
                return JSONResponse(status_code=404, content={"message": "Not Found-no close points after filtering"})
        else:
                latest_loa_operation_maintenance_upc_df = get_min_dist_latest_loa_operation_maintenance(df_within_radius)
                latest_loa_operation_maintenance_upc_list = []
                try:
                    latest_loa_operation_maintenance_upc_list = return_list_of_list(latest_loa_operation_maintenance_upc_df)
                except Exception as e:
                    logging.debug(e)
                return JSONResponse(status_code=200, content={"data": latest_loa_operation_maintenance_upc_list})

        #return {"status": "lat long testing"}


@app.get('/utilityshifting')
def utilityshifting():
    utility_shifting_df = read_utility_shifting_pickle()
    return JSONResponse(status_code=200, content={"data": utility_shifting_df.to_dict(orient='records')})



cfg, predictor = get_cfg_predictor()

@app.post("/toilet_model_api")
def toilet_model(data:data):
    print("calling toilet_model_predict at ", datetime.now())
    res_dict = toilet_model_predict(data, predictor)
    print("after call ", datetime.now())
    return JSONResponse(status_code=200, content=res_dict, media_type="application/json")



model = load_model()
defect_dict, asset_dict, defect_dict_idToType, defect_to_asset_dict = load_defect_asset_master()

@app.post("/remark_to_defectType")
def remark_to_defect_type(defectSummary:defectSummary):
    res_dict = defect_others_model_predict(defectSummary, model, defect_dict, asset_dict, defect_dict_idToType, defect_to_asset_dict)
    return JSONResponse(status_code=200, content={"data": res_dict}, media_type="application/json")



@app.get('/dailyInspection')
def daily_inspection(
    start_date: str = Depends(lambda: (date.today() - timedelta(days=31)).isoformat()),
    end_date: str = Depends(lambda: date.today().isoformat())
):
    headers, data = define_headers_daily_inspection(start_date, end_date)
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/cleanUnclean')
def cleanUnclean(start_date: str = Depends(lambda: (date.today() - timedelta(days=31)).isoformat())):
    headers, data = define_headers_clean_unclean(start_date)
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/supervisorRegistration')
def supervisorRegistration():
    headers, data = define_headers_supervisor_registration()
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/tpmRegistration')
def tpmRegistration():
    headers, data = define_headers_tpm_registration()
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/toiletRegistration')
def toiletRegistration():
    headers, data = define_headers_toilet_registration()
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/plazaMaster')
def plazaMaster():
    headers, data = define_headers_plaza_master()
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/piuMaster')
def piuMaster():
    headers, data = define_headers_piu_master()
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/roMaster')
def roMaster():
    headers, data = define_headers_ro_master()
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/stateMaster')
def stateMaster():
    headers, data = define_headers_state_master()
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/damage')
def Damage():
    headers, data = define_headers_damage()
    headers, content = call_api(headers, data)

    return JSONResponse(status_code=200, content=content, headers=headers)


@app.get('/eoffice_all')
def eoffice_api_all():
    complete_list = e_office_api()
    json_data = JSONResponse(content={"data": complete_list}, media_type="application/json")
    return json_data


@app.get('/la_api')
def la_api():
    la_df = read_la_pickle()
    return JSONResponse(status_code=200, content={"data": la_df.to_dict(orient='records')})


@app.get('/access_permissions_api')
def access_permissions_api():
    access_permissions_df = read_access_permissions_pickle()
    return JSONResponse(status_code=200, content={"data": access_permissions_df.to_dict(orient='records')})


@app.get('/utility_shifting_api')
def utility_shifting_api():
    df = read_utility_shifting_pickle_v2()
    return JSONResponse(status_code=200, content={"data": df.to_dict(orient='records')})


@app.get('/basic_data_details_api')
def basic_data_details_api():
    df = read_basic_data_details_pickle()
    return JSONResponse(status_code=200, content={"data": df.to_dict(orient='records')})


@app.get('/construction_achievement_api')
def construction_achievement_api():
    df = read_construction_achievement_pickle()
    return JSONResponse(status_code=200, content={"data": df.to_dict(orient='records')})


def format_date(date: datetime) -> str:
    #Format a datetime object to DD-MMM-YYYY format.
    return date.strftime("%d-%b-%Y").upper()


@app.get("/canara_bank_api/")
def canara_bank_api(
    fromdt: str = Query(..., description="Start date in DD-MMM-YYYY format"),
    todt: str = Query(default=None, description="End date in DD-MMM-YYYY format")
):
    print("to date", todt)
    if todt is None:
        # Set the default to date dynamically
        todt = format_date(date.today() + timedelta(days=1))
    print("to date", todt)

    headers, payload = define_headers_canara_api(fromdt, todt)
    df = call_api_canara(headers, payload)

    return JSONResponse(status_code=200, content={"data": df.to_dict(orient='records')})


@app.get("/canara_bank_summary/")
def canara_bank_api(
    fromdt: str = Query(default="10-OCT-2022", description="Start date in DD-MMM-YYYY format"),
    todt: str = Query(default=None, description="End date in DD-MMM-YYYY format")
):
    print("to date", todt)
    if todt is None:
        # Set the default to date dynamically
        todt = format_date(date.today() + timedelta(days=1))
    print("to date", todt)

    headers, payload = define_headers_canara_api(fromdt, todt)
    df = call_api_canara(headers, payload)
    df_summary = create_summary_canara_bank(df)

    return JSONResponse(status_code=200, content={"data": df_summary.to_dict(orient='records')})


@app.get("/canara_bank_summary_filtered/")
def canara_bank_summary(
    fromdt: str = Query(default="10-OCT-2022", description="Start date in DD-MMM-YYYY format"),
    todt: str = Query(default=None, description="End date in DD-MMM-YYYY format")
):
    print("to date", todt)
    if todt is None:
        # Set the default to date dynamically
        todt = format_date(date.today() + timedelta(days=1))
    print("to date", todt)

    headers, payload = define_headers_canara_api(fromdt, todt)
    df = call_api_canara(headers, payload)
    df_summary = create_canara_bank_summary(df)

    try:
        json_output = json.dumps(df_summary.to_dict(orient='records'))  # Try converting
        # print(json_output)  # Check if it's valid JSON
        return JSONResponse(status_code=200, content={"data": df_summary.to_dict(orient='records')})
    except Exception as e:
        print("JSON conversion error:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/canara_bank_summary_analysis/")
def canara_bank_summary_analysis(
    fromdt: str = Query(default="10-OCT-2022", description="Start date in DD-MMM-YYYY format"),
    todt: str = Query(default=None, description="End date in DD-MMM-YYYY format")
):
    print("to date", todt)
    if todt is None:
        # Set the default to date dynamically
        todt = format_date(date.today() + timedelta(days=1))
    print("to date", todt)

    headers, payload = define_headers_canara_api(fromdt, todt)
    df = call_api_canara(headers, payload)
    df_summary_analysis = create_canara_bank_summary_analysis(df)

    try:
        json_output = json.dumps(df_summary_analysis.to_dict(orient='records'))  # Try converting
        # print(json_output)  # Check if it's valid JSON
        return JSONResponse(status_code=200, content={"data": df_summary_analysis.to_dict(orient='records')})
    except Exception as e:
        print("JSON conversion error:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})



@app.get('/lat_long_within_upc_alignment')
def lat_long_within_upc(latitude: float = Query(..., description="Latitude of the point"), longitude: float = Query(..., description="Longitude of the point"), upc: str = Query(..., description="Project code")):
        point = Point(longitude, latitude)
        point_transformed = gpd.GeoSeries([point], crs='EPSG:4326').to_crs(epsg=24378).iloc[0]
        logging.debug(latitude)
        logging.debug(longitude)
        logging.debug(upc)
        logging.debug("Point transformed successfully")

        project_alignment = gdf[gdf["UPC"] == upc].copy()
        if project_alignment.empty:
            logging.debug("The project alignment is not available.")
            return JSONResponse(status_code=200, content={"data": [{"result": -1, "distance": 0}]})
#        project_alignment_transformed = project_alignment.to_crs(epsg=24378)

        # Set a threshold distance (in degrees for lat/long, adjust based on CRS)
        threshold_distance = 150 # 150 meters

        # Check the minimum distance between the point and the MultiLineString geometries
        min_distance = project_alignment.geometry.distance(point_transformed).min()
        logging.debug(min_distance)

        if min_distance <= threshold_distance:
           logging.debug(f"The point is within {threshold_distance} meters of the alignment.")
           return JSONResponse(status_code=200, content={"data": [{"result": 1, "distance": min_distance}]})
        else:
           logging.debug("The point is not near the alignment.")
           return JSONResponse(status_code=200, content={"data": [{"result": 0, "distance": min_distance}]})


"""
@app.post("/api/middlelayer/token_old_v1")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    
    #DEPRECATED: Use /api/middlelayer/token instead
    #This endpoint will be removed in a future version
    
    # Authenticate user based on form data
    user = authenticate_user(form_data.username, form_data.password)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Generate and return the JWT token
    return generate_access_token(data={"sub": form_data.username})
"""

@app.post("/api/middlelayer/token")
async def login_for_access_token_v2(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user_v2(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # Pass all needed user data to generate_access_token
    access_token = generate_access_token_v2({
        "sub": user.username,
        "user_id": user.id,
        "source": user.source
    })
    return access_token


@app.post("/api/middlelayer/token_test")
async def login_for_access_token_v2_test(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user_v2_test(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # Pass all needed user data to generate_access_token
    access_token = generate_access_token_v2_test({
        "sub": user.username,
        "user_id": user.id,
        "source": user.source
    })
    return access_token
"""
@app.post('/api/middlelayer/defect_old_v1')
async def middle_layer_single_defect(defect: Defect, current_user: str = Depends(get_current_user)):
    #DEPRECATED: Use /api/middlelayer/defect instead
    #This endpoint will be removed in a future version
    try:
        source = current_user.get("source", "unknown")
        defect.source = source
        result = save_defect(defect)
        return result
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
"""    

@app.post('/api/middlelayer/defect_old_v2')
async def middle_layer_single_defect_v2(defect: Defect, current_user: str = Depends(get_current_user_v2), token: str = Depends(oauth2_scheme)):
    try:
        source = current_user.get("source", "unknown")
        defect.source = source
        result = save_defect_v2(defect, token)
        return result
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")




@app.post("/api/middlelayer/defect_v3")
async def middle_layer_single_defect_v3(
    defect: Defect,
    current_user: str = Depends(get_current_user_v2),
    token: str = Depends(oauth2_scheme)
):
    try:
        source = current_user.get("source", "unknown")
        defect.source = source
        result = await save_defect_v3(defect, token)
        return result
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


@app.post("/api/middlelayer/defect")
async def middle_layer_single_defect_v4(
    defect: Defect,
    current_user: str = Depends(get_current_user_v2),
    token: str = Depends(oauth2_scheme)
):
    try:
        source = current_user.get("source", "unknown")
        defect.source = source
        result = await save_defect_v4(defect, token)
        return result
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


@app.post("/api/middlelayer/defect_v5")
async def middle_layer_single_defect_v5(
    defect: Defect,
    current_user: str = Depends(get_current_user_v2),
    token: str = Depends(oauth2_scheme)
):
    try:
        source = current_user.get("source", "unknown")
        defect.source = source
        result = await save_defect_v5(defect, token)
        return result
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")



@app.post("/api/middlelayer/defect_v4_test")
async def middle_layer_single_defect_v4_test(
    defect: Defect,
    current_user: str = Depends(get_current_user_v2), 
    token: str = Depends(oauth2_scheme)
):
    try:
        source = current_user.get("source", "unknown")
        defect.source = source
        result = await save_defect_v4_test(defect, token)
        return result
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


# GET DEFECTS
# Database URL (replace with your actual credentials)
DATABASE_URL = "postgresql+asyncpg://postgres:8jmNbGYFAoeTPxN6rT2k@defects-db.cn84w2qi6gyi.ap-south-1.rds.amazonaws.com/defects-db"

# Create async engine and session
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

# Create the base model
Base = declarative_base()

# Define the Defect model (corresponding to the "defects" table)
class Defect(Base):
    __tablename__ = 'defects'
    id = Column(Integer, primary_key=True, index=True)
    asset_type = Column(Integer)
    upc = Column(String(50), nullable=False)
    defect_name = Column(String(100), nullable=False)
    defect_type_id = Column(Integer, nullable=False)
    datetime_identification = Column(DateTime, nullable=False)
    side = Column(Integer, nullable=False)
    lane = Column(String(10))
    severity = Column(String(20))
    type_carriageway = Column(Integer, nullable=False)
    chainage_from = Column(Numeric(10, 3))
    chainage_to = Column(Numeric(10, 3))
    latitude = Column(Numeric(9, 6))
    longitude = Column(Numeric(9, 6))
    remarks = Column(Text)
    source = Column(String(50), nullable=False)
    external_source_id = Column(String(100))

# Dependency to get the database session
async def get_db():
    async with SessionLocal() as session:
        yield session

# Pydantic model for the response format
class DefectBase(BaseModel):
    id: int
    asset_type: int
    upc: str
    defect_name: str
    defect_type_id: int
    datetime_identification: str
    side: int
    lane: Optional[str]
    severity: Optional[str]
    type_carriageway: int
    chainage_from: float
    chainage_to: float
    latitude: float
    longitude: float
    remarks: Optional[str]
    source: str
    external_source_id: str

    class Config:
        orm_mode = True


# Endpoint to fetch all defects
@app.get("/get_defects", response_model=List[DefectBase])
async def get_defects(db: AsyncSession = Depends(get_db)):
    try:
        # Perform the SELECT query
        result = await db.execute(select(Defect))
        defects = result.scalars().all()
        # Convert datetime_identification to string
        defects_response = [
            {
                **defect.__dict__,
                "datetime_identification": defect.datetime_identification.isoformat(),
            }
            for defect in defects
        ]
        return defects_response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching defects: {str(e)}")
    

@app.get('/toll_revenue_api')
def toll_revenue_api():
    df = get_toll_revenue_api()
    # print(df.head())
    try:
        json_output = json.dumps(df.to_dict(orient='records'))  # Try converting
        # print(json_output)  # Check if it's valid JSON
        return JSONResponse(status_code=200, content={"data": df.to_dict(orient='records')})
    except Exception as e:
        print("JSON conversion error:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})



@app.get('/toll_revenue_api_summary')
def toll_revenue_api_summary():
    try:
        data = get_toll_revenue_api_summary()
        return JSONResponse(status_code=200, content={"data": data})
    except Exception as e:
        print("JSON conversion error:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})



@app.get('/toll_revenue_api_summary_all')
def toll_revenue_api_summary_all():
    df = get_toll_revenue_api_summary_all()
    # print(df.head())  # Comment out or remove this line
    try:
        return JSONResponse(status_code=200, content={"data": df})
    except Exception as e:
        print("JSON conversion error:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})
    

@app.get('/pc/toilet_clean_unclean_status')
def toilet_status_api(request: Request):
    try:
        validate_token(request)
        headers, data = define_headers_rmy_api()
        print(headers)
        headers, content = call_api_rmy(headers, data)
        return JSONResponse(status_code=200, content=content, headers=headers)
    except HTTPException as e:
        if e.status_code == 401:
            return JSONResponse(
                status_code=401,
                content={
                    "error": "Unauthorized",
                    "errorDescription": "Missing or malformed Authorization header."
                }
            )
        elif e.status_code == 403:
            return JSONResponse(
                status_code=403,
                content={
                    "error": "Forbidden",
                    "errorDescription": "Invalid token provided in the Authorization header."
                }
            )
        elif e.status_code == 502:
            return JSONResponse(
                status_code=502,
                content={
                    "error": "Bad Gateway",
                    "errorDescription": "The downstream service did not return valid JSON."
                }
            )
        else:
            raise e


@app.get('/wsa/wayside_amenities_status')
def wsa_status_api(request: Request):
    """
    Get the status of wayside amenities.
    
    Returns:
        JSONResponse: List of wayside amenities with their status
    """
    try:
        validate_token_wsa(request)
        headers = define_headers_wsa_api()
        print(headers)
        headers, content = call_api_wsa(headers)
        return JSONResponse(status_code=200, content=content, headers=headers)
    except HTTPException as e:
        if e.status_code == 401:
            return JSONResponse(
                status_code=401,
                content={
                    "error": "Unauthorized",
                    "errorDescription": "Missing or malformed Authorization header."
                }
            )
        elif e.status_code == 403:
            return JSONResponse(
                status_code=403,
                content={
                    "error": "Forbidden",
                    "errorDescription": "Invalid token provided in the Authorization header."
                }
            )
        elif e.status_code == 502:
            return JSONResponse(
                status_code=502,
                content={
                    "error": "Bad Gateway",
                    "errorDescription": "The downstream service did not return valid JSON."
                }
            )
        else:
            raise e


@app.get('/rc/road_closures_status')
def road_closures_status_api(request: Request):
    """
    Get the status of road closures from cached pickle file.
    
    Returns:
        JSONResponse: List of road closures with their status
    """
    try:
        validate_token_road_closures(request)
        road_closures_data = read_road_closures_data()
        
        return JSONResponse(
            status_code=200,
            content={"data": road_closures_data},
            headers={"Accept-Encoding": "gzip"}
        )
    except HTTPException as e:
        if e.status_code == 401:
            return JSONResponse(
                status_code=401,
                content={
                    "error": "Unauthorized",
                    "errorDescription": "Missing or malformed Authorization header."
                }
            )
        elif e.status_code == 403:
            return JSONResponse(
                status_code=403,
                content={
                    "error": "Forbidden",
                    "errorDescription": "Invalid token provided in the Authorization header."
                }
            )
        elif e.status_code == 502:
            return JSONResponse(
                status_code=502,
                content={
                    "error": "Bad Gateway",
                    "errorDescription": "Failed to retrieve road closures data."
                }
            )
        else:
            raise e


"""
@app.get('/rc/road_closures_status')
def road_closures_status_api(request: Request):
    try:
        validate_token_road_closures(request)
        headers, data = define_headers_road_closures_api()
        print(headers)
        headers, content = call_api_road_closures(headers, data)
        return JSONResponse(status_code=200, content=content, headers=headers)
    except HTTPException as e:
        if e.status_code == 401:
            return JSONResponse(
                status_code=401,
                content={
                    "error": "Unauthorized",
                    "errorDescription": "Missing or malformed Authorization header."
                }
            )
        elif e.status_code == 403:
            return JSONResponse(
                status_code=403,
                content={
                    "error": "Forbidden",
                    "errorDescription": "Invalid token provided in the Authorization header."
                }
            )
        elif e.status_code == 502:
            return JSONResponse(
                status_code=502,
                content={
                    "error": "Bad Gateway",
                    "errorDescription": "The downstream service did not return valid JSON."
                }
            )
        else:
            raise e
"""

# Temporary link store
temp_links = {}

# Example permanent Power BI link
REAL_LINK = "https://app.powerbi.com/view?r=eyJrIjoiMDZkZmQ5NDktNzAyOC00MjBlLWJlNzMtY2RkOTU1MGI1OGIwIiwidCI6ImFlOWYzZDliLTc4YWItNGZjMS1hM2Q0LWIxYzNiYzY0NjBmMSJ9"

@app.post("/generate/")
def generate_temp_link():
    try:
        print("in /generate")
        print(REAL_LINK)
        temp_id = str(uuid4())
        print(temp_id)
        temp_links[temp_id] = {
            "url": REAL_LINK,
            "expires_at": datetime.utcnow() + timedelta(hours=24)  # 24 hours expiry
        }
        print(temp_links)
        return JSONResponse(content={"temp_link": f"/temp/{temp_id}"}, status_code=200)
    except Exception as e:
        logging.error("Error", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/temp/{temp_id}")
def redirect_temp(temp_id: str):
    link = temp_links.get(temp_id)
    if not link:
        raise HTTPException(status_code=404, detail="Link not found")

    if datetime.utcnow() > link["expires_at"]:
        del temp_links[temp_id]
        raise HTTPException(status_code=410, detail="Link expired")

    return RedirectResponse(url=link["url"])



"""
@app.get("/atms", response_class=HTMLResponse)
def read_atms(request: Request):
    # Login page
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/atms", response_class=HTMLResponse)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    # Login endpoint to authenticate users.
    if not verify_credentials(form_data.username, form_data.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    token = create_access_token(data={"sub": form_data.username})
   # return {"access_token": token, "token_type": "bearer"}
    response = RedirectResponse(url="/atms/dashboard", status_code=302)
    response.set_cookie(key="access_token", value=token, httponly=True, samesite="Lax", secure=False)
    return response


@app.get("/atms/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    # Dashboard after successful login.
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    username = payload.get("sub")
    return templates.TemplateResponse("dashboard.html", {"request": request, "username": username})
    #return templates.TemplateResponse("index.html", {"request": request})

@app.get("/atms/submit-defect", response_class=HTMLResponse)
def submit_defect(request: Request):
    try:
        current_user = get_current_user_from_token(request)
        logging.info(f"Current user: {current_user}")
    except HTTPException:
        logging.error("User not authenticated. Redirecting to login.")
        return RedirectResponse(url="/atms", status_code=302)
    return templates.TemplateResponse("submit_defect.html", {"request": request, "message": None})


@app.post("/atms/submit-defect")
def handle_submit_defect(
    request: Request,
    defect_name: str = Form(...), 
    defect_description: str = Form(...),
    side: str = Form(...),
    lane: str = Form(...),
    carriageway: str = Form(...),
    current_user: str = Depends(get_current_user_from_token)
):
    # Save the defect details in your database or process as needed
    print(f"Defect Submitted: {defect_name}, {defect_description}")
    message = "Defect submitted successfully"
    return templates.TemplateResponse("submit_defect.html", {"request": request, "message": message})
    #return {"message": "Defect submitted successfully", "defect_name": defect_name, "defect_description": defect_description}
"""

@app.post("/vehicle-info", 
    summary="Get vehicle information",
    description="Retrieve vehicle information using Vehicle Registration Number (VRN)")
async def vehicle_info_endpoint(request: VehicleRequest):
    """
    Get vehicle information by Vehicle Registration Number (VRN)
    
    - **vrn**: Vehicle Registration Number to look up
    """
    return get_vehicle_info(request)

