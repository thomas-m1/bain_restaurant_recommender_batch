import requests
import os
import time
from math import radians, cos, sin, asin, sqrt
from sqlalchemy import create_engine, Column, String, Float, Integer, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.mutable import MutableList
from dotenv import load_dotenv
import logging
from datetime import datetime
import json

# Setup logging directory and file
os.makedirs("logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = f"logs/batch_run_{timestamp}.log"

logging.basicConfig(
    filename=log_path,
    filemode="w",
    format="%(asctime)s — %(levelname)s — %(message)s",
    level=logging.INFO,
)

# Load environment variables
load_dotenv()
API_KEY = os.getenv("YELP_API_KEY")
DB_URL = os.getenv("DATABASE_URL")
print("Using DB:", DB_URL)
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Bain Toronto coordinates
BAIN_LAT = 43.670116
BAIN_LON = -79.385757

# SQLAlchemy setup
Base = declarative_base()

# Define Business model for SQLAlchemy
class Business(Base):
    __tablename__ = "businesses"

    id = Column(String, primary_key=True)
    name = Column(String)
    categories = Column(JSON)
    price = Column(String)
    rating = Column(Float)
    review_count = Column(Integer)
    address = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    distance_from_office_km = Column(Float)
    phone = Column(String)
    image_url = Column(String)
    url = Column(String)
    is_closed = Column(Boolean)
    scenario_tags = Column(MutableList.as_mutable(JSON))

    # Premium fields
    website = Column(String, nullable=True)
    accepts_credit_cards = Column(Boolean, nullable=True)
    alcohol = Column(String, nullable=True)
    ambience = Column(JSON, nullable=True)
    good_for_meal = Column(JSON, nullable=True)
    noise_level = Column(String, nullable=True)
    attire = Column(String, nullable=True)
    good_for_groups = Column(Boolean, nullable=True)
    outdoor_seating = Column(Boolean, nullable=True)
    business_hours = Column(JSON, nullable=True)

# Connect to the database and create table if dont exist
engine = create_engine(DB_URL, echo=True)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Haversine distance calculation
def calculate_distance_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return round(c * r, 2)

# # Function to search yelp api using search term
def search_yelp(term, location="Toronto, ON", limit=50, offset=0):
    url = "https://api.yelp.com/v3/businesses/search"
    params = {
        "term": term,
        "location": location,
        "limit": limit,
        "offset": offset,
    }
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()["businesses"]

# Upsert business (insert and update)
def upsert_business(data, tag, stats):
    try:
        rating = data.get("rating", 0)
        review_count = data.get("review_count", 0)

        # Skip businesses with low rating or low review count
        if rating < 3.8:
            stats["skipped"]["low_rating"] += 1
            return
        if review_count < 10:
            stats["skipped"]["low_reviews"] += 1
            return

        # Skip permanently or temporarily closed businesses
        if data.get("is_closed", False):
            stats["skipped"]["closed"] = stats["skipped"].get("closed", 0) + 1
            return
        attrs = data.get("attributes") or {}
        if attrs.get("business_temp_closed"):
            stats["skipped"]["temp_closed"] = stats["skipped"].get("temp_closed", 0) + 1
            return

        # Get coordinates and calculate distance
        coords = data.get("coordinates") or {}
        lat = coords.get("latitude")
        lon = coords.get("longitude")
        if lat is None or lon is None:
            stats["skipped"]["missing_coordinates"] = stats["skipped"].get("missing_coordinates", 0) + 1
            return
        distance = calculate_distance_km(BAIN_LAT, BAIN_LON, lat, lon)

        # Check if the business already exists
        business = session.query(Business).get(data["id"])
        categories = [c.get("title") for c in data.get("categories", []) if c.get("title")]
        location = data.get("location") or {}

        # Premium fields
        website = attrs.get("business_url")
        accepts_credit_cards = (attrs.get("accepted_cards") or {}).get("credit")
        alcohol = attrs.get("alcohol")
        ambience = attrs.get("ambience")
        good_for_meal = attrs.get("good_for_meal")
        noise_level = attrs.get("noise_level")
        attire = attrs.get("restaurants_attire")
        good_for_groups = attrs.get("restaurants_good_for_groups")
        outdoor_seating = attrs.get("outdoor_seating")
        hours = data.get("business_hours")

        # check if business and tag and append
        if business:
            if tag not in business.scenario_tags:
                business.scenario_tags.append(tag)
        # Otherwise, create new business record
        else:
            business = Business(
                id=data["id"],
                name=data.get("name"),
                categories=categories,
                price=data.get("price"),
                rating=rating,
                review_count=review_count,
                address=", ".join(location.get("display_address") or []),
                latitude=lat,
                longitude=lon,
                distance_from_office_km=distance,
                phone=data.get("display_phone"),
                image_url=data.get("image_url"),
                url=data.get("url"),
                is_closed=False,
                scenario_tags=[tag],
                website=website,
                accepts_credit_cards=accepts_credit_cards,
                alcohol=alcohol,
                ambience=ambience,
                good_for_meal=good_for_meal,
                noise_level=noise_level,
                attire=attire,
                good_for_groups=good_for_groups,
                outdoor_seating=outdoor_seating,
                business_hours=hours
            )
            session.add(business)

        stats["added"] += 1

    except Exception as e:
        logging.error(f"Error processing business ID {data.get('id')}: {e}")
        stats["skipped"]["errors"] = stats["skipped"].get("errors", 0) + 1

# Delete previously stored businesses that are now closed
def delete_closed_businesses():
    try:
        closed = session.query(Business).filter(Business.is_closed == True).all()
        count = len(closed)
        if count:
            for biz in closed:
                session.delete(biz)
            session.commit()
            logging.info(f"Deleted {count} businesses marked as closed.")
            print(f"Deleted {count} businesses marked as closed.")
        else:
            print("No closed businesses found for deletion.")
    except Exception as e:
        logging.error(f"Error deleting closed businesses: {e}")
        session.rollback()

# Main batch ingestion
def batch_ingest(terms):
    delete_closed_businesses()# Clean up closed businesses

    for tag in terms:
        logging.info(f"=== Ingesting term: {tag} ===")
        print(f"\nIngesting term: {tag}")
        stats = {
            "added": 0,
            "skipped": {
                "low_rating": 0,
                "low_reviews": 0,
                "closed": 0,
                "temp_closed": 0
            }
        }

        # you can set the amount of results
        # yelp limits to 50 results/api request
        for offset in range(0, 50, 50):
            print(f"Offset: {offset}")
            try:
                results = search_yelp(term=tag, offset=offset)
                if not results:
                    break
                try:
                    for biz in results:
                        upsert_business(biz, tag, stats)
                    session.commit()
                    print("Committed batch to DB")
                except Exception as e:
                    print(f"Error before/during commit: {e}")
                    logging.error(f"{tag} — Error before/during commit: {e}")
                    session.rollback()
            except Exception as e:
                logging.error(f"{tag} — Error at offset {offset}: {e}")
                session.rollback()
                continue

            time.sleep(1)#Yelp API rate limits

        logging.info(f"{tag} — Added: {stats['added']}, Skipped: {json.dumps(stats['skipped'])}")
        print(f"{tag} — Added: {stats['added']}, Skipped: {stats['skipped']}")


#scenario tags to filter on
if __name__ == "__main__":
    SCENARIO_TERMS = [
        "fine dining",
        "business dinner",
        "casual lunch",
        "celebration restaurant",
        "private dining",
        "Michelin",
        "large group dinner",
        "cocktail bar",
        "vegetarian",
    ]
    batch_ingest(SCENARIO_TERMS)
