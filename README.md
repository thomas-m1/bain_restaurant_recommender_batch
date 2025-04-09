# Bain Restaurant Batch Ingestor

This script pulls restaurant data from the Yelp API based on specific search terms (e.g., "fine dining") and stores the results in a PostgreSQL database. It's designed to help teams quickly identify quality dining options near Bain’s Toronto office.

## What It Does

- Queries Yelp for restaurant data using defined search terms  
- Skips restaurants with low ratings, few reviews, or that are closed  
- Calculates distance from Bain's Toronto office using latitude/longitude  
- Enriches data with fields like ambience, noise level, good for groups, outdoor seating, etc.  
- Saves all results to a PostgreSQL database using SQLAlchemy  
- Logs each batch run with timestamped output

## How to Run

1. Clone the repository
2.  Create a `.env` file with the following variables:
YELP_API_KEY=your_yelp_api_key
DATABASE_URL=your_postgres_db_url
3. Run the script:
python batch_ingest.py

Notes
To control how many results are fetched per search term, update this line in the script:
for offset in range(0, 100, 50):
Replace 100 with the total number of results you want to fetch per term (in multiples of 50).

