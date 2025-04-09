Bain Restaurant Batch Ingestor
This script pulls restaurant data from the Yelp API based on specific search terms (e.g., "fine dining") and stores the results in a PostgreSQL database. It's designed to help teams quickly identify quality dining options near Bain’s Toronto office.

What It Does
Queries Yelp for restaurant data using defined search terms

Skips restaurants with low ratings, few reviews, or that are closed

Calculates distance from Bain's Toronto office using latitude/longitude

Enriches data with fields like ambience, noise level, good for groups, outdoor seating, etc.

Saves all results to a PostgreSQL database using SQLAlchemy

Logs each batch run with timestamped output