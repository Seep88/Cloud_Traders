## Project Structure

data/raw/input/amazon  
Contains raw Amazon Seller Central reports as received (no transformations).

scripts/raw_loaders  
Python scripts to ingest raw Seller Central reports into the database.

scripts/staging_transform  
Data cleaning, standardization, and business-rule transformations.

warehouse/amazon/dimensions  
Dimension tables (ASIN, SKU, date).

warehouse/amazon/facts  
Fact tables for sales, traffic, and advertising metrics.
