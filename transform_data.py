# transform_data.py
import pandas as pd
import json
import os

INPUT_FILE = "raw_popular_movies.json"
OUTPUT_FILE = "transformed_high_popularity_movies.csv"
POPULARITY_THRESHOLD = 50 # Set the threshold for high popularity

def transform_movie_data(input_file, output_file_csv, threshold):
    """Loads raw movie data, cleans, deduplicates, filters, and saves as CSV."""
    print("--- Starting Data Transformation (T) ---")
    
    if not os.path.exists(input_file):
        print(f"Error: Input file not found: {input_file}")
        return

    # 1. Load and Clean
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            movie_list = json.load(f)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return

    df = pd.DataFrame(movie_list)
    print(f"Total raw records loaded: {len(df)}")
    
    # CRITICAL FIX: Deduplication to prevent DB UniqueViolation error
    df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    print(f"Unique records after deduplication: {len(df)}")

    # 2. Select and Rename Columns
    df_transformed = df[[
        'id', 'title', 'release_date', 'popularity', 'vote_average', 'overview'
    ]].copy()

    # 3. Filter based on Popularity
    df_filtered = df_transformed[df_transformed['popularity'] > threshold]
    print(f"Filtering complete. Records with popularity > {threshold}: {len(df_filtered)}")

    # 4. Save the transformed data
    try:
        df_filtered.to_csv(output_file_csv, index=False)
        print(f"--- Transformation complete! Data saved to {output_file_csv} ---")
    except IOError as e:
        print(f"Error saving CSV file: {e}")

if __name__ == "__main__":
    transform_movie_data(INPUT_FILE, OUTPUT_FILE, POPULARITY_THRESHOLD)