# extract_data.py
import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv("")
BASE_URL = "https://api.themoviedb.org/3/movie/popular"
TARGET_FILE = "raw_popular_movies.json"
PAGES_TO_FETCH = 5 # Fetch 5 pages of movies (100 records)

def extract_data_from_tmdb(api_key, base_url, num_pages, target_file):
    """Fetches popular movie data from TMDb and saves it to a JSON file."""
    
    if not api_key:
        print("Error: TMDB_API_KEY not found in .env file.")
        return

    all_movies = []
    print(f"--- Starting Data Extraction from TMDb API ({num_pages} pages) ---")
    
    for page in range(1, num_pages + 1):
        params = {
            "api_key": api_key,
            "language": "en-US",
            "page": page
        }
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            
            # Check if 'results' key exists and is a list
            if 'results' in data and isinstance(data['results'], list):
                all_movies.extend(data['results'])
                print(f"Successfully fetched Page {page} of {data.get('total_pages')}...")
            else:
                print(f"Warning: Page {page} data is not in expected format.")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            break # Stop extraction on error
    
    # Save the data to a JSON file
    try:
        with open(target_file, 'w', encoding='utf-8') as f:
            json.dump(all_movies, f, ensure_ascii=False, indent=4)
        print(f"\n--- Data successfully saved to {target_file} (Total records: {len(all_movies)}) ---")
    except IOError as e:
        print(f"Error saving data to file: {e}")

if __name__ == "__main__":
    extract_data_from_tmdb(API_KEY, BASE_URL, PAGES_TO_FETCH, TARGET_FILE)
