import requests
from pymongo import MongoClient
import json
from tqdm import tqdm
from bson import ObjectId
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27019/')

# Select the authors and publications databases and collections
db = client.authors
authors_collection = db.authors
publications_collection = db.publications

# Unpaywall API email
EMAIL = 'sara.salamat@torontomu.ca'

# Custom JSON encoder to handle ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(JSONEncoder, self).default(obj)

# Function to check if a publication is open access using the Unpaywall API
def is_open_access(doi):
    url = f"https://api.unpaywall.org/v2/{doi}?email={EMAIL}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data:
            return data.get('is_oa', False), data.get('best_oa_location', {}).get('url', 'No OA URL available')
    return False, 'No OA URL available'

# Function to process a single publication and return detailed info if open access
def process_publication(duplicate, pub):
    try:
        publication_key = pub['publicationKey']
        publication = publications_collection.find_one({'key': publication_key})
        if publication:
            doi = publication.get('doiLink', '').replace('https://doi.org/', '')
            if doi:  # Only check open access if DOI is available
                is_oa, oa_url = is_open_access(doi)
                if is_oa:
                    # Return detailed publication info if open access
                    return [
                        duplicate['author_name'],
                        duplicate['author_id'],
                        duplicate['title'],
                        publication_key,
                        doi,
                        pub['year'],
                        pub['type'],
                        oa_url
                    ]
    except Exception as e:
        pass
    return None

# Function to process duplicate publications and write detailed info to a TSV file if open access
def process_duplicate_publications(input_file, output_file, max_workers=10):
    try:
        with open(input_file, 'r') as file:
            duplicates = json.load(file)
    except json.JSONDecodeError as e:
        print(f"Error reading JSON from {input_file}: {e}")
        return

    with open(output_file, 'w', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        # Write header
        writer.writerow(['author_name', 'author_id', 'title', 'publication_key', 'doi', 'year', 'type', 'open_access_url'])

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_pub = {executor.submit(process_publication, duplicate, pub): (duplicate, pub) for duplicate in duplicates for pub in duplicate['publications']}

            for future in tqdm(as_completed(future_to_pub), total=len(future_to_pub), desc="Processing duplicates"):
                result = future.result()
                if result:
                    writer.writerow(result)

# Process duplicates and write detailed information to 'open_access_publications.tsv'
process_duplicate_publications('/home/sara/ArXivData/reformatted_duplicate_publications.json', '/home/sara/ArXivData/open_access_publications.tsv', max_workers=20)
