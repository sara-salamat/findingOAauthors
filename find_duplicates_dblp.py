from pymongo import MongoClient
from collections import defaultdict
import json
from tqdm import tqdm
import time

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27019/')

# Select the authors database and collection
db = client.authors
collection = db.authors

# Function to find duplicate publications for each author and write to a file
def find_duplicate_publications_and_write_to_file(output_file):
    total_authors = collection.count_documents({})  # Get the total number of authors for progress bar
    authors = collection.find()
    start_time = time.time()

    with open(output_file, 'w') as file:
        for i, author in enumerate(tqdm(authors, total=total_authors, desc="Processing authors")):
            pub_titles = defaultdict(list)
            duplicates = []

            for pub in author.get('publications', []):
                pub_titles[pub['title']].append(pub)

            for title, pubs in pub_titles.items():
                if len(pubs) > 1:
                    duplicates.append({
                        'author_name': author['mainName'],
                        'author_id': str(author['_id']),
                        'title': title,
                        'publications': pubs
                    })

            if duplicates:
                for duplicate in duplicates:
                    file.write(json.dumps(duplicate, indent=4) + '\n')

            # Calculate and print estimated time to finish occasionally
            elapsed_time = time.time() - start_time
            progress = (i + 1) / total_authors
            estimated_total_time = elapsed_time / progress
            estimated_time_left = estimated_total_time - elapsed_time

            if (i + 1) % 10000 == 0:  # Print every 10 authors
                print(f"Estimated time left: {estimated_time_left:.2f} seconds")

# Find duplicate publications and write to 'duplicate_publications.json'
find_duplicate_publications_and_write_to_file('/home/sara/ArXivData/duplicate_publications.json')
