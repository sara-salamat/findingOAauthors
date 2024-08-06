import requests
import csv
import os
import re
import pdfplumber
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Unpaywall API email
EMAIL = 'sara.salamat@torontomu.ca'

# Function to download a PDF
def download_pdf(url, file_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return True
    return False

def extract_emails_from_pdf(file_path):
    emails = set()
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                emails.update(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b', text))
    return emails


def process_publication(publication):
    try:
        oa_url = publication['open_access_url']
        publication_key = publication['publication_key']
        
        if oa_url != 'No OA URL available':
            # Download the PDF
            pdf_path = f'/tmp/{publication_key}.pdf'
            if download_pdf(oa_url, pdf_path):
                # Extract emails
                emails = extract_emails_from_pdf(pdf_path)
                os.remove(pdf_path)  # Clean up the downloaded PDF

                # Return publication info with extracted emails
                publication['emails'] = ', '.join(emails)
                return publication
    except Exception as e:
        print(f"Error processing publication {publication_key}: {e}")
    return None
def update_filtered_tsv(input_file, output_file, max_workers=10):
    publications = []
    
    # Read the filtered TSV file
    with open(input_file, 'r', newline='') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            publications.append(row)
    
    with open(output_file, 'w', newline='') as tsvfile:
        fieldnames = reader.fieldnames + ['emails']
        writer = csv.DictWriter(tsvfile, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_pub = {executor.submit(process_publication, pub): pub for pub in publications}
            
            for future in tqdm(as_completed(future_to_pub), total=len(future_to_pub), desc="Processing publications"):
                result = future.result()
                if result:
                    writer.writerow(result)

# Update the filtered TSV file with extracted emails
update_filtered_tsv('/home/sara/ArXivData/open_access_publications.tsv', '/home/sara/ArXivData/updated_open_access_publications.tsv', max_workers=20)