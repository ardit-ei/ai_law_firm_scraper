# save_data.py
import json
import csv
import os

def save_to_csv(json_data, csv_file):
    try:
        data = json.loads(json_data)
    except json.JSONDecodeError:
        print("Invalid JSON data. Skipping...")
        return

    fieldnames = ['Full Name', 'Job Title', 'Email', 'Phone Number']

    file_exists = os.path.isfile(csv_file)

    with open(csv_file, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header only if the file is new
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            'Full Name': data.get('Full Name', ''),
            'Job Title': data.get('Job Title', ''),
            'Email': data.get('Email', ''),
            'Phone Number': data.get('Phone Number', '')
        })
