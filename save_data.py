# save_data.py
import json
import csv
import os

def save_to_csv(json_data, csv_file):
    # Ensure the data is a dictionary. If it's a string, try to load it as JSON.
    if isinstance(json_data, str):
        try:
            data = json.loads(json_data)
        except json.JSONDecodeError:
            print("Invalid JSON data. Skipping...")
            return
    elif isinstance(json_data, dict):
        data = json_data
    else:
        print("Unexpected data format. Skipping...")
        return

    # Add the scraped URL to the data
    scraped_url = data.get('scraped_url', '')

    fieldnames = [
        'scraped_url', 'first_name', 'middle_name', 'last_name', 'job_title', 'direct_phone', 'direct_phone_extension', 'mobile_phone', 'email',
        'location_city', 'location_state', 'profile_image_url', 'practice_areas'
    ]

    file_exists = os.path.isfile(csv_file)

    with open(csv_file, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header only if the file is new
        if not file_exists:
            writer.writeheader()

        person = data.get("person", {})
        writer.writerow({
            'scraped_url': scraped_url,
            'first_name': person.get('first_name', ''),
            'middle_name': person.get('middle_name', ''),
            'last_name': person.get('last_name', ''),
            'job_title': person.get('job_title', ''),
            'direct_phone': person.get('direct_phone', ''),
            'direct_phone_extension': person.get('direct_phone_extension', ''),
            'mobile_phone': person.get('mobile_phone', ''),
            'email': person.get('email', ''),
            'location_city': person.get('location_city', ''),
            'location_state': person.get('location_state', ''),
            'profile_image_url': person.get('profile_image_url', ''),
            'practice_areas': person.get('practice_areas', '')
        })
