# extractor.py
import openai
import os
import time
import json
import random
import traceback

# Set your OpenAI API key
openai.api_key = os.getenv('OPENAI_API_KEY', '')

def extract_data(cleaned_text):
    prompt = f"""
    You will be provided HTML from a web page. Your goal is to analyze this HTML and return a JSON object with the following structure:
    ```json
        {{
        "person": {{
            "first_name": "",
            "middle_name": "",
            "last_name": "",
            "job_title": "",
            "direct_phone": "",
            "direct_phone_extension": "",
            "mobile_phone": "",
            "email": "",
            "location_city": "",
            "location_state": "",
            "profile_image_url": "",
            "practice_areas": ""
        }}
        }}
    ```
    Notes:
        - The company_address array should contain objects with address details, and there may be multiple locations.
        - law_firm_email is the general inbox for the lawfirm, similar to info@lawfirm.com or intake@lawfirm.com. It should not be the same as the attorney.email.
        - The person.direct_phone or person.cell should be found in the main content area and they should differ from law_firm_phone.
        - Ensure all values are extracted from the HTML and correctly assigned to the respective fields in the JSON object.
        - Ensure all text is properly cased. We don't want titles, for example, to be fully uppercased.
        - Do not guess the email address. If the email address is not found in the code, it should be left blank.
    Text:
    {cleaned_text}
    """

    max_retries = 5
    for attempt in range(max_retries):
        try:
            ## DO NOT EDIT THIS SECTION
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=4095,
                top_p=0.2,
                frequency_penalty=0,
                presence_penalty=0,
                response_format={
                    "type": "json_object"
                }
            )
            # Access the message content from the response
            message_content = response.choices[0].message.content

            # Log the raw content received
            print(f"Received content from OpenAI: {message_content}")

            try:
                data = json.loads(message_content)
                print("Parsed JSON data:", data)
            except json.JSONDecodeError:
                print("Failed to parse JSON data.")


            # Additional validation to ensure the extracted data matches the expected schema
            if "person" not in data or not isinstance(data.get("person"), dict):
                raise ValueError("Invalid JSON schema: Missing 'person' key or incorrect type")

            required_fields = [
                "first_name", "last_name", "job_title", "direct_phone", "direct_phone_extension",
                "mobile_phone", "email", "location_city", "location_state"
            ]
            for field in required_fields:
                if field not in data["person"]:
                    raise ValueError(f"Invalid JSON schema: Missing required field '{field}' in 'person'")

            return message_content
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Validation error: {e}. Content was: {message_content}")
            traceback.print_exc()
            time.sleep(2 ** attempt + random.uniform(0, 1))
        except openai.error.OpenAIError as e:
            print(f"OpenAI API error: {e}. Retrying in {2 ** attempt + random.uniform(0, 1)} seconds...")
            traceback.print_exc()
            time.sleep(2 ** attempt + random.uniform(0, 1))
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying in {2 ** attempt + random.uniform(0, 1)} seconds...")
            traceback.print_exc()
            time.sleep(2 ** attempt + random.uniform(0, 1))

    return '{}'
