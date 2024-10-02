# extractor.py
import openai
import os
import time
import json

# Set your OpenAI API key
openai.api_key = ''

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
\"\"\"
{cleaned_text}
\"\"\"
"""

    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{'role': 'user', 'content': prompt}],
                temperature=0.3,
                max_tokens=1500,  # Adjust as needed
                top_p=0.2,
                frequency_penalty=0,
                presence_penalty=0,
                response_format={
                    "type": "json_object"
                }
            )
            content = response.choices[0].message.content
            # Ensure the response is a valid JSON
            json.loads(content)
            return content
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}. Retrying in {2 ** attempt} seconds...")
            traceback.print_exc()
            time.sleep(2 ** attempt)
        except openai.error.OpenAIError as e:
            print(f"OpenAI API error: {e}. Retrying in {2 ** attempt} seconds...")
            traceback.print_exc()
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying in {2 ** attempt} seconds...")
            traceback.print_exc()
            time.sleep(2 ** attempt)
    return '{}'