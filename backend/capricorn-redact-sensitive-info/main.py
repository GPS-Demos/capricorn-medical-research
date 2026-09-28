# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functions_framework
from flask import jsonify
from google.cloud import dlp_v2
from google import genai
from google.genai import types
import json
import re
from datetime import datetime
import os

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Allowed origins for CORS
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.environ.get(
        "ALLOWED_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,https://capricorn-medical-research.web.app"
    ).split(",")
    if origin.strip()
]

def get_cors_origin(req):
    origin = req.headers.get("Origin", "")
    if origin in ALLOWED_ORIGINS:
        return origin
    return ALLOWED_ORIGINS[0] if ALLOWED_ORIGINS else "http://localhost:3000"

# Initialize DLP client
dlp_client = dlp_v2.DlpServiceClient()

# Initialize Gemini client with environment variables
client = genai.Client(
    vertexai=True,
    project=os.environ.get('PROJECT_ID', 'gemini-med-lit-review'),
    location=os.environ.get('LOCATION', 'us-central1'),
)

def get_info_types():
    """Get list of info types to redact, including DATE_OF_BIRTH."""
    all_types = [
        {"name": info_type}
       for info_type in [
            "EMAIL_ADDRESS",
            "STREET_ADDRESS",
            "LOCATION",
            "DATE_OF_BIRTH",
            "US_SOCIAL_SECURITY_NUMBER",
            "PASSPORT",
            "DRIVERS_LICENSE_NUMBER",
            "PHONE_NUMBER",
            "FIRST_NAME",
            "LAST_NAME",
            "PERSON_NAME",
            "MEDICAL_RECORD_NUMBER",
            "CREDIT_CARD_NUMBER",
            "CREDIT_CARD_EXPIRATION_DATE",
            "US_PASSPORT",
            "UK_DRIVERS_LICENSE_NUMBER",
            "AUSTRALIA_DRIVERS_LICENSE_NUMBER",
            "CANADA_DRIVERS_LICENSE_NUMBER",
            "GERMANY_DRIVERS_LICENSE_NUMBER",
            "US_DRIVERS_LICENSE_NUMBER",
            "IRELAND_DRIVING_LICENSE_NUMBER",
            "UK_PASSPORT",
            "FRANCE_DRIVERS_LICENSE_NUMBER",
            "MEXICO_CURP_NUMBER",
            "HONG_KONG_ID_NUMBER",
            "TAIWAN_ID_NUMBER",
            "BRAZIL_CPF_NUMBER",
            "ARGENTINA_DNI_NUMBER",
            "BELGIUM_NATIONAL_ID_CARD_NUMBER",
            "ITALY_FISCAL_CODE",
            "POLAND_PASSPORT",
            "SOUTH_AFRICA_ID_NUMBER",
            "US_STATE",
            "US_EMPLOYER_IDENTIFICATION_NUMBER",
            "GERMANY_PASSPORT",
            "AUSTRALIA_MEDICARE_NUMBER",
            "POLAND_PESEL_NUMBER",
            "SPAIN_NIE_NUMBER",
            "ITALY_PASSPORT",
            "US_HEALTHCARE_NPI",
            "CANADA_SOCIAL_INSURANCE_NUMBER",
            "FINLAND_NATIONAL_ID_NUMBER",
            "NETHERLANDS_BSN_NUMBER",
            "NEW_ZEALAND_NHI_NUMBER",
            "THAILAND_NATIONAL_ID_NUMBER",
            "AUSTRALIA_PASSPORT",
            "PERU_DNI_NUMBER",
            "COLOMBIA_CDC_NUMBER",
            "DENMARK_CPR_NUMBER",
            "SPAIN_SOCIAL_SECURITY_NUMBER",
            "SPAIN_DRIVERS_LICENSE_NUMBER",
            "GERMANY_SCHUFA_ID",
            "KOREA_RRN",
            "NEW_ZEALAND_IRD_NUMBER",
            "CANADA_OHIP",
            "FRANCE_NIR",
            "INDIA_GST_INDIVIDUAL",
            "JAPAN_INDIVIDUAL_NUMBER",
            "SPAIN_DNI_NUMBER",
            "INDIA_PAN_INDIVIDUAL",
            "IRELAND_PPSN",
            "ARMENIA_PASSPORT",
            "FRANCE_CNI",
            "AUSTRALIA_TAX_FILE_NUMBER",
            "UKRAINE_PASSPORT",
            "MEXICO_PASSPORT",
            "NETHERLANDS_PASSPORT",
            "CHINA_RESIDENT_ID_NUMBER",
            "CANADA_PASSPORT",
            "UZBEKISTAN_PASSPORT",
            "VENEZUELA_CDI_NUMBER",
            "PARAGUAY_CIC_NUMBER",
            "JAPAN_PASSPORT",
            "TAIWAN_PASSPORT",
            "UK_TAXPAYER_REFERENCE",
            "IRELAND_EIRCODE",
            "US_DEA_NUMBER",
            "PORTUGAL_CDC_NUMBER",
            "URUGUAY_CDI_NUMBER",
            "SPAIN_NIF_NUMBER",
            "GERMANY_IDENTITY_CARD_NUMBER",
            "ISRAEL_IDENTITY_CARD_NUMBER",
            "UK_NATIONAL_HEALTH_SERVICE_NUMBER",
            "SWEDEN_NATIONAL_ID_NUMBER",
            "FINANCIAL_ACCOUNT_NUMBER",
            "DOD_ID_NUMBER",
            "CHINA_PASSPORT",
            "SCOTLAND_COMMUNITY_HEALTH_INDEX_NUMBER",
            "US_INDIVIDUAL_TAXPAYER_IDENTIFICATION_NUMBER",
            "US_ADOPTION_TAXPAYER_IDENTIFICATION_NUMBER",
            "NORWAY_NI_NUMBER",
            "IRELAND_PASSPORT",
            "POLAND_NATIONAL_ID_NUMBER",
            "AZERBAIJAN_PASSPORT",
            "JAPAN_BANK_ACCOUNT",
            "FINANCIAL_ID",
            "VEHICLE_IDENTIFICATION_NUMBER",
            "MARITAL_STATUS",
            "IMMIGRATION_STATUS",
            "RELIGIOUS_TERM",
            "EMPLOYMENT_STATUS",
            "COUNTRY_DEMOGRAPHIC",
            "GEOGRAPHIC_DATA",
            "LOCATION_COORDINATES",
            "GENERIC_ID",
            "TECHNICAL_ID",
            "DOCUMENT_TYPE/FINANCE/REGULATORY",
            "POLAND_PASSPORT",
            "POLITICAL_TERM",
            "WEAK_PASSWORD_HASH",
            "XSRF_TOKEN",
            "DOMAIN_NAME",
            "US_TOLLFREE_PHONE_NUMBER",
            "IRELAND_PPSN",
            "PORTUGAL_NIB_NUMBER",
            "SINGAPORE_NATIONAL_REGISTRATION_ID_NUMBER",
            "PORTUGAL_SOCIAL_SECURITY_NUMBER",
            "UK_NATIONAL_INSURANCE_NUMBER",
            "AMERICAN_BANKERS_CUSIP_ID",
            "CHILE_CDI_NUMBER",
            "US_BANK_ROUTING_MICR",
            "KOREA_ARN",
            "SECURITY_DATA",
            "US_PREPARER_TAXPAYER_IDENTIFICATION_NUMBER",
            "US_VEHICLE_IDENTIFICATION_NUMBER",
            "FINANCIAL_ID",
            "ICCID_NUMBER",
            "MALE_NAME",
            "FEMALE_NAME",
            "ADVERTISING_ID",
        ]
    ]
    return all_types

COMMON_DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y", "%d-%m-%Y",
    "%Y/%m/%d", "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y",
    "%m/%d/%y", "%m-%d-%y"
]

def standardize_date(date_string):
    cleaned = date_string.strip()
    for fmt in COMMON_DATE_FORMATS:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Fallback to Gemini if standard parsing fails
    prompt = f"""
    Convert the following date to YYYY-MM-DD format: {cleaned}
    
    Respond with ONLY the standardized date in YYYY-MM-DD format.
    If the date cannot be converted, respond with 'INVALID'.
    """
    
    contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
    
    generate_content_config = types.GenerateContentConfig(
        temperature=0,
        top_p=1,
        max_output_tokens=20,
        response_modalities=["TEXT"],
    )
    
    response = client.models.generate_content(
        model="gemini-2.0-flash-001",
        contents=contents,
        config=generate_content_config
    )
    
    standardized_date = response.text.strip()
    if standardized_date == 'INVALID':
        raise ValueError("Invalid date format")
    return standardized_date

def calculate_age(birth_date):
    today = datetime.now()
    birth_date = datetime.strptime(birth_date, "%Y-%m-%d")
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return age

def deidentify_content(project_id, text, debug_info=None):
    """Deidentify sensitive content using DLP API and localized date processing."""
    if not text:
        return text

    logger.info("Starting deidentify_content execution")

    # Get info types for redaction
    info_types = get_info_types()

    # Set up DLP API request for inspection
    inspect_config = {
        "info_types": info_types,
        "min_likelihood": dlp_v2.Likelihood.LIKELY,
        "include_quote": True,
    }

    # Use DLP to find all instances of sensitive information
    item = {"value": text}
    parent = f"projects/{project_id}"
    inspect_request = {
        "parent": parent,
        "inspect_config": inspect_config,
        "item": item,
    }

    logger.info("Calling DLP API for content inspection")
    inspect_response = dlp_client.inspect_content(request=inspect_request)

    # Process each finding safely (bounded to prevent resource exhaustion)
    dob_count = 0
    for finding in inspect_response.result.findings:
        info_type = finding.info_type.name
        quote = finding.quote
        
        # Safely track identified info type without storing plaintext quote in debug_info
        if debug_info is not None:
            debug_info.append(f"Found {info_type}")

        if info_type == "DATE_OF_BIRTH":
            dob_count += 1
            if dob_count > 20:
                text = text.replace(quote, "[REDACTED DATE_OF_BIRTH]")
                continue
            try:
                standardized_date = standardize_date(quote)
                age = calculate_age(standardized_date)
                text = text.replace(quote, f"Age: {age}")
            except Exception as e:
                logger.warning(f"Error processing DATE_OF_BIRTH: {str(e)}")
                text = text.replace(quote, "[REDACTED DATE_OF_BIRTH]")

    # Set up DLP API request for deidentification of remaining sensitive information
    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "replace_config": {
                            "new_value": {
                                "string_value": "[REDACTED]"
                            }
                        }
                    }
                }
            ]
        }
    }

    deidentify_request = {
        "parent": parent,
        "deidentify_config": deidentify_config,
        "inspect_config": inspect_config,
        "item": {"value": text},
    }

    try:
        logger.info("Calling DLP API for content deidentification")
        deidentify_response = dlp_client.deidentify_content(request=deidentify_request)
        return deidentify_response.item.value
    except Exception as e:
        logger.error(f"Error in deidentify_content: {str(e)}")
        return None

@functions_framework.http
def redact_sensitive_info(request):
    """HTTP Cloud Function for redacting sensitive information."""
    origin = get_cors_origin(request)
    # Handle CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': origin,
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {
        'Access-Control-Allow-Origin': origin,
        'Content-Type': 'application/json'
    }

    # Isolated request-scoped debug info list (never touches global builtins.print)
    debug_info = []

    try:
        request_json = request.get_json()
        if not request_json:
            logger.error("No JSON data received in request")
            return jsonify({'error': 'No JSON data received'}), 400, headers

        # Extract text to redact
        text = request_json.get('text')
        if not text:
            logger.error("No text provided for redaction")
            return jsonify({'error': 'No text provided'}), 400, headers

        # Redact sensitive information using project ID from environment
        project_id = os.environ.get('DLP_PROJECT_ID', os.environ.get('PROJECT_ID', 'gemini-med-lit-review'))
        redacted_text = deidentify_content(project_id, text, debug_info=debug_info)

        if redacted_text is None:
            return jsonify({
                'success': False,
                'error': 'Failed to redact text',
                'debugInfo': debug_info
            }), 500, headers

        # Extract identified info types from debug_info safely
        identified_info_types = list(dict.fromkeys(
            line.replace('Found ', '').strip() for line in debug_info if line.startswith('Found ')
        ))

        return jsonify({
            'success': True,
            'redactedText': redacted_text,
            'debugInfo': debug_info,
            'identifiedInfoTypes': identified_info_types
        }), 200, headers

    except Exception as e:
        logger.error(f"Error in redact_sensitive_info: {str(e)}")
        return jsonify({'error': 'An internal error occurred.', 'debugInfo': []}), 500, headers


if __name__ == "__main__":
    # Test cases
    test_texts = [
        "A now almost 4 year old female diagnosed with KMT2A-rearranged AML on 05/15/2021.",
        "Patient born on 1990-03-22 is now 33 years old.",
        "Treatment started on 12/31/2023 for this 45-year-old male.",
        "The 2-month-old infant was admitted on 2025-01-15.",
        "John Doe's email is john.doe@example.com and phone number is 555-123-4567."
    ]

    print("Running test cases:")
    for i, text in enumerate(test_texts, 1):
        print(f"\nTest Case {i}:")
        print(f"Original text: {text}")
        result = deidentify_content("gemini-med-lit-review", text)
        print(f"Redacted text: {result}")
        print("=" * 50)

    # Run the Flask app
    app = functions_framework.create_app(target="redact_sensitive_info")
    app.run(host="0.0.0.0", port=8080, debug=True)
