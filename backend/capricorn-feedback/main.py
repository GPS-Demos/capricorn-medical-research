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

import html
import os
import json
import logging
from flask import jsonify, request
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, To

# Configure logging
logging.basicConfig(level=logging.INFO)
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

# Define team recipients
TEAM_RECIPIENTS = [
    'williszhang@google.com',
    'U.ilan-2@prinsesmaximacentrum.nl'
]

def send_feedback_email(request):
    """
    Cloud Function to send feedback emails using SendGrid.
    
    Args:
        request (flask.Request): The request object.
        
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`.
    """
    origin = get_cors_origin(request)
    # Set CORS headers for preflight requests
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': origin,
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': origin
    }
    
    try:
        # Get request data
        request_json = request.get_json(silent=True)
        
        if not request_json:
            logger.error("No JSON data in request")
            return (jsonify({'success': False, 'error': 'No data provided'}), 400, headers)
        
        # Extract and HTML-escape feedback data to prevent email HTML injection / XSS
        name = html.escape(str(request_json.get('name', 'Anonymous User')))
        email = html.escape(str(request_json.get('email', 'No email provided')))
        raw_feedback = request_json.get('feedback')
        
        if not raw_feedback:
            logger.error("No feedback text provided")
            return (jsonify({'success': False, 'error': 'No feedback text provided'}), 400, headers)
        
        feedback_text = html.escape(str(raw_feedback))
        
        # Get SendGrid API key from environment variable
        api_key = os.environ.get('SENDGRID_API_KEY')
        if not api_key:
            logger.error("SendGrid API key not found")
            return (jsonify({'success': False, 'error': 'Email service configuration error'}), 500, headers)
        
        # Restrict recipients strictly to verified team members; never reflect user-provided email
        recipients = TEAM_RECIPIENTS.copy()
        
        # Create email message with escaped content
        message = Mail(
            from_email='williszhang@williszhang.com',
            to_emails=[To(email=recipient) for recipient in recipients],
            subject='Capricorn Medical Research App Feedback',
            html_content=f"""
            <h2>New Feedback Received</h2>
            <p><strong>From:</strong> {name}</p>
            <p><strong>Email:</strong> {email}</p>
            <p><strong>Feedback:</strong></p>
            <p>{feedback_text}</p>
            <p>Thank you for helping us improve the Capricorn Medical Research App.</p>
            """
        )
        
        # Send email
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        
        logger.info(f"Email sent with status code: {response.status_code}")
        
        return (jsonify({'success': True}), 200, headers)
        
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")
        return (jsonify({'success': False, 'error': 'Failed to process feedback'}), 500, headers)
