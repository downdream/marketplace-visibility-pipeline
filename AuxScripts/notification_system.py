# -*- coding: utf-8 -*-

### Python libraries ###
import requests

### Python scripts ###
from credentials import credentials_dict


'''Email settings'''
MAILGUN_API_KEY = 'mailgun api'
MAILGUN_DOMAIN = 'mailgun domain'
EMAIL_HOST = 'smtp.mailgun.org'
EMAIL_PORT = 587
EMAIL_USE_TLS = True
EMAIL_HOST_USER = credentials_dict['mailgun']['username']
EMAIL_HOST_PASSWORD = credentials_dict['mailgun']['password']
DEFAULT_FROM_EMAIL = 'email addrresses'

'''Send email notification using Mailgun API'''
def send_email(to, subject, body, attachments=None):
    # Construct the API endpoint and payload
    url = f'https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages'
    data = {
        'from': DEFAULT_FROM_EMAIL,
        'subject': subject,
        'text': body
    }
    # Add attachments to the payload if provided
    if attachments:
        files = [('attachment', (attachment.name, attachment.read())) for attachment in attachments]
        data['attachment'] = files
    # Send the email via Mailgun's API
    for recipient in to:
        data['to'] = recipient
        # Send the email via Mailgun's API
        response = requests.post(url, auth=('api', MAILGUN_API_KEY), data=data)
        if response.status_code != 200:
            # If there was an error sending the email, raise an exception
            raise Exception(f'Failed to send email to {recipient}: {response.text}')
