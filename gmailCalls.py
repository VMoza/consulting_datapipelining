import os
import pickle
import base64
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import mimetypes
import re


CLIENT_SECRET_FILE = 'client_secret_file.json'
SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.compose', 'https://www.googleapis.com/auth/gmail.modify']

def get_credentials():
    token_file = 'token.pickle'
    
    if os.path.exists(token_file):
        with open(token_file, 'rb') as token:
            credentials = pickle.load(token)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        credentials = flow.run_local_server(port=8080)
        
        with open(token_file, 'wb') as token:
            pickle.dump(credentials, token)
            
    return credentials


def get_gmail_service(credentials):
    return build('gmail', 'v1', credentials=credentials)


def choose_draft(service):
    drafts = service.users().drafts().list(userId='me').execute()

    if not drafts['drafts']:
        print('No drafts found.')
        return None

    for index, draft in enumerate(drafts['drafts'], start=1):
        draft_details = service.users().drafts().get(userId='me', id=draft['id']).execute()
        draft_subject = next((header['value'] for header in draft_details['message']['payload']['headers'] if header['name'].lower() == 'subject'), "(No subject)")
        print(f"{index}: {draft_subject}")

    while True:
        try:
            selected_draft_number = int(input("Enter the number of the draft you'd like to use or 0 to exit: "))
            if 0 <= selected_draft_number <= len(drafts['drafts']):
                break
            else:
                print("Please enter a valid number.")
        except ValueError:
            print("Please enter a valid number.")

    if selected_draft_number == 0:
        return None
    else:
        return drafts['drafts'][selected_draft_number - 1]


def get_draft_details(service, draft_id):
    return service.users().drafts().get(userId='me', id=draft_id).execute()


def get_draft_subject(payload):
    return next((header['value'] for header in payload['headers'] if header['name'] == 'Subject'), "(No subject)")


def find_draft_body(parts):
    for part in parts:
        mimeType = part.get('mimeType', '')
        if mimeType.startswith('text/'):
            body = part.get('body', {})
            if 'data' in body:
                body_data = body['data']
                content = base64.urlsafe_b64decode(body_data).decode('utf-8')
                content = re.sub('\r\n|\r|\n', '\n', content).lstrip('\n')  # Normalize newline characters and remove leading newline

                # Replace the placeholders with the actual placeholders
                content = content.replace("{{FIRST}}", "{{FIRST}}").replace("{{COMPANY}}", "{{COMPANY}}")

                # Split content into paragraphs and signature
                paragraphs = content.split('\n\n')

                # Remove single newline characters within paragraphs
                cleaned_paragraphs = [re.sub('\n', ' ', paragraph) for paragraph in paragraphs]

                # Reassemble the email body using double newline characters
                content = "\n\n".join(cleaned_paragraphs)

                return MIMEText(content, _subtype=mimeType.split('/')[-1])
        elif mimeType.startswith('multipart/'):
            nested_parts = part.get('parts', [])
            if nested_parts:
                return find_draft_body(nested_parts)
    return None


def update_body_content(body, name, company):
    content = body.get_payload().replace('{{FIRST}}', name.split()[0]).replace('{{COMPANY}}', company)
    charset = body.get_content_charset()
    subtype = body.get_content_subtype()

    new_body = MIMEText(content, _subtype=subtype, _charset=charset)
    new_body.replace_header('Content-Type', body['Content-Type'])
    new_body.replace_header('Content-Transfer-Encoding', body['Content-Transfer-Encoding'])
    new_body.set_charset(charset)

    return new_body


def send_email(service, to_email, subject, body, attachment_path=None):
    message = MIMEMultipart("related")
    message['to'] = to_email
    message['subject'] = subject

    message.attach(body)

    if attachment_path:
        content_type, encoding = mimetypes.guess_type(attachment_path)

        if content_type is None or encoding is not None:
            content_type = 'application/octet-stream'

        main_type, sub_type = content_type.split('/', 1)
        with open(attachment_path, 'rb') as file:
            attachment = MIMEBase(main_type, sub_type)
            attachment.set_payload(file.read())

        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_path))
        message.attach(attachment)

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
    send_message = {'raw': raw_message}
    send_message = service.users().messages().send(userId="me", body=send_message).execute()

    print(F'sent message to {to_email} Message Id: {send_message["id"]}')
    return send_message


def main():
    data = {
        'COMPANY': ['JV Consulting', 'U r Gay'],
        'NAME': ['Vasuman Moza', 'Shriya Moza'],
        'EMAIL': ['vasumanmoza@berkeley.edu', 'shriyanan@berkeley.edu']
    }

    df = pd.DataFrame(data)
    credentials = get_credentials()
    service = get_gmail_service(credentials)

    selected_draft = choose_draft(service)

    if selected_draft is None:
        print("No email was sent. Exiting.")
        return

    else:
        draft_details = get_draft_details(service, selected_draft['id'])
        payload = draft_details['message']['payload']

        draft_subject = get_draft_subject(payload)
        parts = payload.get('parts', [])
        if parts:
            draft_body = find_draft_body(parts)
        else:
            mimeType = payload.get('mimeType', '')
            draft_body = payload.get('body', {}).get('data', '')
            if draft_body:
                if mimeType.startswith('text/'):
                    draft_body = MIMEText(base64.urlsafe_b64decode(draft_body).decode('utf-8'), _subtype=mimeType.split('/')[-1])
                else:
                    draft_body = ''

        attachment_path = os.path.join(os.getcwd(), 'JV Consulting Deck.pdf')

        for _, row in df.iterrows():
            name, company, email = row['NAME'], row['COMPANY'], row['EMAIL']
            email_subject = draft_subject.replace('{{COMPANY}}', company)

            if draft_body:
                updated_body = update_body_content(draft_body, name, company)
                send_email(service, email, email_subject, updated_body, attachment_path)
            else:
                print(f"Error: Draft body is empty. Skipping email for {name}.")


if __name__ == '__main__':
    main()
