import openai
from openai import ChatCompletion
import pickle
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

openai.api_key = 'OPENAI_API_SECRET_KEY'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = None
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'client_secret_file.json', SCOPES)
        creds = flow.run_local_server(port=8080)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

service = build('sheets', 'v4', credentials=creds)

# Assuming you have a sheet ID
SHEET_ID = '1KqChsN0FtaSN4lZrcJU57aPsIdx7NdssAonWOgQMXzw'
RANGE_NAME = 'Pilot Scrape!A:Z'
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SHEET_ID, range=RANGE_NAME).execute()
values = result.get('values', [])


def extract_role(text):
    messages = [
        {"role": "user", "content": "You are an assistant designed to extract the primary job title or function from a LinkedIn professional description. You should only return the role, nothing more. For example, given 'Manager, Commercial Strategy - Cardiovascular at The Janssen Pharmaceutical Companies of Johnson & Johnson', you should return Commercial Strategy Manager. Do not return any extra text."},
        {"role": "user", "content": f"Here is the LinkedIn Professional Description. Please return what is required: '{text}'."}
    ]
    
    response = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=messages)
    role = response.choices[0].message['content'].strip()
    return role

def categorize_lead(role, company):
    messages = [
        {"role": "user", "content": "You are an assistant designed to categorize potential leads based on how best we can add value and appeal to their role and company. If we believe our services in artificial intelligence, especially generative models, can be impactful for the role or company, categorize it as 'GENERATIVE AI'. If our Diversity, Equity, and Inclusion solutions can assist the role or company's objectives, categorize it as 'DEI'. If our expertise in consumer product investigation, competitive analysis, industry trends, marketing efforts, or related fields can benefit the role or company, categorize it as 'CPEA'. If the role or company doesn't seem to fit prominently into any of these categories, categorize it as 'GENERAL'."},
        {"role": "user", "content": f"The role is: '{role}' and the company is: '{company}'. Considering how best we can appeal to and assist this role and company, please categorize this lead. Remember to ONLY return one of the following: GENERAL, GENERATIVE AI, CPEA, or DEI. Do not include any more text or symbols or quotes."}
    ]
    
    response = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=messages)
    category = response.choices[0].message['content'].strip()
    return category

def customized_email_portion(role, company):
    messages = [
        {"role": "user", "content": "You are assisting a boutique consulting firm in tailoring a portion of an outreach email. This firm has expertise in various areas including go-to-market strategy, competitor analysis, and industry/consumer research. The objective is to draw a connection between the firm's services and the recipient's professional role and company. This connection will fit into the middle of the email, following a sentence where the writer has expressed an interest in discussing collaboration based on the recipient's work at their company. Here's the larger context of the email:"},
        {"role": "user", "content": """
        "Dear [Recipient Name],

        I hope this email finds you well. I recently came across your work at {{COMPANY}} and wanted to reach out to discuss a potential collaboration opportunity to support your ongoing projects and initiatives.

        [Your text should fit here seamlessly, making it sound like a continuous flow from the previous sentence.]

        My name is Vasuman Moza, and alongside my colleague Jacob Leroux, I have led and executed several consulting projects for over a dozen Fortune-500 companies over the last 4 years, including Nike, DoorDash, Uber, and more. Our projects have included topics such as go-to-market strategy, competitor analysis, and industry/consumer research.

        We understand that today's economic climate can make people hesitant about investing in new services. To address this concern and demonstrate our commitment to providing value, we'd like to offer you a complimentary, no-obligation consultation. During this initial call, we can discuss your current challenges and share insights on how we can help as a formal proposal.

        We're confident that our tailored solutions can complement your efforts at {{COMPANY}} and contribute to the success of your projects.

        If you're open to exploring the possibility of working together, feel free to reach out to vasumanmoza@berkeley.edu. For a brief overview of our services and testimonials from previous clients, please see the deck attached.

        Thank you for your time, and we look forward to the opportunity to learn more about your work and how we might collaborate.

        Best regards,
        Vasuman Moza"
        """
        },
        {"role": "user", "content": f"Given the recipient's role as a '{role}' at '{company}', craft a tailored segment that naturally establishes a connection between our consulting services and the recipient's role at their company. This segment should flow naturally from the preceding sentence and lead into the introduction of Vasuman Moza. Please ONLY return the exact text that will replace the [Your text should fit here seamlessly...] placeholder in the email. Ensure that it reads naturally and seamlessly within the overall context. Again, only return the text that will be copy pasted over the [Your text], do not actually return [Your Text] or anything like that. "}
    ]

    response = openai.ChatCompletion.create(model="gpt-4", messages=messages)
    custom_content = response.choices[0].message['content'].strip()
    return custom_content


if not values:
    print('No data found.')
else:
    customized_portions = []
    num = 0
    
    # Limit processing to the first 50 rows or the total number of rows, whichever is smaller
    max_rows = min(50, len(values) - 1) # subtract 1 to account for the header row
    
    for row in values[1:max_rows+1]: # Start from index 1 to skip the header
        num += 1
        role_description = row[7]  # Assuming the 8th column is 'Role'
        company_name = row[2]      # Assuming the 3rd column is 'Company'
        
        # Get the customized email portion for the specific role and company
        customized_portions.append([customized_email_portion(role_description, company_name)])
        print("Called API", num, "times")

    # Update your sheet with the generated customized email portions, perhaps in a new column
    update_range = 'K2:K' + str(num + 1)  # Adjusting the update range based on the number of processed rows
    body = {'values': customized_portions}
    result = sheet.values().update(spreadsheetId=SHEET_ID, range=update_range, valueInputOption="RAW", body=body).execute()
    print(f"{result.get('updatedCells')} cells updated with customized email portions.")

    # results_35 = []
    # for row in values:
    #     role_description = row[0]
    #     results_35.append([extract_role(role_description)])

    # # Update column H with GPT-3.5 results
    # update_range_H = 'H2:H51'
    # body_H = {'values': results_35}
    # result_H = sheet.values().update(spreadsheetId=SHEET_ID, range=update_range_H, valueInputOption="RAW", body=body_H).execute()
    # print(f"{result_H.get('updatedCells')} cells updated in column H with GPT-3.5 results.")
