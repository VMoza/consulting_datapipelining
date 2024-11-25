import pandas as pd
import os
import time
import warnings
import re
from email.utils import parseaddr

warnings.simplefilter(action='ignore', category = FutureWarning)

MERGE_KEEP_LIST = ['EMAIL_SENT', 'EMAIL_CLICKED', 'EMAIL_OPENED', '', None]
MERGE_DROP_LIST = ['BOUNCED', 'ERROR', '0', 'RESPONDED', 'NO_RECIPIENT', 'UNSUBSCRIBED', 'UNINTERESTED']

GOOD_EMAILS = set()
BAD_EMAILS = set()
COLUMNS_WANTED = ['COMPANY', 'NAME', 'FIRST', 'EMAIL', 'ROLE', 'MERGE']

COMPANY_AVOID_LIST = ['Facebook', 'Y Combinator', 'Instagram', 'Meta', 'Whatsapp', 'Oculus']

def getDataFrameFromExcelFile(file_path):
    return pd.read_excel(pd.ExcelFile(file_path))

def writeDataFrameToExcelFile(df, file_path, sheet_title):
    writer = pd.ExcelWriter(file_path, engine = 'openpyxl', mode = 'w')
    df.to_excel(writer, sheet_name = sheet_title, index = False)
    writer.save()

def addDataFrameToExistingExcelFile(df, file_path, sheet_name):
    if os.path.exists(file_path):
        writer = pd.ExcelWriter(file_path, engine = 'openpyxl', mode = 'a', if_sheet_exists = 'replace')
        df.to_excel(writer, sheet_name = sheet_name, index = False)
        writer.save()

def writeListOfDataFramesToExcelFile(list_of_dfs, file_path):
    for n, df in enumerate(list_of_dfs):
        most_common_company = df['COMPANY'].mode()[0]
        if n % 2 == 0:
            founder = 'VAS'
        else:
            founder = 'JAKE'
        sheet_name = founder + ' ' + most_common_company + ' ' + str(n)
        if n == 0: 
            writeDataFrameToExcelFile(df, file_path, sheet_name)
        else: 
            addDataFrameToExistingExcelFile(df, file_path, sheet_name)
    print("Created " + file_path + " with " + str(len(list_of_dfs)) + " sheets.")

def addListOfDataFramesToExistingExcelFile(list_of_dfs, file_path):
    for n, df in enumerate(list_of_dfs):
        sheet_name = str(n) + " ADDITION"
        addDataFrameToExistingExcelFile(df, file_path, sheet_name)
    print("Added to " + file_path + " with " + str(len(list_of_dfs)) + " sheets")

def getGoodAndBadEmailsList(list_of_files, do_not_email_file):
    global GOOD_EMAILS, BAD_EMAILS
    dne_df = getDataFrameFromExcelFile(do_not_email_file)
    BAD_EMAILS.update(set(dne_df['EMAIL']))
    for file in list_of_files:
        print(file)
        list_of_dfs = getListOfDataFramesFromExcelFile(file)
        for df in list_of_dfs:
            df.columns = df.columns.str.replace(' ', '')
            df.columns = df.columns.str.upper()
            if 'MERGE' in df.columns:
                df = df.rename({'MERGE' : 'MERGESTATUS'})
            if 'MERGESTATUS' in df.columns:
                keep_emails_df = df[df.MERGESTATUS.isin(MERGE_DROP_LIST) == False]
                bounced_emails_df = df[df.MERGESTATUS.isin(MERGE_DROP_LIST) == True]
                keep_emails, bounced_emails = keep_emails_df['EMAIL'], bounced_emails_df['EMAIL']
            else:
                continue
            GOOD_EMAILS.update(set(keep_emails))
            BAD_EMAILS.update(set(bounced_emails))

def populateGoodAndBadEmailsList(list_of_files, do_not_email_file):
    getGoodAndBadEmailsList(list_of_files, do_not_email_file)
    dne_df = pd.DataFrame(list(BAD_EMAILS), columns=['EMAIL'])
    writeDataFrameToExcelFile(dne_df, do_not_email_file, "DNE")

def getListOfDataFramesFromExcelFile(file_path):
    file = pd.ExcelFile(file_path)
    list_of_sheets = pd.read_excel(file, file.sheet_names)
    list_of_dfs = list_of_sheets.values()
    return list_of_dfs

def joinDataFrames(list_of_dfs):
    df = pd.concat(list_of_dfs, ignore_index = True)
    return df

def cleanDataFrame(df):
    df.columns = df.columns.str.replace(' ', '')
    df.columns = df.columns.str.upper()

    global GOOD_EMAILS, BAD_EMAILS

    if 'MERGE' in df.columns:
        df = df.rename({'MERGE' : 'MERGESTATUS'})
    if 'MERGESTATUS' in df.columns:
        bounced_emails_df = df[df.MERGESTATUS.isin(MERGE_DROP_LIST) == True]
        bounced_emails = bounced_emails_df['EMAIL']
        BAD_EMAILS.update(set(bounced_emails))
        df = df.copy()
        df = df[df.MERGESTATUS.isin(MERGE_DROP_LIST) == False]

    df = df.copy()
    df = df[df.COMPANY.isin(COMPANY_AVOID_LIST) == False]

    df = df.copy()
    df.drop_duplicates(subset = 'EMAIL', keep = 'first', inplace = True)
    df.drop_duplicates(subset = ['COMPANY', 'NAME'], keep = 'first', inplace = True)

    mask = df['EMAIL'].apply(validate_email)
    df = df[mask]

    df = df.copy()
    df = df[df.EMAIL.isin(BAD_EMAILS) == False]
    # df = df[df.EMAIL.isin(GOOD_EMAILS) == False]

    GOOD_EMAILS.update(set(df['EMAIL'])) 
    
    df = df.sort_values(by = ['COMPANY'], ascending = True)
    return df

def cleanListOfDataFrames(list_of_dfs):
    cleaned_dfs = []
    for df in list_of_dfs:
        cleaned_dfs.append(cleanDataFrame(df))
    return cleaned_dfs

def splitDataFrame(df, split_level):
    df = df.sort_values(by = ['COMPANY'], ascending = True)
    split_dfs = [df.iloc[n:n + split_level, :] for n in range(0, len(df), split_level)]
    return split_dfs

def keepColumns(df):
    return df[COLUMNS_WANTED]   

def prettyPrint(list_of_stuff):
    for i in list_of_stuff:
        print(i)
    print("\nTotal entries in " + str(type(list_of_stuff))  + ": " + str(len(list_of_stuff)))

def getOneBigDataFrameFromExcelFile(file_path):
    list_of_dfs = getListOfDataFramesFromExcelFile(file_path)
    master_df = joinDataFrames(list_of_dfs)
    return master_df

def DEBUG_PLAN(contacts_file_path, base_file_path):
    populateGoodAndBadEmailsList([contacts_file_path], 'DO NOT EMAIL.xlsx')

    list_of_dfs = getListOfDataFramesFromExcelFile(contacts_file_path)
    writeListOfDataFramesToExcelFile(list_of_dfs, 'STEP1.xlsx')
    countEntriesInExcelFile('STEP1.xlsx')
    print("STEP 1 SUCCESS: REPLICATED SOURCING DOCUMENT.\n")

    cleaned_list_of_dfs = cleanListOfDataFrames(list_of_dfs)
    writeListOfDataFramesToExcelFile(cleaned_list_of_dfs, 'STEP2.xlsx')
    countEntriesInExcelFile('STEP2.xlsx')
    print("STEP 2 SUCCESS: CLEANED SOURCING DOCUMENT.\n")

    master_df = joinDataFrames(cleaned_list_of_dfs)
    writeDataFrameToExcelFile(master_df, 'STEP3.xlsx', 'STEP3')
    countEntriesInExcelFile('STEP3.xlsx')
    print("STEP 3 SUCCESS: MERGED CLEANED DOCUMENT INTO MASTER SHEET.\n")

    split_master_df = splitDataFrame(master_df, 1400)
    writeListOfDataFramesToExcelFile(split_master_df, base_file_path)
    countEntriesInExcelFile(base_file_path)
    print("STEP 4 SUCCESS: SPLIT CLEANED MASTER INTO SHEETS OF 1400.\n")

def createNewBase(contacts_file_path, base_file_path):
    populateGoodAndBadEmailsList([contacts_file_path], 'DO NOT EMAIL.xlsx')

    list_of_dfs = getListOfDataFramesFromExcelFile(contacts_file_path)

    cleaned_list_of_dfs = cleanListOfDataFrames(list_of_dfs)

    master_df = joinDataFrames(cleaned_list_of_dfs)

    split_master_df = splitDataFrame(master_df, 1400)
    writeListOfDataFramesToExcelFile(split_master_df, base_file_path)

def countNumberOfSheetsInExcelFile(file_path):
    file = pd.ExcelFile(file_path)
    return len(file.sheet_names)

def countEntriesInExcelFile(file_path):
    master_df = getOneBigDataFrameFromExcelFile(file_path)
    print(str(len(master_df)) + " entries in " + file_path)
    # return len(master_df)

def countEntriesAndSheetsInExcelFile(file_path):
    master_df = getOneBigDataFrameFromExcelFile(file_path)
    print(str(len(master_df)) + " entries and " + str(countNumberOfSheetsInExcelFile(file_path)) + " sheets in " + file_path)
    return len(master_df)

def addToBase(base_file_path, to_add_file_path):
    populateGoodAndBadEmailsList([base_file_path], 'DO NOT EMAIL.xlsx')
    list_of_dfs = getListOfDataFramesFromExcelFile(to_add_file_path)
    cleaned_list_of_dfs = cleanListOfDataFrames(list_of_dfs)
    master_df = joinDataFrames(cleaned_list_of_dfs)
    split_master_df = splitDataFrame(master_df, 1400)
    writeListOfDataFramesToExcelFile(split_master_df, "2023 Email List With Append.xlsx")

def filterColumns(df):
    columns_to_keep = ['COMPANY', 'NAME', 'FIRST', 'EMAIL']
    filtered_df = df.filter(columns_to_keep)
    return filtered_df

def validate_email(email):
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    if re.match(email_regex, email):
        return True
    else:
        return False

def checkInBoth(left_df, right_df):
    result = pd.merge(left_df, right_df, on = 'EMAIL', how = 'outer', indicator = True)
    result2 = result.loc[result['_merge'] == 'both', ['EMAIL']]
    return result2

def checkOnlyInLeft(left_df, right_df):
    result = pd.merge(left_df, right_df, on = 'EMAIL', how = 'outer', indicator = True)
    result2 = result.loc[result['_merge'] == 'left_only', ['EMAIL']]
    return result2

def checkOnlyInRight(left_df, right_df):
    return checkOnlyInLeft(right_df, left_df)

def updateBouncesPostSend(file_path):
    return 0

def uploadToGoogleDrive():
    return 0

def pullFromGoogleDrive():
    return 0

def emailsThatBouncedFromDataFrame(df):
    return 0

def generateFollowUpSheet(file_path):
    populateGoodAndBadEmailsList([file_path], 'DO NOT EMAIL.xlsx')
    list_of_dfs = getListOfDataFramesFromExcelFile(file_path)
    cleaned_list_of_dfs = cleanListOfDataFrames(list_of_dfs)
    master_df = joinDataFrames(cleaned_list_of_dfs)
    filtered_df = filterColumns(master_df)
    split_up_df = splitDataFrame(filtered_df, 1490)
    writeListOfDataFramesToExcelFile(split_up_df, '2023 Email List FOLLOWUP.xlsx')
    print(countEntriesAndSheetsInExcelFile('2023 Email List FOLLOWUP.xlsx'))

# generateFollowUpSheet('2023 Email List.xlsx')
countEntriesAndSheetsInExcelFile('DO NOT EMAIL.xlsx')