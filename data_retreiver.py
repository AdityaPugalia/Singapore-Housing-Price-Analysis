
import os
from urllib import response
import requests
from dotenv import set_key, load_dotenv
import traceback  
import csv

load_dotenv()

AUTHENTICATION_URL = 'https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1'
TRANSACTION_URL = 'https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1?service=PMI_Resi_Transaction&batch={batch_number}'
RENTAL_URL = 'https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1?service=PMI_Resi_Rental&refPeriod={ref_period}'
access_key = os.getenv('URA_ACCESS_KEY')

def write_to_file(filename, data, col):
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=col)
        writer.writeheader()
        writer.writerows(data)

def parse_transaction_data(batch_number, json_data):
    filename = f"Data/property_transactions_batch_{batch_number}.csv"
    csv_data = []
    col = set(["project", "marketSegment", "street", "x", "y"])
    for data in json_data:
        for txn in data["transaction"]:
            row = {
                "project": data["project"],
                "marketSegment": data["marketSegment"],
                "street": data["street"],
                "x": data.get("x", ""),
                "y": data.get("y", ""),
                **txn   # unpack transaction fields
            }
            col.update(row.keys())
            csv_data.append(row)

    write_to_file(filename, csv_data, list(col))
    print(f"Data for batch {batch_number} written to {filename}")

def get_access_token():
    headers = {
        'AccessKey' : access_key,
        "User-Agent": "curl/7.64.1",
    }
    try: 
        response = requests.get(AUTHENTICATION_URL, headers=headers)
        print(response.status_code)
        print(response.text)
        if response.json().get('Status') != "Success":
            print("Failed to retrieve access token")
        
        token = response.json().get('Result')
        set_key('.env', 'URA_TOKEN', token)
        print("Access token updated successfully.")
    except requests.RequestException as e:
        print(headers)
        print(f"Error fetching access token: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"Unexpected error: {e}")

def get_transaction_data(batch_number):
    token = os.getenv('URA_TOKEN')
    headers = {
        'AccessKey' : access_key,
        'Token' : token,
        "User-Agent": "curl/7.64.1",
    }
    try: 
        response = requests.get(TRANSACTION_URL.format(batch_number=batch_number), headers=headers)
        print(response.status_code)
        if response.status_code != 200:
            print("Failed to retrieve transaction data")
            return None
        json_data = response.json().get('Result')
        parse_transaction_data(batch_number, json_data)
        print(f"Transaction data for batch {batch_number} fetched successfully.")
    except requests.RequestException as e:
        print(headers)
        print(f"Error fetching transaction data: {e}")
        traceback.print_exc()
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        traceback.print_exc()
        return None
        
def get_rental_data(ref_period):
    token = os.getenv('URA_TOKEN')
    headers = {
        'AccessKey' : access_key,
        'Token' : token,
        "User-Agent": "curl/7.64.1",
    }
    try: 
        response = requests.get(RENTAL_URL.format(ref_period=ref_period), headers=headers)
        print(response.status_code)
        if response.status_code != 200:
            print("Failed to retrieve rental data")
            return None
        json_data = response.json().get('Result')
        print(f"Rental data for reference period {ref_period} fetched successfully.")
        parse_rental_data(ref_period,json_data)
        return
    except requests.RequestException as e:
        print(headers)
        print(f"Error fetching rental data: {e}")
        traceback.print_exc()
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        traceback.print_exc()
        return None

def parse_rental_data(ref_period, json_data):
    filename = f"Data/property_rentals_{ref_period}.csv"
    csv_data = []
    col = set(["project", "street", "x", "y"])
    for data in json_data:
        for txn in data["rental"]:
            row = {
                "project": data["project"],
                "street": data["street"],
                "x": data.get("x", ""),
                "y": data.get("y", ""),
                **txn   # unpack transaction fields
            }
            col.update(row.keys())
            csv_data.append(row)

    write_to_file(filename, csv_data, list(col))
    print(f"Data for rental contracts written to {filename}")
    
if __name__ == '__main__':
    # get_access_token()
    # for batch in range(1, 5):
    #     get_transaction_data(batch)
    for ref_period in ['20Q3', '20Q4', '21Q1', '21Q2', '21Q3', '21Q4', '22Q1', '22Q2', '22Q3', '22Q4', '23Q1', '23Q2', '23Q3', '23Q4', '24Q1', '24Q2', '24Q3', '24Q4', '25Q1', '25Q2', '25Q3']:
        get_rental_data(ref_period)
