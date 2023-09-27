import json
import boto3
from datetime import datetime, timedelta
import calendar
from apify_client import ApifyClient

# AWS Path
folder_path = "instagram/Account Profiles/"
bucket = "s3-apify-instagram-raw-dta"

# Time range
from_date = "2023-03-01"
pro_time = datetime.today() - timedelta(days=1)
to_date = str(pro_time)

# Weekly Updates - Filename
cur_time = datetime.today()
year = cur_time.year
month_name = calendar.month_abbr[cur_time.month]
week = (cur_time.day - 1) // 7 + 1
formatted_date = "{}-{}-Week{}".format(year, month_name, week)

# Initialize the ApifyClient with your API token


session = boto3.Session(
    aws_access_key_id=aws_id,
    aws_secret_access_key=aws_key
)
s3 = session.client('s3')

competitors_lst = ["lacroixwater", "bublywater", "perriercanada", "monsterenergy", "sanpellegrinoca",
                   "waterloosparkling", "drinkspindrift", "purelifecanada", "montelliercanada"]

all_brands_data = []

for brand in competitors_lst:
    print(f"Get apify profile data from brand {brand}, from {from_date} to {to_date}")

    # Prepare the Actor input for each brand
    run_input = {"usernames": [brand]}

    # Run the Actor and wait for it to finish
    run = client.actor("apify/instagram-profile-scraper").call(run_input=run_input)
    data_variable = client.dataset(run["defaultDatasetId"]).list_items().items

    all_brands_data.extend(data_variable)

# After collecting all the data, prepare it for S3 storage
payload = {
    "data": all_brands_data
}

data_length = str(len(payload['data']))
print(f"Collected a total of {data_length} rows of data from all brands")

# Convert payload to a valid JSON string and store it in S3
fileName = f"all-brands-profile-{from_date}.json"
data = json.dumps(payload).encode('UTF-8')
object_key = folder_path + fileName

s3.put_object(Body=data, Bucket=bucket, Key=object_key)
print(f"File uploaded into s3 {bucket}/{object_key}")
