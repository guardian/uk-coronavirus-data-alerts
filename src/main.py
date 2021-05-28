from dataclasses import dataclass
import os
import sys
import boto3
import json
import pandas as pd
import requests
import textwrap

SITE_BASE = "https://coronavirus.data.gov.uk"
V1_API_BASE = SITE_BASE + "/api/v1"
V2_API_BASE = SITE_BASE + "/api/v2"

AWS_REGION = "eu-west-1"
METRICS_BUCKET = "investigations-data-dev"
METRICS_KEY = "uk-coronavirus-data-alerts/metrics.json"

NOTIFY_EMAILS = ["michael.barton@guardian.co.uk"]

if "AWS_EXECUTION_ENV" in os.environ:
    # Use the default credentials chain when running in AWS
    boto_session = boto3.session.Session()
else:
    boto_session = boto3.session.Session(profile_name = "investigations")

s3_client = boto_session.client("s3", region_name = AWS_REGION)
ses_client = boto_session.client("ses", region_name = AWS_REGION)

def get_current_metric_definitions():
    url = SITE_BASE + "/public/assets/dispatch/api_variables.json"

    sys.stderr.write(f"GET {url}... ")
    resp = requests.get(url).json()
    print(f"Done", file=sys.stderr)

    return resp

def get_previous_metric_definitions():
    sys.stderr.write(f"Downloading metrics from s3://${METRICS_BUCKET}/{METRICS_KEY}... ")
    response = s3_client.get_object(Bucket = METRICS_BUCKET, Key = METRICS_KEY)
    print(f"Done", file=sys.stderr)

    return json.load(response['Body'])

def save_metric_definitions(metric_names):
    body = json.dumps(metric_names)

    sys.stderr.write(f"Uploading metrics to s3://${METRICS_BUCKET}/${METRICS_KEY}... ")
    s3_client.put_object(Body=body, Bucket = METRICS_BUCKET, Key = METRICS_KEY)
    print(f"Done", file=sys.stderr)

def get_average_hospital_admissions_by_region():
    """
                        areaName  averageAdmissionsLastWeek  averageAdmissionsWeekBefore  percentageChange
    0                    London                  12.571429                    13.857143         90.721649
    1                South East                   6.571429                     8.142857         80.701754
    2                South West                   6.142857                     5.142857        119.444444
    3           East of England                   8.285714                    10.714286         77.333333
    4                  Midlands                  18.571429                    21.857143         84.967320
    5  North East and Yorkshire                  17.571429                    19.285714         91.111111
    6                North West                  14.714286                    13.428571        109.574468
    """
    url = "https://api.coronavirus.data.gov.uk/v2/data?areaType=nhsRegion&metric=newAdmissions&format=csv"
    df = pd.read_csv(url)

    df['date'] = pd.to_datetime(df['date'])

    area_names = df.areaName.unique()
    ret = []

    for area_name in area_names:
        area_data = df[df.areaName.eq(area_name)]
        
        latest = df['date'].max()

        # We want 7 days inclusive of the latest
        six_days_before = latest - pd.Timedelta(days=6)
        seven_days_before = latest - pd.Timedelta(days=7)
        # As above
        thirteen_days_before = latest - pd.Timedelta(days=13)

        area_data_last_week = area_data[area_data.date.ge(six_days_before)]
        area_data_week_before_last = area_data[area_data.date.ge(thirteen_days_before) & area_data.date.le(seven_days_before)]

        average_admissions_last_week = area_data_last_week['newAdmissions'].mean()
        average_admissions_week_before = area_data_week_before_last['newAdmissions'].mean()

        percentage_change = ((average_admissions_last_week - average_admissions_week_before) / average_admissions_week_before) * 100.0

        ret.append([area_name, average_admissions_week_before, average_admissions_last_week, percentage_change])

    column_names = [
        'areaName',
        f'averageAdmissions-{thirteen_days_before.strftime("%m-%d-%Y")}-to-{seven_days_before.strftime("%m-%d-%Y")}',
        f'averageAdmissions-{six_days_before.strftime("%m-%d-%Y")}-to-{latest.strftime("%m-%d-%Y")}',        
        'percentageChange'
    ]

    ret_df = pd.DataFrame(ret, columns = column_names)
    return ret_df

def send_notification_email(subject, body):
    print(f"Email {', '.join(NOTIFY_EMAILS)}. Subject: {subject}. Body: {body}", file=sys.stderr)

    ses_client.send_email(
        Source = "investigations.and.reporting@theguardian.com",
        Destination = {
            "ToAddresses": NOTIFY_EMAILS
        },
        Message = {
            "Subject": {
                "Data": subject
            },
            "Body": {
                "Html": {
                    "Data": body
                }
            }
        }
    )

    print(f"Email sent succesfully", file=sys.stderr)

def compare_available_metrics():
    current = get_current_metric_definitions()
    previous = {}
    
    try:
        previous = get_previous_metric_definitions()
    except Exception as e:
        print("Error getting previous metrics", e)
        print("Continuing with empty previous metrics set")

    new_metric_names = set(current.keys()) - set(previous.keys())
    new_metrics_list = "".join([f"<li>{metric}</li>" for metric in new_metric_names])

    if len(new_metric_names) > 0:
        body = textwrap.dedent(f"""
            <p>
                New metrics available:
                <ul>
                    {new_metrics_list}
                </ul>
            </p>
            <p>
                Check https://coronavirus.data.gov.uk/
            </p>
        """)

        send_notification_email("[UK Coronavirus Data Alert] New metrics available", body)
    else:
        print(f"No new metrics found", file=sys.stderr)

    save_metric_definitions(current)

def check_hospital_admissions():
    df = get_average_hospital_admissions_by_region()
    print("All regions:", file=sys.stderr)
    print(df, file=sys.stderr)

    # TODO MRB: put threshold back
    # threshold = 125.0
    threshold = 1.0

    to_alert = df[df.percentageChange.gt(threshold)].sort_values("percentageChange", ascending=False)

    if len(to_alert) > 0:
        body = textwrap.dedent(f"""
            <p>Some regions have exceeded {threshold}% change week on week for hospital admissions:</p>
            <p>{to_alert.to_html()}</p>
            <p>Check https://coronavirus.data.gov.uk/</p>
        """)
        send_notification_email("[UK Coronavirus Data Alert] Hospital admissions alert", body)
    else:
        print(f"No region exceeded percentage change threshold {threshold}%", file=sys.stderr)

def lambda_handler(event, lambda_context):
    compare_available_metrics()
    check_hospital_admissions()

if __name__ == "__main__":
    lambda_handler(None, None)