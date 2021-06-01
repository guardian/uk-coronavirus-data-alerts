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

def get_percentage_change(url, metric_name):
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

        average_last_week = area_data_last_week[metric_name].mean()
        average_week_before = area_data_week_before_last[metric_name].mean()

        percentage_change = ((average_last_week - average_week_before) / average_week_before) * 100.0

        ret.append([area_name, average_week_before, average_last_week, percentage_change])

    column_names = [
        'areaName',
        f'{metric_name}-{thirteen_days_before.strftime("%m-%d-%Y")}-to-{seven_days_before.strftime("%m-%d-%Y")}',
        f'{metric_name}-{six_days_before.strftime("%m-%d-%Y")}-to-{latest.strftime("%m-%d-%Y")}',
        'percentageChange'
    ]

    ret_df = pd.DataFrame(ret, columns = column_names)
    return ret_df

def get_areas_above_threshold(area_type, metric_name, threshold):
    url = f"https://api.coronavirus.data.gov.uk/v2/data?areaType={area_type}&metric={metric_name}&format=csv"
    df = get_percentage_change(url, metric_name)

    print(f"{metric_name} data:", file=sys.stderr)
    print(df.to_string(), file=sys.stderr)

    df = df[df.percentageChange.gt(threshold)].sort_values("percentageChange", ascending=False)

    return df

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

def check_last_two_weeks_of_metrics():
    # TODO MRB: put threshold back
    # threshold = 125.0
    threshold = 1.0

    all_data = [
        get_areas_above_threshold("nhsRegion", "newAdmissions", threshold),
        get_areas_above_threshold("ltla", "newCasesBySpecimenDate", threshold),
        get_areas_above_threshold("nhsRegion", "hospitalCases", threshold)
    ]

    to_alert = [f"<p>{df.to_html()}</p>" for df in all_data if len(df) > 0]

    if len(to_alert) > 0:
        body = textwrap.dedent(f"""
            <p>Some metrics have exceeded {threshold}% change week on week:</p>
            {"".join(to_alert)}
            <p>Check https://coronavirus.data.gov.uk/</p>
        """)
        send_notification_email("UK Coronavirus Data Alert", body)
    else:
        print(f"No metric exceeded {threshold}% change", file=sys.stderr)

def lambda_handler(event, lambda_context):
    compare_available_metrics()
    check_last_two_weeks_of_metrics()

if __name__ == "__main__":
    lambda_handler(None, None)