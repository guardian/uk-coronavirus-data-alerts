from dataclasses import dataclass
import os
import sys
import boto3
import json
import pandas as pd
import requests
import textwrap

AWS_REGION = "eu-west-1"
METRICS_BUCKET = "investigations-data-dev"
METRICS_KEY = "uk-coronavirus-data-alerts/metrics.json"

PERCENTAGE_CHANGE_THRESHOLD = 100.0
CASES_PER_100000_POPULATION_THRESHOLD = 50.0
HOSPITAL_CASES_PER_100000_POPULATION_THRESHOLD = 0.0
ADMISSIONS_PER_100000_POPULATION_THRESHOLD = 0.0

NOTIFY_EMAILS = os.environ.get("NOTIFY_EMAIL_ADDRESSES")
if NOTIFY_EMAILS is None:
    NOTIFY_EMAILS = []
else:
    NOTIFY_EMAILS = [email.strip() for email in NOTIFY_EMAILS.split(",")]

if "AWS_EXECUTION_ENV" in os.environ:
    # Use the default credentials chain when running in AWS
    boto_session = boto3.session.Session()
else:
    boto_session = boto3.session.Session(profile_name = "investigations")

s3_client = boto_session.client("s3", region_name = AWS_REGION)
ses_client = boto_session.client("ses", region_name = AWS_REGION)


def get_current_metric_definitions():
    url = "https://coronavirus.data.gov.uk/public/assets/dispatch/api_variables.json"

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
    s3_client.put_object(Body = body, Bucket = METRICS_BUCKET, Key = METRICS_KEY)
    print(f"Done", file=sys.stderr)


def get_populations():
    url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates" \
          "/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland" \
          "/mid2019april2019localauthoritydistrictcodes/ukmidyearestimates20192019ladcodes.xls"
    resp = requests.get(url)
    df = pd.read_excel(io=resp.content, sheet_name="MYE2 - Persons", header=4, usecols="A,B,D", index_col=0)

    return df


def population_for_area(populations_df, area_name, area_code):
    try:
        area_population = populations_df.at[area_code, "All ages"]
        return area_population
    except KeyError:
        print(f"Error getting population for area '{area_name}', area code {area_code} not found.", file=sys.stderr)


def get_metric_value_per_100000(metric_value, area_name, area_code):
    populations_df = get_populations()
    area_population = population_for_area(populations_df, area_name, area_code)
    return (metric_value / area_population) * 100000


def get_stats(url, metric_name, aggregation_function):
    metric_df = pd.read_csv(url)
    metric_df['date'] = pd.to_datetime(metric_df['date'])

    area_names = metric_df.areaName.unique()
    ret = []

    for area_name in area_names:
        area_data = metric_df[metric_df.areaName.eq(area_name)]

        latest = metric_df['date'].max()

        # We want 7 days inclusive of the latest
        six_days_before = latest - pd.Timedelta(days=6)
        seven_days_before = latest - pd.Timedelta(days=7)
        # As above
        thirteen_days_before = latest - pd.Timedelta(days=13)

        area_data_last_week = area_data[area_data.date.ge(six_days_before)]
        area_data_week_before_last = area_data[area_data.date.ge(thirteen_days_before) & area_data.date.le(seven_days_before)]

        aggregation_output_last_week = getattr(area_data_last_week[metric_name], aggregation_function)()
        aggregation_output_week_before = getattr(area_data_week_before_last[metric_name], aggregation_function)()

        area_code = metric_df[metric_df.areaName.eq(area_name)].areaCode.unique()[0]
        per_100000_population = get_metric_value_per_100000(aggregation_output_last_week, area_name, area_code)
        percentage_change = ((aggregation_output_last_week - aggregation_output_week_before) / aggregation_output_week_before) * 100.0

        ret.append([
            area_name,
            aggregation_output_week_before,
            aggregation_output_last_week,
            per_100000_population,
            percentage_change,
        ])

    column_names = [
        'areaName',
        f'{metric_name}-{thirteen_days_before.strftime("%m-%d-%Y")}-to-{seven_days_before.strftime("%m-%d-%Y")}',
        f'{metric_name}-{six_days_before.strftime("%m-%d-%Y")}-to-{latest.strftime("%m-%d-%Y")}',
        f'lastSevenDaysPer100000',
        'percentageChange',
    ]

    ret_df = pd.DataFrame(ret, columns = column_names)
    return ret_df


def get_areas_above_thresholds(area_type, metric_name, percentage_change_threshold, metric_value_per_100000_threshold,
                               aggregation_function):
    url = f"https://api.coronavirus.data.gov.uk/v2/data?areaType={area_type}&metric={metric_name}&format=csv"
    df = get_stats(url, metric_name, aggregation_function)

    print(f"{metric_name} data:", file=sys.stderr)
    print(df.to_string(), file=sys.stderr)

    df = df[
        df.percentageChange.gt(percentage_change_threshold) & df.lastSevenDaysPer100000.gt(metric_value_per_100000_threshold)
        ].sort_values("percentageChange", ascending=False)

    return df


def send_notification_email(subject, body):
    if len(NOTIFY_EMAILS) == 0:
        print(f"No email addresses configured. Not sending an email but if I did it would look like this:", file=sys.stderr)
        print(f"Subject: {subject}. Body: {body}", file=sys.stderr)
    else:
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
    percentage_change_threshold = PERCENTAGE_CHANGE_THRESHOLD
    admissions_threshold = ADMISSIONS_PER_100000_POPULATION_THRESHOLD
    cases_threshold = CASES_PER_100000_POPULATION_THRESHOLD
    hospital_cases_threshold = HOSPITAL_CASES_PER_100000_POPULATION_THRESHOLD

    all_data = [
        get_areas_above_thresholds("nhsRegion", "newAdmissions", percentage_change_threshold, admissions_threshold, 'mean'),
        get_areas_above_thresholds("ltla", "newCasesBySpecimenDate", percentage_change_threshold, cases_threshold, 'sum'),
        get_areas_above_thresholds("nhsRegion", "hospitalCases", percentage_change_threshold, hospital_cases_threshold, 'mean')
    ]

    to_alert = [f"<p>{df.to_html()}</p>" for df in all_data if len(df) > 0]

    if len(to_alert) > 0:
        body = textwrap.dedent(f"""
            <p>Some metrics have exceeded {percentage_change_threshold}% change week on week:</p>
            {"".join(to_alert)}
            <p>Check https://coronavirus.data.gov.uk/</p>
        """)
        send_notification_email("UK Coronavirus Data Alert", body)
    else:
        print(f"No metric exceeded {percentage_change_threshold}% change", file=sys.stderr)


def lambda_handler(event, lambda_context):
    compare_available_metrics()
    check_last_two_weeks_of_metrics()


if __name__ == "__main__":
    lambda_handler(None, None)
