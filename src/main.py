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
CASES_PER_100000_POPULATION_THRESHOLD = 100.0

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


def get_ltla_populations():
    url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates" \
          "/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland" \
          "/mid2019april2019localauthoritydistrictcodes/ukmidyearestimates20192019ladcodes.xls"
    resp = requests.get(url)
    df = pd.read_excel(io=resp.content, sheet_name="MYE2 - Persons", header=4, usecols="A,B,D", index_col=0)

    return df

def get_nhs_regions_populations():
    # TODO: find latest (published weekly, work back from today)
    url ="https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/06/COVID-19-weekly-announced-vaccinations-24-June-2021.xlsx"
    resp = requests.get(url)
    df = pd.read_excel(io=resp.content, sheet_name="Population estimates (ONS)", header=12, usecols="B,D,R", index_col=0, nrows=9)

    return df


def population_for_area(populations_df, area_name, area_code):
    try:
        area_population = populations_df.at[area_code, "All ages"]
        return area_population
    except KeyError as e:
        print(f"Error getting population for area '{area_name}', area code {area_code} not found.", e, file=sys.stderr)


def get_cases_per_100000(cases, populations_df, metric_df, area_name):
    area_code = area_code_for_area(metric_df, area_name)
    try:
        area_population = population_for_area(populations_df, area_name, area_code)
        return (cases / area_population) * 100000
    except TypeError as e:
        print(f"Cannot calculate population for area ${area_name}", e, file=sys.stderr)


def convert_nhs_region_key(nhs_region_key):
    if nhs_region_key == "East of England":
        return "East Of England"
    elif nhs_region_key == "North East and Yorkshire":
        return "North East And Yorkshire"
    else:
        return nhs_region_key

def get_metric_per_100000_nhs_region(metric, nhs_populations_df, nhs_region_name):
    nhs_region = convert_nhs_region_key(nhs_region_name)
    try:
        region_population = nhs_populations_df.at[nhs_region, "Under 16"] + nhs_populations_df.at[nhs_region, "16+"]
        return (metric / region_population) * 100000
    except TypeError as e:
        print(f"Cannot calculate population for region ${nhs_region_name}")


# We want 7 days inclusive of the latest
def dates_from_latest(latestDate):
    return {
        "latest": latestDate,
        "six_days_before": latestDate - pd.Timedelta(days=6),
        "seven_days_before": latestDate - pd.Timedelta(days=7),
        "thirteen_days_before": latestDate - pd.Timedelta(days=13)
    }

def date_dependent_column_names(metric_name, dates):
    return {
        "week_before": f'{metric_name}-{dates["thirteen_days_before"].strftime("%m-%d-%Y")}-to-{dates["seven_days_before"].strftime("%m-%d-%Y")}',
        "last_week": f'{metric_name}-{dates["six_days_before"].strftime("%m-%d-%Y")}-to-{dates["latest"].strftime("%m-%d-%Y")}'
    }


def get_percentage_change(area_name, metric_name, metric_df, aggregation_function):
    area_data = metric_df[metric_df.areaName.eq(area_name)]

    latest = metric_df['date'].max()
    dates = dates_from_latest(latest)

    area_data_last_week = area_data[area_data.date.ge(dates["six_days_before"])]
    area_data_week_before_last = area_data[area_data.date.ge(dates["thirteen_days_before"])
                                           & area_data.date.le(dates["seven_days_before"])]

    aggregation_output_last_week = getattr(area_data_last_week[metric_name], aggregation_function)()
    aggregation_output_week_before = getattr(area_data_week_before_last[metric_name], aggregation_function)()

    percentage_change = ((aggregation_output_last_week - aggregation_output_week_before) / aggregation_output_week_before) * 100.0

    return {
        "aggregation_output_week_before": aggregation_output_week_before,
        "aggregation_output_last_week": aggregation_output_last_week,
        "percentage_change": percentage_change,
        "date_dependent_column_names": date_dependent_column_names(metric_name, dates)
    }


def percentage_changes(url, metric_name, aggregation_function):
    metric_df = pd.read_csv(url)
    metric_df['date'] = pd.to_datetime(metric_df['date'])
    area_names = metric_df.areaName.unique()
    ret = []

    ltla_populations_df = get_ltla_populations()
    nhs_region_populations_df = get_nhs_regions_populations()

    for area_name in area_names:
        percentage_change_stats = get_percentage_change(area_name, metric_name, metric_df, aggregation_function)

        per_100000_stats = get_cases_per_100000(percentage_change_stats["aggregation_output_last_week"],
                                                ltla_populations_df,
                                                metric_df,
                                                area_name,
                                                ) if metric_name == "newCasesBySpecimenDate" \
            else get_metric_per_100000_nhs_region(percentage_change_stats["aggregation_output_last_week"],
                                                  nhs_region_populations_df,
                                                  area_name)

        ret.append([
            area_name,
            percentage_change_stats["aggregation_output_week_before"],
            percentage_change_stats["aggregation_output_last_week"],
            percentage_change_stats["percentage_change"],
            per_100000_stats
        ])
        column_names = [
            "areaName",
            percentage_change_stats["date_dependent_column_names"]["week_before"],
            percentage_change_stats["date_dependent_column_names"]["last_week"],
            "percentageChange",
            "lastSevenDaysPer100000"
        ]

        ret_df = pd.DataFrame(ret, columns=column_names)
    return ret_df


def area_code_for_area(metric_df, area_name):
    area_codes = metric_df[metric_df.areaName.eq(area_name)].areaCode.unique()
    if area_codes.size != 1:
        raise Exception(f"Unexpected area_codes found: {area_codes} for area_name: {area_name}")
    else:
        return area_codes[0]


def get_areas_above_thresholds(area_type, metric_name, thresholds, aggregation_function):
    url = f"https://api.coronavirus.data.gov.uk/v2/data?areaType={area_type}&metric={metric_name}&format=csv"
    df = percentage_changes(url, metric_name, aggregation_function)

    print(f"{metric_name} data:", file=sys.stderr)
    print(df.to_string(), file=sys.stderr)

    if metric_name == "newCasesBySpecimenDate":
        df = df[
            df.percentageChange.gt(thresholds["percentage_change_threshold"]) & df.lastSevenDaysPer100000.gt(
                thresholds["metric_value_per_100000_threshold"])
            ].sort_values("percentageChange", ascending=False)
        return df
    else:
        df = df[
            df.percentageChange.gt(thresholds["percentage_change_threshold"])
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

        print(f"Email sent successfully", file=sys.stderr)


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
    cases_threshold = CASES_PER_100000_POPULATION_THRESHOLD

    percentage_change_thresholds = {"percentage_change_threshold": percentage_change_threshold}
    cases_thresholds = {"percentage_change_threshold": percentage_change_threshold, "metric_value_per_100000_threshold": cases_threshold}

    all_data = [
        get_areas_above_thresholds("nhsRegion", "newAdmissions", percentage_change_thresholds, 'mean'),
        get_areas_above_thresholds("ltla", "newCasesBySpecimenDate", cases_thresholds, 'sum'),
        get_areas_above_thresholds("nhsRegion", "hospitalCases", percentage_change_thresholds, 'mean')
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
