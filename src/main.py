import os
import sys
import boto3
import json
import pandas as pd
import requests
import textwrap
from enum import Enum

AWS_REGION = "eu-west-1"
METRICS_BUCKET = "investigations-data-dev"
METRICS_KEY = "uk-coronavirus-data-alerts/metrics.json"

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

class EmailTypes(Enum):
    VERIFIED = "VERIFIED"
    UNVERIFIED = "UNVERIFIED"

EMAIL_TYPE_STR = os.environ.get("EMAIL_TYPE")
EMAIL_TYPE = EmailTypes(EMAIL_TYPE_STR) if EMAIL_TYPE_STR in EmailTypes.__members__ else EmailTypes.VERIFIED

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
    url = "https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fpopulationandmigration%2fpopulationestimates" \
          "%2fdatasets%2fpopulationestimatesforukenglandandwalesscotlandandnorthernireland%2fmid2020/" \
          "ukpopestimatesmid2020on2021geography.xls"
    resp = requests.get(url)
    df = pd.read_excel(io=resp.content, sheet_name="MYE2 - Persons", header=7, usecols="A,B,D", index_col=0)

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
        print(f"Cannot calculate population for area {area_name}", e, file=sys.stderr)

# We want 7 days inclusive of the latest
def dates_from_upper_bound(upper_bound):
    return {
        "upper_bound": upper_bound,
        "six_days_before": upper_bound - pd.Timedelta(days=6),
        "seven_days_before": upper_bound - pd.Timedelta(days=7),
        "thirteen_days_before": upper_bound - pd.Timedelta(days=13)
    }

def date_dependent_column_names(metric_name, dates):
    return {
        "week_before": f'{metric_name}-{dates["thirteen_days_before"].strftime("%d-%m-%Y")}-to-{dates["seven_days_before"].strftime("%d-%m-%Y")}',
        "last_week": f'{metric_name}-{dates["six_days_before"].strftime("%d-%m-%Y")}-to-{dates["upper_bound"].strftime("%d-%m-%Y")}'
    }


def get_percentage_change(area_name, metric_name, metric_df, aggregation_function):
    area_data = metric_df[metric_df.areaName.eq(area_name)]

    max_report_date = metric_df['date'].max()
    """
    From https://coronavirus.data.gov.uk/: "Data shown are cases by specimen date and because these are 
    incomplete for the most recent dates, the period represented is the 7 days ending 5 days before the date 
    when the website was last updated."
    VERIFIED - up to five days before most recent stats
    UNVERIFIED - up to date of most recent stats
    """
    upper_bound = max_report_date - pd.Timedelta(days=4) if EMAIL_TYPE is EmailTypes.VERIFIED else max_report_date

    dates = dates_from_upper_bound(upper_bound)


    area_data_last_week = area_data[area_data.date.ge(dates["six_days_before"])
                                            & area_data.date.le(dates["upper_bound"])]
    area_data_week_before_last = area_data[area_data.date.ge(dates["thirteen_days_before"])
                                           & area_data.date.le(dates["seven_days_before"])]

    aggregation_output_last_week = getattr(area_data_last_week[metric_name], aggregation_function)()
    aggregation_output_week_before = getattr(area_data_week_before_last[metric_name], aggregation_function)()

    if aggregation_output_week_before == 0 and aggregation_output_last_week == 0:
        percentage_change = 0
    elif aggregation_output_week_before == 0:
        percentage_change = float("inf")
    else:
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

    # TODO move outside of this function
    ltla_populations_df = get_ltla_populations()

    for area_name in area_names:
        percentage_change_stats = get_percentage_change(area_name, metric_name, metric_df, aggregation_function)

        row = [
            area_name,
            percentage_change_stats["aggregation_output_week_before"],
            percentage_change_stats["aggregation_output_last_week"],
            round(percentage_change_stats["percentage_change"], 1),
        ]
        

        if metric_name == "newCasesBySpecimenDate":
            per_100000_stats = get_cases_per_100000(percentage_change_stats["aggregation_output_last_week"],
                                                ltla_populations_df,
                                                metric_df,
                                                area_name,
                                                )

            row.append(round(per_100000_stats, 1) if per_100000_stats is not None else None)

            # Values in this column are filtered by index later! If these columns are changed 
            # then get_areas_above_thresholds will have to be updated.
            column_names = [
                "areaName",
                percentage_change_stats["date_dependent_column_names"]["week_before"],
                percentage_change_stats["date_dependent_column_names"]["last_week"],
                "percentageChange",
                "lastSevenDaysPer100000"
            ]

        else: 
            # We don't have a reliable source for NHS Trust populations, so can't calculate
            # stats per 100,000.
            per_100000_stats = None

            # Since we only have population data for ltla regions and not NHS trusts omit the last column.
            column_names = [
                "areaName",
                percentage_change_stats["date_dependent_column_names"]["week_before"],
                percentage_change_stats["date_dependent_column_names"]["last_week"],
                "percentageChange",
            ]  

        ret.append(row)  

        ret_df = pd.DataFrame(ret, columns=column_names)
    return ret_df


def area_code_for_area(metric_df, area_name):
    area_codes = metric_df[metric_df.areaName.eq(area_name)].areaCode.unique()
    if area_codes.size != 1:
        raise Exception(f"Unexpected area_codes found: {area_codes} for area_name: {area_name}")
    else:
        return area_codes[0]


def get_areas_above_thresholds(area_type, metric_name, aggregation_function):
    thresholds = {"percentage_change_threshold": 50.0, "new_cases_per_100000_threshold": 50.0, "hospital_cases_threshold": 30}
    url = f"https://api.coronavirus.data.gov.uk/v2/data?areaType={area_type}&metric={metric_name}&format=csv"
    df = percentage_changes(url, metric_name, aggregation_function)

    print(f"{metric_name} data:", file=sys.stderr)
    print(df.to_string(), file=sys.stderr)

    if metric_name == "newCasesBySpecimenDate":
        # TODO: if we can't find population data for a region, ignore new_cases_per_100000_threshold
        df = df[
            df.percentageChange.gt(thresholds["percentage_change_threshold"]) & df.lastSevenDaysPer100000.gt(
                thresholds["new_cases_per_100000_threshold"])
            ].sort_values("percentageChange", ascending=False)
    else:
        df = df[
            df.percentageChange.gt(thresholds["percentage_change_threshold"]) 
            # accessing third column by index since its name changes every day
            & df[df.columns[2]].gt(thresholds["hospital_cases_threshold"])
        ].sort_values("percentageChange", ascending=False)

    print(f"{metric_name} data after filtering + sorting:", file=sys.stderr)
    print(df.to_string(), file=sys.stderr)
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
    all_data = [
        get_areas_above_thresholds("ltla", "newCasesBySpecimenDate", 'sum'),
        get_areas_above_thresholds("nhsTrust", "hospitalCases", 'mean')
    ]

    to_alert = [f"<p>{df.to_html()}</p>" for df in all_data if len(df) > 0]

    if len(to_alert) > 0:
        if EMAIL_TYPE is EmailTypes.VERIFIED:
            subject = "[UK Coronavirus Data Alert] New metrics available"
            email_type_text = "<p>These metrics are in line with what is published on the government dashboard at <a href='https://coronavirus.data.gov.uk/'>coronavirus.data.gov.uk</a> " \
                "As such, the period represented is the 7 days ending 5 days before the date when the website was last updated.</p>"
        else:
            subject = "[UK Coronavirus Data Alert] New metrics available. NOT FOR PUBLISH - most recent, unverified metrics"
            email_type_text = "<p>WARNING: The period represented is the 7 days ending with the latest day for which data is available. This is different from the government dashboard which " \
                "looks at the period ending 5 days before the website was last updated.</p>"

        body = textwrap.dedent(f"""
            {email_type_text}
            <p>Some metrics have exceeded the change threshold week on week:</p>
            {"".join(to_alert)}
            <p>Check https://coronavirus.data.gov.uk/</p>
        """)
        send_notification_email(subject, body)
    else:
        print(f"No metric exceeded the change threshold", file=sys.stderr)


def lambda_handler(event, lambda_context):
    compare_available_metrics()
    check_last_two_weeks_of_metrics()


if __name__ == "__main__":
    lambda_handler(None, None)
