import argparse
import csv
import datetime
import dotenv
import gzip
import json
import os
import re
import requests
import sys
import time
import traceback
from urllib.parse import urlparse

# https://circleci.com/docs/api/v2/index.html#tag/Usage


dotenv.load_dotenv()


HEADERS = {"content-type": "application/json", "Circle-Token": os.getenv("CCI_PAT")}

EXPECTED_CSV_HEADERS = [
    "ORGANIZATION_ID",
    "ORGANIZATION_NAME",
    "ORGANIZATION_CREATED_DATE",
    "PROJECT_ID",
    "PROJECT_NAME",
    "PROJECT_CREATED_DATE",
    "LAST_BUILD_FINISHED_AT",
    "VCS_NAME",
    "VCS_URL",
    "VCS_BRANCH",
    "PIPELINE_ID",
    "PIPELINE_CREATED_AT",
    "PIPELINE_NUMBER",
    "IS_UNREGISTERED_USER",
    "PIPELINE_TRIGGER_SOURCE",
    "PIPELINE_TRIGGER_USER_ID",
    "WORKFLOW_ID",
    "WORKFLOW_NAME",
    "WORKFLOW_FIRST_JOB_QUEUED_AT",
    "WORKFLOW_FIRST_JOB_STARTED_AT",
    "WORKFLOW_STOPPED_AT",
    "IS_WORKFLOW_SUCCESSFUL",
    "JOB_NAME",
    "JOB_RUN_NUMBER",
    "JOB_ID",
    "JOB_RUN_DATE",
    "JOB_RUN_QUEUED_AT",
    "JOB_RUN_STARTED_AT",
    "JOB_RUN_STOPPED_AT",
    "JOB_BUILD_STATUS",
    "RESOURCE_CLASS",
    "OPERATING_SYSTEM",
    "EXECUTOR",
    "PARALLELISM",
    "JOB_RUN_SECONDS",
    "MEDIAN_CPU_UTILIZATION_PCT",
    "MAX_CPU_UTILIZATION_PCT",
    "MEDIAN_RAM_UTILIZATION_PCT",
    "MAX_RAM_UTILIZATION_PCT",
    "COMPUTE_CREDITS",
    "DLC_CREDITS",
    "USER_CREDITS",
    "STORAGE_CREDITS",
    "NETWORK_CREDITS",
    "LEASE_CREDITS",
    "LEASE_OVERAGE_CREDITS",
    "IPRANGES_CREDITS",
    "TOTAL_CREDITS",
]


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def eprint(*args, color=bcolors.OKCYAN):
    repacked_args = []

    for arg in args:
        if isinstance(arg, dict):
            repacked_args.append(json_dumps(arg, indent=2))
        else:
            repacked_args.append(arg)

    print(color, end="", file=sys.stderr)
    print(*repacked_args, end="", file=sys.stderr)
    print(bcolors.ENDC, file=sys.stderr)


def json_dumps(x, **kwargs):
    return json.dumps(x, **kwargs, sort_keys=True, default=lambda o: str(o))


def create_report_request(
    org_id, shared_org_ids, start_date_time_string, end_date_time_string
):
    payload = {
        "start": start_date_time_string,
        "end": end_date_time_string,
        "shared_org_ids": shared_org_ids,
    }

    eprint("create_report_request", payload)

    response = requests.post(
        f"https://circleci.com/api/v2/organizations/{org_id}/usage_export_job",
        json=payload,
        headers=HEADERS,
    )

    response.raise_for_status()

    response_json = response.json()

    assert "failed" != response_json["state"]

    eprint("create_report_request", response_json["state"])

    return response_json["usage_export_job_id"]


def get_report_request(org_id, usage_export_job_id):
    eprint("get_report_request:", usage_export_job_id)
    response_json = None

    while True:
        response = requests.get(
            f"https://circleci.com/api/v2/organizations/{org_id}/usage_export_job/{usage_export_job_id}",
            headers=HEADERS,
        )

        response.raise_for_status()

        response_json = response.json()

        if "completed" == response_json["state"]:
            break
        elif "failed" == response_json["state"]:
            raise Exception("Non success/continue status encountered", response_json)

        eprint(
            f"get_report_request [{usage_export_job_id}]: Sleeping for 15 seconds while we wait (currently '{response_json['state']}') to be completed..."
        )
        time.sleep(15)

    eprint("get_report_request:", response_json["download_urls"])
    return response_json["download_urls"]


def download_report(start_date_time_string, end_date_time_string, download_url):
    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    s = re.sub(r"[:-]", "_", start_date_time_string)
    e = re.sub(r"[:-]", "_", end_date_time_string)

    file_name = urlparse(download_url).path.split("/")[-1]
    file_path = f"/tmp/cci-usage--raw--{s}-{e}--{file_name}"

    eprint(f"download_report [{file_name}]: downloading...")
    with open(file_path, mode="wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            # filter out keep-alive new chunks
            if chunk:
                f.write(chunk)

    eprint(f"download_report [{file_name}]:", file_path)
    return file_path


def _parse_row(row):
    for k, v in list(row.items()):
        if "\\N" == v:
            row[k] = None
    return row


def _parse_downloaded_report_to_standard_csv(downloaded_file_path):
    """
    Returns a generator of parsed csv lines.

    Due to the general size of some of the downloaded files, we have to use a generator here
    otherwise we run the risk of OOM'ing _pretty quickly_.
    """
    with gzip.open(downloaded_file_path, "rt") as f:
        for row in csv.DictReader(f, quoting=csv.QUOTE_NONE, escapechar="\\"):
            yield _parse_row(row)


def _write_standard_csv_to_cleansed_file_path(downloaded_file_path, dicts):
    file_name = re.match(r".*cci-usage--raw--(.*)", downloaded_file_path).group(1)
    file_path = f"./cci-usage--cleansed--{file_name}"

    with gzip.open(file_path, "wt") as f:
        writer = csv.DictWriter(
            f, fieldnames=EXPECTED_CSV_HEADERS, extrasaction="ignore"
        )
        writer.writeheader()

        first_element = next(dicts)
        observed_keys = set(first_element.keys())
        if set(EXPECTED_CSV_HEADERS) != observed_keys:
            eprint(
                f"Unexpected keys found for row: {first_element}", color=bcolors.WARNING
            )
            eprint(
                f"+ {observed_keys - set(EXPECTED_CSV_HEADERS)}", color=bcolors.WARNING
            )
            eprint(
                f"- {set(EXPECTED_CSV_HEADERS) - observed_keys}", color=bcolors.WARNING
            )
            eprint(
                "Dropping these unexpected values/replacing with empty.",
                color=bcolors.WARNING,
            )
        writer.writerow(first_element)

        for d in dicts:
            writer.writerow(d)

    return file_path


def cleanse_downloaded_report_to_standard_csv(downloaded_file_path):
    return _write_standard_csv_to_cleansed_file_path(
        downloaded_file_path,
        _parse_downloaded_report_to_standard_csv(downloaded_file_path),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a", "--start_date_time_string", type=str, help="YYYY-MM-DDT00:00:00Z"
    )
    parser.add_argument(
        "-b",
        "--end_date_time_string",
        type=str,
        help="YYYY-MM-DDT00:00:00Z",
        default=datetime.datetime.now().strftime("%Y-%m-%dT00:00:00Z"),
    )
    parser.add_argument("--verbose_format", action="store_true")
    parser.add_argument("org_id")
    parser.add_argument("shared_org_ids", nargs="*", default=[])
    args = parser.parse_args()

    usage_export_job_id = create_report_request(
        args.org_id,
        args.shared_org_ids,
        args.start_date_time_string,
        args.end_date_time_string,
    )

    download_urls = get_report_request(args.org_id, usage_export_job_id)

    report_local_paths = [
        download_report(
            args.start_date_time_string, args.end_date_time_string, download_url
        )
        for download_url in download_urls
    ]

    for downloaded_file_path in report_local_paths:
        print(cleanse_downloaded_report_to_standard_csv(downloaded_file_path))

    return 0


try:
    sys.exit(main())
except Exception:
    eprint(traceback.format_exc(), color=bcolors.FAIL)
    sys.exit(-1)
