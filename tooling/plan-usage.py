import argparse
import dotenv
import os
import requests
import time
from urllib.parse import urlparse

# https://circleci.com/docs/api/v2/index.html#tag/Usage


dotenv.load_dotenv()


HEADERS = {
    "content-type": "application/json",
    "Circle-Token": os.getenv('CCI_PAT')
}


def create_report_request(org_id, shared_org_ids, start_date_time_string, end_date_time_string):
    payload = {
        "start": start_date_time_string,
        "end": end_date_time_string,
        "shared_org_ids": shared_org_ids
    }

    print("create_report_request", payload)

    response = requests.post(
        f"https://circleci.com/api/v2/organizations/{org_id}/usage_export_job",
        json=payload,
        headers=HEADERS
    )

    response.raise_for_status()

    response_json = response.json()

    assert "failed" != response_json["state"]

    print("create_report_request", response_json["state"])

    return response_json["usage_export_job_id"]


def get_report_request(org_id, usage_export_job_id):
    print("get_report_request:", usage_export_job_id)
    response_json = None

    while True:
        response = requests.get(
            f"https://circleci.com/api/v2/organizations/{org_id}/usage_export_job/{usage_export_job_id}",
            headers=HEADERS
        )

        response.raise_for_status()

        response_json = response.json()

        if "completed" == response_json["state"]:
            break
        elif "failed" == response_json["state"]:
            raise Exception("Non success/continue status encountered", response_json)

        print(f"get_report_request [{usage_export_job_id}]: Sleeping for 15 seconds while we wait (currently '{response_json['state']}') to be completed...")
        time.sleep(15)

    print("get_report_request:", response_json["download_urls"])
    return response_json["download_urls"]


def download_report(download_url):
    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    file_name = urlparse(download_url).path.split('/')[-1]
    file_path = f"/tmp/cci-usage--{file_name}"

    print(f"download_report [{file_name}]: downloading...")
    with open(file_path, mode="wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            # filter out keep-alive new chunks
            if chunk:
                f.write(chunk)

    print(f"download_report [{file_name}]:", file_path)
    return file_path



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--start_date_time_string", type=str)
    parser.add_argument("-b", "--end_date_time_string", type=str)
    parser.add_argument("org_id")
    parser.add_argument("shared_org_ids", nargs="*", default=[])
    args = parser.parse_args()

    usage_export_job_id = create_report_request(
        args.org_id,
        args.shared_org_ids,
        args.start_date_time_string,
        args.end_date_time_string,
    )

    download_urls = get_report_request(
        args.org_id,
        usage_export_job_id
    )

    for download_url in download_urls:
        download_report(download_url)


main()
