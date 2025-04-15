import logging
import requests
import sys
import os
import zipfile
import time
def get_token(host_url, creds_json):
    """
    Authenticates against the X5 API and retrieves a bearer token.

    Args:
        host_url (str): Base URL of the X5 API (e.g., 'https://lp-api.x5.ru/v2/logistics/report').
        creds_json (dict): Dictionary containing login credentials (e.g., {'email': ..., 'password': ...}).

    Returns:
        dict: Authorization header in the format {'Authorization': 'Bearer <token>'}.

    Raises:
        SystemExit: If the authentication request fails.
    """

    try:
        response = requests.post(f"{host_url.replace('/report', '/auth')}", json=creds_json, verify=False)
        response.raise_for_status()
        token = 'Bearer ' + response.json()['result']['token']
        auth = {"Authorization": token}
        return auth
    except Exception as e:
        logging.error("Authentication failed", exc_info=True)
        sys.exit(1)

def get_report_ids(auth,host_url,start_date,end_date, reports_list):
    """
    Sends requests to generate reports for a specified list of report types and returns their report IDs.

    Args:
        auth (dict): Authorization header returned from `get_token`.
        host_url (str): Base URL of the X5 report API (e.g., 'https://lp-api.x5.ru/v2/logistics/report').
        start_date (str): Start date for the report (format: 'YYYY-MM-DD').
        end_date (str): End date for the report (format: 'YYYY-MM-DD').
        reports_list (list): List of report types to request (e.g., ['SALES', 'INVENTORY']).

    Returns:
        list of tuples: Each tuple contains (report_id, index_of_report_type_in_list).

    Raises:
        SystemExit: If any report request fails.
    """
    reports = []
    for report in reports_list:
        data = {
            "startDate": start_date,
            "finishDate": end_date,
            "isArchive": "True",
            "isCheck": "False",
            "typeReport": report
        }
        try:
            response = requests.post(host_url, headers=auth, json=data, verify=False)
            response.raise_for_status()
            report_id = response.json()['result']['reportId']
            reports.append((report_id, reports_list.index(report)))
            logging.info(f"Requested report: {report}, ID: {report_id}")
        except Exception as e:
            logging.error("Failed to request report", exc_info=True)
            sys.exit(1)
    return reports


def download_reports(auth,host_url,reports_with_ids,report_names_list, start_date, end_date, save_root_path):
    logging.info("Waiting for reports to be ready...")

    # Checking reports statuses until they are all ready
    ready_reports = set()
    total_reports = len(reports_with_ids)
    while True:
        for report_id, report_index in reports_with_ids:
            try:
                status = requests.get(f"{host_url}/{report_id}/status", headers=auth, verify=False).json()['result']['reportStatus']
                logging.info(f"Current status for report {report_names_list[report_index]}: {status}")
                time.sleep(5)
                if status == "DONE":
                    logging.info(f"Report {report_names_list[report_index]} is ready for download")
                    ready_reports.add(report_id)
            except Exception as e:
                logging.error("Error checking report status", exc_info=True)
                sys.exit(1)
            logging.info(f"Reports left: {total_reports - len(ready_reports)}")
        if len(ready_reports) == total_reports:
            break

    logging.info("All reports ready. Starting download...")
    for report_id, report_index in reports_with_ids:
        try:
            parts = requests.get(f"{host_url}/{report_id}/status", headers=auth, verify=False).json()['result']['partIds']
        except Exception:
            logging.error("No parts found for report.", exc_info=True)
            sys.exit(1)
        
        # create path using save_root_path and report_name
        report_name = report_names_list[report_index]
        os.makedirs(os.path.join(save_root_path, report_name, "zip"), exist_ok=True)

        for part_id in parts:
            # write report content parts to zip archive
            zip_path = os.path.join(save_root_path, report_name, "zip", f"{start_date}_{end_date}_{part_id[11:20]}.zip")
            report_content = requests.get(f"{host_url}/{part_id}/download", headers=auth, verify=False).content
            with open(zip_path, 'wb') as f:
                f.write(report_content)

            # extract content from zip archive
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.join(save_root_path, report_name))
                logging.info(f"Downloaded and extracted report: {report_name}")
    logging.info(f"All reports were successfully downloaded and extracted")