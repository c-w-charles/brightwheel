"""This script contains functions that process BrightWheel provider data"""
from bs4 import BeautifulSoup
import csv
import pandas as pd
import requests
import warnings


def get_provider_data_from_api(api_file_name: str):
    """Gets provider data from API and writes it to a delimited file.
    Args:
        api_file_name (str): file name to store output in
    """
    # Source metadata
    URL = "https://bw-interviews.herokuapp.com/data/providers"
    header_list = [
        "id",
        "provider_name",
        "phone",
        "email",
        "owner_name",
    ]

    # Suppress request warnings
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    # Open output file and write header row
    output_file = open(api_file_name, "w")
    csv_file = csv.writer(output_file)
    csv_file.writerow(header_list)

    # Call API for specified request
    response = requests.get(URL, verify=False)

    # Parse JSON results out into a delimited file
    provider_results = response.json()["providers"]
    for row in provider_results:
        csv_row = [
            row["id"],
            row["provider_name"],
            row["phone"],
            row["email"],
            row["owner_name"],
        ]
        csv_file.writerow(csv_row)

    output_file.close()


def get_provider_data_from_web_site(web_file_name: str):
    """Gets provider data from a web site and writes it to a delimited file.
    Args:
        web_file_name (str): file name to store output in
    """
    # Source metadata
    URL = "http://naccrrapps.naccrra.org/navy/directory/programs.php?program=omcc&state=CA&pagenum="
    header_list = [
        "provider_name",
        "type_of_care",
        "address",
        "city",
        "state",
        "zip",
        "phone",
        "email",
    ]

    # Suppress request warnings
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    # Open output file and write header row
    output_file = open(web_file_name, "w")
    csv_file = csv.writer(output_file)
    csv_file.writerow(header_list)

    # Get 45 pages of data from web site
    for page in range(1, 45):
        page_req = requests.get(URL + str(page), verify=False)

        # Parse HTML, extracting providers from an HTML table
        soup = BeautifulSoup(page_req.content, "html.parser")
        html_table = soup.find("table").find_all("td")
        column_num = 1
        row = []

        # Write 8 columns per row to a csv file
        for element in html_table:
            row.append(element.text)
            if column_num == 8:
                csv_file.writerow(row)
                row = []
                column_num = 0
            column_num += 1

    output_file.close()


def add_header_to_file(input_file_name: str, output_file_name: str, header_list: list):
    """Adds a header row to specified file.
    Args:
        input_file_name (str): file name to read from
        output_file_name (str): file name to write to
    """
    # Read file and add a header row
    df = pd.read_csv(input_file_name)
    df.to_csv(output_file_name, header=header_list, index=False)


def merge_provider_files():
    """Merges 3 BrightWheel provider sources into 1 output csv file"""

    # Pull provider data from API and web site, storing in csv files
    api_file_name = "providers_from_api.csv"
    get_provider_data_from_api(api_file_name)
    web_file_name = "providers_from_web.csv"
    get_provider_data_from_web_site(web_file_name)

    # Add header row to attached file
    attached_file_name = "x_ca_omcc_providers.csv"
    attached_file_name_with_header = "x_ca_omcc_providers_new.csv"
    header_list = [
        "provider_name",
        "type_of_care",
        "address",
        "city",
        "state",
        "zip",
        "phone",
    ]
    add_header_to_file(attached_file_name, attached_file_name_with_header, header_list)

    # Read 3 source files into dataframes
    df_api = pd.read_csv(api_file_name)
    df_web = pd.read_csv(web_file_name)
    df_attached = pd.read_csv(attached_file_name_with_header)

    # Merge 3 dataframes into 1
    merged_df = pd.merge(df_api, df_web, on="provider_name").merge(
        df_attached, on="provider_name"
    )

    # Sort df by provider name
    merged_df = merged_df.sort_values(by=["provider_name"])

    # Write to csv file
    merged_file_name = "merged_providers.csv"
    merged_df.to_csv(merged_file_name, index=False)


if __name__ == "__main__":
    merge_provider_files()
