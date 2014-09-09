#!/usr/bin/env python

"""
Script to export an RJMetrics table to CSV (up to 10M rows) using the Export API.

Docs: https://rjmetrics.zendesk.com/entries/24125416-Data-Export-API-Automate-data-retrieval-from-RJMetrics

Usage:

python export.py -k <your export API key> -c <your client ID> -t <table ID to export> 1> outfile.csv

Note: the Export API key is different from an Import API key and must be whitelisted by IP.
"""

import argparse

import sys
import StringIO
import zipfile

import requests


def get_table(args, max_tries=5):
    # Trigger table export
    export_url = "https://api.rjmetrics.com/0.1/client/{0.client_id}/table/{0.table_id}/export".format(args)
    print >> sys.stderr, 'POST', export_url
    response = requests.post(export_url, data="",
                             headers={'X-RJM-API-Key': args.api_key})

    if not response.ok:
        print >> sys.stderr, "There was an error starting the export:", response.status_code, response.text
        if response.status_code == 403:
            print >> sys.stderr, "Please grant this IP address access to your export API."
        exit(1)
    export_id = response.json()['export_id']

    # Fetch the exported table
    while True:
        export_data_url = "https://api.rjmetrics.com/0.1/export/{0}".format(export_id)
        print >> sys.stderr, 'GET', export_data_url

        data_response = requests.get(export_data_url,
                                     headers={'X-RJM-API-Key': args.api_key})

        if data_response.ok:
            break
        else:
            print >> sys.stderr, data_response.status_code, data_response.text

        max_tries -= 1
        if max_tries < 0:
            print >> sys.stderr, "Error exporting table."
            exit(1)

    # Unzip exported table and write to stdout
    export_zip = zipfile.ZipFile(StringIO.StringIO(data_response.content), 'r')
    for filename in export_zip.namelist():
        print export_zip.read(filename)

    print >> sys.stderr, "Exported table."


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('-k', '--api-key', required=True, help="Data Export API Key")
    parser.add_argument('-c', '--client-id', required=True, help="Client ID")
    parser.add_argument('-t', '--table-id', required=False, help="Table ID to export")

    args = parser.parse_args()

    get_table(args)


if __name__ == '__main__':
    main()
