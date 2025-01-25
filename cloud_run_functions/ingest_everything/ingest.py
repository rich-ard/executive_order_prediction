#!/usr/bin/env python3

import IngestionScripts.retrieve_economic_indicators as econ
import IngestionScripts.retrieve_executive_orders as exec
import IngestionScripts.retrieve_presidential_approval as appr
import functions_framework

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def ingest_everything(cloud_event):
    econ.retrieve_and_write_csv_to_bucket()
    exec.retrieve_and_write_json_to_bucket()
    appr.retrieve_and_write_csv_to_bucket()