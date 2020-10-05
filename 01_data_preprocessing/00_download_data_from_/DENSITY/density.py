import pandas as pd
from pathlib import Path
import os, re,  requests, json
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
