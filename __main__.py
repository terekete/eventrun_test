import yaml
import re
import pulumi
import os
import glob

from pulumi_gcp import storage, bigquery, serviceaccount, projects
from pulumi import automation as auto
from cerberus import Validator

print("Hello")
