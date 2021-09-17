import pandas as pd
import pymssql
import os
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import numpy as np


def connect_sql():

	return pymssql.con