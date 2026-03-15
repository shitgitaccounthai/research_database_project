import os
import kagglehub

# Define your user-defined path
os.environ["KAGGLEHUB_CACHE"] = "D:/datascience_project/data"

# Download the dataset to that specific path
path = kagglehub.dataset_download("Cornell-University/arxiv")

print("Path to dataset files:", path)