import requests
import json

urls = [
    "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id=3624",
    "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id=3912"
]

data = []
for url in urls:
    response = requests.get(url)
    if response.status_code == 200:
        data.extend(response.json())

with open("bcsample.json", "w") as file:
    json.dump(data, file, indent=4)

print("Data fetched and saved to bcsample.json")