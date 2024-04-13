import requests, json
 
api_key = "7641bfab7dd341c1357ecf41d4b30e55"
 
base_url = "http://api.openweathermap.org/data/2.5/weather?"
 
city_name = "Portland, Oregon"
 
complete_url = base_url + "appid=" + api_key + "&q=" + city_name
 
response = requests.get(complete_url)
 
x = response.json()

if x["cod"] != "404":
    y = x["main"]
    weathervar = x["weather"][0]
else:
    print(" City Not Found ")

if 'Rain' in weathervar.values():
    print("It's raining in Portland, Oregon")
else:
    print("It's not raining in Portland, Oregon")
    #print(weathervar)

base_url2 = "http://api.openweathermap.org/data/2.5/forecast?"
complete_url2 = base_url + "appid=" + api_key + "&q=" + city_name

response2 = requests.get(complete_url2)
data = response2.json()
#print(data)

if data["cod"] != "404":
    z = data["main"]
    weathervar2 = data["weather"][0]
else:
    print("city not found")

if "Rain" and "dt=1713218400" in weathervar.values():
    print("It's raining on Monday")
else:
    print("It's not raining on Monday")

