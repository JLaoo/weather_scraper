import scrapy
import urllib
import sys
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import os
import csv
import re
import json
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Constants
domain = "https://www.timeanddate.com"
states = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado",
  "Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois",
  "Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland",
  "Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana",
  "Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
  "North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
  "Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
  "Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]
initial_query = "https://www.timeanddate.com/weather/usa/{}"
headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"}
state_queries = []
city_queries = []

fields = ["city", "date", "high_temp_faren", "low_temp_faren", "wind_mph", 
			"humidity_percent", "precipitation_chance_percent", "precipitation_amount_inches"]

# Generate URLs to all states
for state in states:
	url_formatted = state.replace(" ", "-")
	file_name_formatted = state.replace(" ", "_")
	state_queries.append((file_name_formatted, initial_query.format(url_formatted)))

# Directory setup
if not os.path.exists('output'):
	os.makedirs('output')
for state in states:
	formatted = state.replace(" ", "_")
	with open('output/' + formatted + '.csv', 'w') as f:
		writer = csv.writer(f)
		writer.writerow(fields)

# find initial queries
for query in state_queries:
	q = query[1]
	state = query[0]
	# Edge case
	if state == "New_York":
		q += "-state"
	response = requests.get(q, headers=headers)
	soup = bs(response.text, 'html.parser')
	hrefs = soup.find_all('a', href=True)
	location_tag = soup.findAll(attrs={"class": "small soft"})
	try:
		location_txt = location_tag[0].getText()
	except:
		print(query)
		sys.exit()
	num_locations = int(re.search(r'\d+', location_txt).group())
	city_hrefs = [h for h in hrefs if "/weather/usa/" in h["href"]]
	city_hrefs = city_hrefs[-num_locations:]
	for h in city_hrefs:
		city_queries.append((state, domain + h['href'] + "/ext"))

	# only alabama for now
	# break

# build spider
class scraper(scrapy.Spider):
	def __init__(self):
		self.name = 'scraper'
		self.allowed_domains = ['timeanddate.com']

	# Start a scrape request for every single city in the state
	def start_requests(self):
		for info in city_queries:
			url = info[1]
			state = info[0]
			yield scrapy.FormRequest(url=url,
								headers=headers,
								callback=self.parse, 
								meta={ 
								'url': url,
								'state': state}, 
								dont_filter=True, 
								errback=self.handle_failure)

	# If it fails, just try again. No proxy enabled for now.
	def handle_failure(self, failure):
		yield scrapy.FormRequest(url=failure.request.meta['url'],
							headers=headers,
							callback=self.parse,
							meta={
								'url': failure.request.meta['url'],
								'state': failures.request.meta['state']},
							dont_filter=True,
							errback=self.handle_failure)

	def parse(self, response):
		soup = bs(response.text, "html.parser")
		city = soup.findAll(attrs={"target": "_top"})[-1].getText()
		rawJ = soup.findAll(attrs={"type": "text/javascript"})
		J = str(rawJ[0])
		J1 = J.split('var data=')
		J2 = J1[1].rsplit(';', 1)
		data = json.loads(J2[0])
		entries = data['detail']
		dates, temphighs, templows, winds, humidities, precip_chances, precip_amounts = [], [], [], [], [], [], []
		cities = []
		for row in entries:
			try:
				date = row['ds']
			except:
				date = ""
			try:
				temp_high = row['temp']
			except:
				temp_high = ""
			try:
				temp_low = row['templow']
			except:
				temp_low = ""
			try:
				wind = row['wind']
			except:
				wind = ""
			try:
				humidity = row['hum']
			except:
				humidty = ""
			try:
				precip_chance = row['pc']
			except:
				precip_chance = ""
			try:
				precip_amount = row['rain']
			except:
				precip_amount = ""
			dates.append(date)
			temphighs.append(temp_high)
			templows.append(temp_low)
			winds.append(wind)
			humidities.append(humidity)
			precip_chances.append(precip_chance)
			precip_amounts.append(precip_amount)
			cities.append(city)

		# Build dataframe
		fields = {"city": cities,
				"date": dates,
				"high_temp_faren": temphighs,
				"low_temp_faren": templows,
				"wind_mph": winds,
				"humidity_percent": humidities,
				"precipitation_chance_percent": precip_chances,
				"precipitation_amount_inches": precip_amounts}
		df = pd.DataFrame(data=fields)
		with open('output/' + response.meta['state'] + '.csv', 'a') as f:
			df.to_csv(f, header=None, index=False)

# Function to start scraping
def start_scraping():
	process = CrawlerProcess(get_project_settings())
	process.crawl(scraper)
	process.start()

start_scraping()

