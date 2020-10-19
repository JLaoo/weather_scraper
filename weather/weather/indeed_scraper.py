import scrapy
import urllib
import sys
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import os
import csv
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

for state in states:
	url_formatted = urllib.parse.quote(state)
	state_queries.append(initial_query.format(url_formatted))
print(state_queries)
sys.exit()

# Generate starting URLs to scrape
for state in states:
	url_formatted = urllib.parse.quote(state)
	state_queries.append([state, (initial_query + url_formatted).lower()])
fields = ["job_name", "job_description", "company", "sector", "salary", "company_rating", "city", "state"]

# Directory setup
if not os.path.exists('output'):
	os.makedirs('output')
for state in states:
	with open('output/' + state + '.csv', 'w') as f:
		writer = csv.writer(f)
		writer.writerow(fields)

# find initial queries
city_urls = []
for query in state_queries:
	q = query[1]
	state = query[0]
	response = requests.get(q, headers=headers)
	soup = bs(response.text, 'html.parser')
	tags = soup.findAll(attrs={"class": "city"})
	for tag in tags:
		name = tag.findAll(attrs={"cityTitle"})[0].text
		href = tag.findAll(attrs={"cityTitle"})[0]['href']
		city_url = domain + href
		city_urls.append([name, city_url, state])

# build spider
class scraper(scrapy.Spider):
	def __init__(self):
		self.name = 'scraper'
		self.allowed_domains = ['indeed.com']

	# Start a scrape request for every single city in the state
	def start_requests(self):
		for info in city_urls:
			city_name = info[0]
			city_url = info[1]
			state = info[2]
			yield scrapy.FormRequest(url=city_url,
								headers=headers,
								callback=self.parse, 
								meta={
								'city': city_name, 
								'url': city_url,
								'start': True,
								'state': state}, 
								dont_filter=True, 
								errback=self.handle_failure)

	# If it fails, just try again. No proxy enabled for now.
	def handle_failure(self, failure):
		yield scrapy.FormRequest(url=failure.request.meta['url'],
							headers=headers,
							callback=self.parse,
							meta={
								'city': failure.request.meta['city'],
								'url': failure.request.meta['url'],
								'start': failure.request.meta['start'],
								'state': failure.request.meta['state']},
							dont_filter=True,
							errback=self.handle_failure)

	def parse(self, response):
		# If we've just started, turn off the "ignore duplicates" filter and resend the request.
		if response.meta['start']:
			soup = bs(response.text, 'html.parser')
			by_date = domain + soup.findAll(attrs={"class": "no-wrap"})[0].findAll()[1]['href']
			by_date += "&filter=0"
			yield scrapy.FormRequest(url=by_date,
									headers=headers,
									callback=self.parse,
									meta={
										'city': response.meta['city'],
										'url': by_date,
										'start': False,
										'state': response.meta['state']},
									dont_filter=True,
									errback=self.handle_failure)
			return
		soup = bs(response.text, "html.parser")
		postings = soup.findAll(attrs={"class": "jobsearch-SerpJobCard unifiedRow row result"})
		titles, companies, salaries, ratings, cities, states = [], [], [], [], [], []
		sectors, descriptions = [], []
		for post in postings:
			try:
				title = post.findAll(attrs={"class": "title"})[0].findAll()[0]['title'].strip("\n")
			except:
				title = ""
			try:
				company = post.findAll(attrs={"class": "sjcl"})[0].findAll(attrs={"class": "company"})[0].text.strip("\n")
			except:
				company = ""
			try:
				salary = post.findAll(attrs={"class": "salaryText"})[0].text.strip("\n")
			except:
				salary = ""
			try:
				rating = post.findAll(attrs={"class": "ratingsContent"})[0].text.strip("\n")
			except:
				rating = ""
			city = response.meta['city']
			state = response.meta['state']
			titles.append(title)
			companies.append(company)
			salaries.append(salary)
			ratings.append(rating)
			cities.append(city)
			states.append(state)
			# Some data require another request (ie company sector)
			metadata = post.findAll(attrs={"class": "turnstileLink"})
			industry = ''
			found_industry = False
			for data in metadata:
				if data['data-tn-element'] == "companyName":
					cmp_href = domain + data['href']
					c = requests.get(cmp_href, headers=headers)
					cmp_soup = bs(c.text, 'html.parser')
					cmp_data = cmp_soup.findAll(attrs={"class": "cmp-AboutMetadata-item"})
					for dat in cmp_data:
						if dat.find(attrs={'class': 'cmp-AboutMetadata-itemTitle'}).text == 'Industry':
							industry = dat.find(attrs={"class": "cmp-AboutMetadata-plainLink"}).text
							found_industry = True
							break
				if found_industry:
					break
			sectors.append(industry)
			try:
				full_post_url = domain + post.find(attrs={"class": "jobtitle turnstileLink"})['href']
				full_post_r = requests.get(full_post_url, headers=headers)
				full_soup = bs(full_post_r.text, 'html.parser')
				job_description = full_soup.find(attrs={'class': 'jobsearch-jobDescriptionText'}).text
			except:
				job_description = ''
			descriptions.append(job_description)
		# Build dataframe
		fields = {"job_name": titles,
				"job_description": descriptions,
				"company": companies,
				"sector": sectors,
				"salary": salaries,
				"company_rating": ratings,
				"city": city,
				"state": states}
		df = pd.DataFrame(data=fields)
		with open('output/' + response.meta['state'] + '.csv', 'a') as f:
			df.to_csv(f, header=None, index=False)
		pages = soup.findAll(attrs={"class": "pagination"})[0].findAll()
		# Check if there's a next button. If so, scrape the URL of the result of clicking next,
		# otherwise return and end the current recursive scrape.
		if "Next" in pages[len(pages) - 1].text:
			next_url = domain + pages[len(pages) - 3]['href']
		else:
			return
		yield scrapy.FormRequest(url=next_url,
								headers=headers,
								callback=self.parse,
								meta={
									'city': response.meta['city'],
									'url': next_url,
									'start': False,
									'state': response.meta['state']},
								dont_filter=True,
								errback=self.handle_failure)

# Function to start scraping
def start_scraping():
	process = CrawlerProcess(get_project_settings())
	process.crawl(scraper)
	process.start()

start_scraping()

