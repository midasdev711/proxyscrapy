# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from datetime import datetime
import MySQLdb
import time
from bs4 import BeautifulSoup
import hashlib
import json
################## MAX CONNECTIONS MYSQL!!!! #######################
import random
from subprocess import check_output
import threading
from threading import Thread
import time
from multiprocessing import Process, Value, Array
import requests
global pages_to_visit,total_links,country_number,country_code,scraper,numbers_non_matched,numbers_found,scraper,run_id,ad_updated_on,domain,id_country,ads,same_numbers,unique_numbers
unique_numbers = 0
#from selenium import webdriver
pages_to_visit = []
scraper = 'www.olx.ua'
ads = []
# id_country = ''
country_number = "380"
numbers_non_matched = []
total_links = 0
numbers_found = []
country_code = 'UA'
category_visited = [] 
same_numbers = 0
# from selenium import webdriver
pages_to_visit = []

header = {
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"

    }


price_inc = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 200000, 500000, 1000000, 5000000,1000000000]
sq_feet = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 50, 60, 70, 80, 90, 100, 500]
milage = [0,50000,100000,150000,200000,500000]
cat_link = []
cat_name = []



def proxyUpdate(curr_proxy):
	# print "UPDATEDING PROXYYYYYYYYYYYYYYYYYYYYYYYYYY##################",curr_proxy
	proxy_ip = curr_proxy.split('@')[-1].split(':')[0]
	port =  curr_proxy.split(':')[-1]
	updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	# suspended_level = 0
	proxy_id = ''
	# print "DB CALLLLLLLL"
	while True:
		try:
			db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
			break
		except:
			pass
	cur = db.cursor()
	cur.execute("SELECT id FROM proxies WHERE ip ='"+str(proxy_ip)+"' AND port ='"+str(port)+"'")
	for row in cur.fetchall() :	
		proxy_id = str(row[0])
	db.close()
	# print "AFTER DBBB"
	# print "PROXY IDDDD",proxy_id
	# print suspended_level
	while True:
		try:
			db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
			break
		except:
			print "DB Re-request"
	cur = db.cursor()
	print("UPDATE classified_websites_proxies SET updated_at='"+str(updated_at) + "',status='online',suspended_level='0'  WHERE proxy_id ='"+str(proxy_id)+"' and classified_id='"+str(id_country)+"'")

	cur.execute("UPDATE classified_websites_proxies SET updated_at='"+str(updated_at) + "',status='online',suspended_level='0'  WHERE proxy_id ='"+str(proxy_id)+"' and classified_id='"+str(id_country)+"'")
	db.commit()
	db.close()

def scraping_history(run_id_):
	# global run_id
	if str(run_id_) == 'new':
		created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		while True:
			try:
				db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
				break
			except:
				print "DB Re-request"
		cur = db.cursor()

		cur.execute("INSERT INTO scraping_history (scraper, created_at) VALUES (%s,%s)",(scraper,created_at))
		db.commit()
		run_id = cur.lastrowid
		db.close()

	else:

		updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		run_id = str(run_id_)


		while True:
			try:
				db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
				break
			except:
				print "DB Re-request"
		cur = db.cursor()
		#print "UPDATE scraping_history SET numbers_found='"+str(len(numbers_found))+"',numbers_unique='"+str(len(list(set(numbers_found))))+"',numbers_non_matched='"+str(len(numbers_non_matched))+"', updated_at='"+str(updated_at)+"',links_found='"+str(total_links)+"',links_unique='"+str(total_links)+"' WHERE run_id ='"+str(run_id)+"'"



		cur.execute("UPDATE scraping_history SET numbers_found='"+str(len(numbers_found))+"',numbers_unique='"+str(unique_numbers)+"',numbers_non_matched='"+str(len(numbers_non_matched))+"', updated_at='"+str(updated_at)+"',links_found='"+str(total_links)+"',links_unique='"+str(total_links)+"' WHERE run_id ='"+str(run_id)+"'")
		db.commit()
		db.close()
	return run_id


def insertPhoneNumber(phonenumber,city,area,district):
	global numbers_found,same_numbers,unique_numbers
	try:
		city_name = city.lower().replace('-','_').replace(' ','_')
		area_name = area.lower().replace('-','_').replace(' ','_')
		district_name = district.lower().replace('-','_').replace(' ','_')

		created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		updated_at = created_at 

		#GETING ALL CITIES IN THE COUNTRY AND MATCHING THE DESIRED ONE
		while True:
			try:
				db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
				break
			except:
				print "DB Re-request"
		cur = db.cursor()
		# print "SELECT id FROM cities WHERE country_code ='"+str(pn_country_code)+ "' AND name='"+str(city) +"'"
		cur.execute("SELECT id FROM cities WHERE country_code ='"+str(country_code)+ "' AND name='"+str(city_name) +"'")
		pn_city_id = 0
		for row in cur.fetchall() :
			pn_city_id = row[0]
		db.close()

		if pn_city_id == 0 and len(city_name) > 2:
			
			while True:
				try:
					db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
					break
				except:
					print "DB Re-request"
			cur = db.cursor()					
			print("INSERT INTO cities (country_code, name,display_name,created_at,updated_at) VALUES (%s,%s,%s,%s,%s)",(country_code,city_name,city,created_at,updated_at))
			cur.execute("INSERT INTO cities (country_code, name,display_name,created_at,updated_at) VALUES (%s,%s,%s,%s,%s)",(country_code,city_name,city,created_at,updated_at))
			db.commit()
				
			pn_city_id = cur.lastrowid
			db.close()
		#GETING ALL AREAAS IN THE CITY  AND MATCHING THE DESIRED ONE
		while True:
			try:
				db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
				break
			except:
				print "DB Re-request"
		cur = db.cursor()
		cur.execute("SELECT id FROM areas WHERE city_id ='"+str(pn_city_id)+ "' AND name='"+str(area_name) +"'")
		pn_area_id = 0
		for row in cur.fetchall() :
			pn_area_id = row[0]
		db.close()
		if pn_area_id == 0 and len(area_name) > 2:
			
			while True:
				try:
					db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
					break
				except:
					print "DB Re-request"
			cur = db.cursor()
			cur.execute("INSERT INTO areas (city_id, name,display_name,created_at,updated_at) VALUES (%s,%s,%s,%s,%s)",(pn_city_id,area_name,area,created_at,updated_at))
			db.commit()
				
			pn_area_id = cur.lastrowid
			db.close()


		#GETING ALL DISTRICTS IN THE AREA AND MATCHING THE DESIRED ONE
		while True:
			try:
				db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
				break
			except:
				print "DB Re-request"
		cur = db.cursor()
		cur.execute("SELECT id FROM districts WHERE area_id ='"+str(pn_area_id)+ "' AND name='"+str(district_name) +"'")
		pn_district_id = 0
		for row in cur.fetchall() :
			pn_district_id = row[0]
		db.close()
		if pn_district_id == 0 and len(district_name) > 2:
			
			while True:
				try:
					db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
					break
				except:
					print "DB Re-request"
			cur = db.cursor()
			cur.execute("INSERT INTO districts (area_id, name,display_name,created_at,updated_at) VALUES (%s,%s,%s,%s,%s)",(pn_area_id,district_name,district,created_at,updated_at))
			db.commit()
				
			pn_district_id = cur.lastrowid
			db.close()
		created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		updated_at = created_at 
		while True:
			try:
				db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
				break
			except:
				print "DB Re-request"
		cur = db.cursor()
		# print "INSERT INTO mobile_numbers (`number`,`country_code`,postal_code_id,created_at,updated_at,city_id,area_id,district_id) VALUES ("+str(phonenumber)+\
		# 	",'"+str(country_code)+"',0,'"+str(created_at)+"','"+str(updated_at)+"','"+str(pn_city_id)+"','"+str(pn_area_id)+"','"+str(pn_district_id)+"')"

		cur.execute("INSERT INTO mobile_numbers (`number`,`country_code`,postal_code_id,created_at,updated_at,city_id,area_id,district_id) VALUES ("+str(phonenumber)+\
			",'"+str(country_code)+"',0,'"+str(created_at)+"','"+str(updated_at)+"','"+str(pn_city_id)+"','"+str(pn_area_id)+"','"+str(pn_district_id)+"')")
		db.commit()
		db.close()
		same_numbers = 0
		unique_numbers += 1
	except:
		
		# numbers_found = numbers_found[1:]
		same_numbers += 1
		print 'number not insterted,already in',phonenumber,'same_numbers',same_numbers

def proxySuspended(proxy_suspended):
	# global index_proxy
	proxy_ip = proxy_suspended.split('@')[-1].split(':')[0]
	port =  proxy_suspended.split(':')[-1]
	updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	suspended_level = 0
	proxy_id = ''
	while True:
		try:
			db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
			break
		except:
			print "DB Re-request"
	cur = db.cursor()
	cur.execute("SELECT id FROM proxies WHERE ip ='"+str(proxy_ip)+"' AND port ='"+str(port)+"'")
	for row in cur.fetchall() :	
		proxy_id = int(row[0])
	db.close()
	

	while True:
		try:
			db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
			break
		except:
			print "DB Re-request"
	cur = db.cursor()
	cur.execute("SELECT suspended_level FROM classified_websites_proxies WHERE proxy_id ='"+str(proxy_id)+"' and classified_id='"+str(id_country)+"'")
	for row in cur.fetchall() :	
		suspended_level = int(row[0])
	db.close()


	suspended_level += 1
	# print suspended_level
	while True:
		try:
			db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
			break
		except:
			print "DB Re-request"
	cur = db.cursor()
	cur.execute("UPDATE classified_websites_proxies SET updated_at='"+str(updated_at) + "',status='suspended',suspended_level='"+str(suspended_level) + "'  WHERE proxy_id ='"+str(proxy_id)+"' and classified_id='"+str(id_country)+"'")
	db.commit()
	db.close()
def scrape_ad(proxy,url):
	global numbers_found,total_links,numbers_non_matched,same_numbers
	trys = 0
	print 'vnatree',url
	while trys < 3:
		try:
			http_proxy  = "http://"+ str(proxy)
			https_proxy = "https://"+ str(proxy)

			proxy_ = {
			"http": http_proxy,
			"https": https_proxy,
			}
			source_code = requests.get(url,headers=header,proxies=proxy_,timeout=20)
			plain_text = source_code.text
			soup = BeautifulSoup(plain_text,"lxml")
			break	
		
		except:
			trys += 1
			time.sleep(1)
	if trys >= 3:
		print "proxy loso - Timeout",proxy
		proxySuspended(proxy)
	elif 'DOMContentLoaded' not in plain_text:
		print "proxy loso, no DOMContentLoaded",proxy,url,plain_text,"<<<<"
		proxySuspended(proxy)

	else:
			# f = open("numbers.txt","a+")
		try:
			proxy_bad = 'no'
			num_req = 'https://www.olx.ua/ajax/misc/contact/phone/'+plain_text.split("'id':")[1].split("'")[1]
			
			try:
				source_code_pn = requests.get(num_req,headers=header,proxies=proxy_,timeout=20)
				plain_text_pn = source_code_pn.text
				soup_pn = BeautifulSoup(plain_text_pn,"lxml")
				
			except:
				proxy_bad = 'yes'
			phonenumber = plain_text_pn.split('"value":')[1].split('"')[1]
			phonenumber = phonenumber.replace(' ','').replace('-','').replace('(','').replace(')','')
			if phonenumber[:4] == "00380":
				phonenumber = phonenumber[4:]
			# elif phonenumber[:2] == "39" and (phonenumber[2] != '0' and phonenumber[2] != '0' and phonenumber[2] != '0' and phonenumber[2] != '0' ):
			# 	phonenumber = phonenumber[2:]
			elif phonenumber[:2] == "00":
				phonenumber = "999999999"
			elif phonenumber[0] == '0':
				phonenumber = phonenumber[1:]
			else:
				print phonenumber,"ok"
			
			if phonenumber[:2] == "39" or phonenumber[:2] == "50" or phonenumber[:2] == "63" or phonenumber[:2] == "66" or phonenumber[:2] == "67" or phonenumber[:2] == "68" \
			or phonenumber[:2] == "91" or phonenumber[:2] == "92" or phonenumber[:2] == "93" or phonenumber[:2] == "94" or phonenumber[:2] == "95" or phonenumber[:2] == "96" \
			or phonenumber[:2] == "97" or phonenumber[:2] == "98" or phonenumber[:2] == "99":
			
 				phonenumber = "380"+phonenumber
			
				address = soup.find('div',{"class":"offer-titlebox__details"}).find('a',{"class":"show-map-link"}).text.strip()
				try:
					#city = loc2
					city = address.split(',')[-1].strip().title()
				except:
					city = ''

				try:
					area =address.split(',')[-2].strip().title()
					#area = loc3
				except:
					area = ''
				
				try:
					#district = loc5
					district = address.split(',')[0].strip().title()
				except:
					district = ''
				numbers_found.append(phonenumber)
				print phonenumber,"##",city,"##",area,"##",district
				insertPhoneNumber(phonenumber,city,area,district)
			else:
				numbers_non_matched.append(phonenumber)
			# update_link(hashed_url)
			
			# total_links += 1
		except:
			# update_link(hashed_url)
			print "URL NEMA BROJ",url,proxy,">>>>>>>",str(soup.find('title')),"<<<<<<",proxy_bad
			# if 'Cookies and JavaScript support required' not in plain_text:
			# 	print url+ " bez broj,da se proveri " + proxy +  " __"
			# 	print "______________"
			# 	print plain_text
			# 	print "______________"
			# 	# update_link(hashed_url)
			# else:
			# 	print url+ " proxy se poeba " + proxy
		# f.write(m+" "+ datetime.now().str
		if proxy_bad == 'no':
			total_links  += 1
			proxyUpdate(proxy)
			ads.remove(url)
		else:
			proxySuspended(proxy)
def mt_ads(ads_):
	global ads,run_id,id_country
	ads = ads_
	data = ""
	print domain
	
	total_done = int(open("ads_done.txt").readline())
	print len(ads),"line 340"
	ads = ads[total_done:]
	all_ads = len(ads)
	print total_done,"line 343",all_ads

	while len(ads) > 0:
		while True:
			try:
				db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
				break
			except:
				print "db  re-request" # name of the data base
		cur = db.cursor()
		cur.execute("select proxy_countries,id,max_proxies from classified_websites where domain = '"+domain+"'")
		
		for row in cur.fetchall():
			data = json.loads(row[0])
			id_country = row[1]
			max_proxies = int(row[2])
		db.close()
		print id_country,"COUNTRYYYYY"
		
		p_ids = []
		for c_code in data:
			while True:
				try:
					db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
					break
				except:
					print "db  re-request" # name of the data base
			cur = db.cursor()
			
			cur.execute("SELECT id FROM proxies WHERE COUNTRY_CODE ='"+str(c_code)+ "' AND ID NOT IN (SELECT PROXY_ID FROM classified_websites_proxies WHERE classified_id='"+str(id_country)+"') " )
			for row in cur.fetchall():
				p_ids.append(row[0])

			db.close()

		print len(p_ids)

		for proxy_id in p_ids:
			created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			while True:
				try:
					db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
					break
				except:
					print "db  re-request" # name of the data base
			cur = db.cursor()
			cur.execute("INSERT INTO classified_websites_proxies (classified_id, proxy_id,status,suspended_level,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s)", (id_country,proxy_id,"online","0",created_at,updated_at))
			db.commit()
			db.close()

		p_ids = []
		while True:
			try:
				db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
				break
			except:
				print "db  re-request" # name of the data base
		cur = db.cursor()
		cur.execute("SELECT proxy_id FROM classified_websites_proxies WHERE classified_id='"+str(id_country)+"' AND (STATUS='online' or \
			   (status='suspended' and updated_at < NOW() - INTERVAL 1 HOUR and suspended_level = '1')\
		    or (status='suspended' and updated_at < NOW() - INTERVAL 3 HOUR and suspended_level = '2') \
		    or (status='suspended' and updated_at < NOW() - INTERVAL 6 HOUR and suspended_level = '3') \
			or (status='suspended' and updated_at < NOW() - INTERVAL 12 HOUR and suspended_level = '4') \
			or (status='suspended' and updated_at < NOW() - INTERVAL 24 HOUR and suspended_level = '5') \
			or (status='suspended' and updated_at < NOW() - INTERVAL 30 HOUR and suspended_level = '6')) " )
		for row in cur.fetchall():
			p_ids.append(row[0])

		db.close()
		plist = []
		for p_id in p_ids:
			while True:
				try:
					db = MySQLdb.connect(host="localhost",user="smswords_swadmin",passwd="PASSWORD_HERE", db="smswords_project",charset='utf8',use_unicode=True) # name of the data base
					break
				except:
					print "db  re-request" # name of the data base
			cur = db.cursor()
			cur.execute("SELECT  ip,username,password,port,id FROM proxies WHERE ID ='"+str(p_id)+ "'")
			for row in cur.fetchall() :
				ip = row[0]
				username = row[1]
				password = row[2]
				port= row[3]
				if username != '':
					fpx = str(username) + ":"+ str(password) + "@"+ str(ip) + ":"+ str(port)
				else:
					fpx = str(ip) + ":"+ str(port)
				plist.append(fpx)
			db.close()
		plist = plist[:max_proxies]
		print plist,len(plist), "total proxies"

		

		threads = []
		if len(ads) >= len(plist):
			upper_bound = len(plist)
		else:
			upper_bound = len(ads)

		print "ads",len(ads),"proxies",len(plist),"upper_bound",upper_bound
		for j in range(0,upper_bound):

			pr = plist[j]
			t = threading.Thread(target=scrape_ad, args=(pr,ads[j]))
			threads.append(t)
			t.start()
		
		for t in threads:
			
			t.join()
		# print "sleeping 10"

		print "ads left",len(ads)
		total_done = all_ads - len(ads)
		f = open("ads_done.txt","w+")
		f.write(str(total_done))
		f.close()

		run_id = scraping_history(run_id)
		if len(ads) > 0:
			to_sleep = random.uniform(5,15) 
			print "now sleeping",to_sleep
			time.sleep(to_sleep)
		else:
			to_sleep = random.uniform(0,1) 
			print "now sleeping",to_sleep
			time.sleep(to_sleep)
	f = open("ads_done.txt","w+")
	f.write("0")
	f.close()



def collect_ads(url):
	ads = []
	offset = 1
	while True:
		try:
			source_code = requests.get(url+"&page="+str(offset),headers=header)
			break
		except:
			pass
	plain_text = source_code.text
	soup = BeautifulSoup(plain_text,"lxml")
	max_pages = int(plain_text.split('"page_count"')[1].split('"')[1])
	if max_pages > 110:
		max_pages = 110
	all_in = 0
	while offset <= max_pages:
		print offset,"max page",max_pages
		for a in soup.findAll('a',{"class":"thumb"}):
			if a.get('href') not in ads:
				ads.append(a.get('href'))

		offset += 1
		while True:
			try:
				source_code = requests.get(url+"&page="+str(offset),headers=header)
				break
			except:
				pass
		plain_text = source_code.text
		soup = BeautifulSoup(plain_text,"lxml")
		# max_pages = int(plain_text.split('"page_count"')[1].split('"')[1])

	ads = list(set(ads))
	print len(ads)," ADSSS TO INSERT"
	b = len(ads)
	#######################
	mt_ads(ads)
	#########################
	print "collected "
	return b


def cars(url,cat_name,cat_link,min_price,nr_ads):
	# min_mil = 0
	# max_mil = 5000
	
	f = open('milage.txt','r+')
	line = f.readline()#.split(' ')[0]
	min_mil = int(line.split(' ')[0])
	max_mil = int(line.split(' ')[1])
	f.close()

	price_index = 0
	print"INSIDEEE"
	url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price) + "&search[filter_float_milage:from]="+str(min_mil)+"&search[filter_float_milage:to]="+str(max_mil)

	sum_ads = 0
	while (sum_ads<nr_ads) and max_mil < 1000000:
		try:
			print url
			while True:
				try:
					source_code = requests.get(url,headers=header)
					break
				except:
					pass
			plain_text = source_code.text
			soup = BeautifulSoup(plain_text,"lxml")
			
			f = open("milage.txt","w+")
			f.write(str(min_mil)+" "+str(max_mil))
			f.close()

			total_ads = int(plain_text.split('totalAds')[1].split("'")[1].replace('.','').replace(',','').replace(' ',''))
			
			print total_ads,"INSIDEE",sum_ads,url

			if total_ads <= 5000:
				tmp = max_mil - min_mil
				min_mil += tmp + 1
				max_mil += tmp + 5000
				sum_ads += collect_ads(url)
				url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price) + "&search[filter_float_milage:from]="+str(min_mil)+"&search[filter_float_milage:to]="+str(max_mil)

			elif max_mil-min_mil < 2000:
				sum_ads += collect_ads(url+"&search%5Border%5D=filter_float_price%3Adesc")
				sum_ads += collect_ads(url+"&search%5Border%5D=filter_float_price%3Aasc")

			else:
				max_mil = max_mil - 1000
				url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price) + "&search[filter_float_milage:from]="+str(min_mil)+"&search[filter_float_milage:to]="+str(max_mil)


		except:
			tmp = max_mil - min_mil
			min_mil += tmp + 1
			max_mil += tmp + 5000
			url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price) + "&search[filter_float_milage:from]="+str(min_mil)+"&search[filter_float_milage:to]="+str(max_mil)
			print url,"cars except"
			
	f = open("milage.txt","w+")
	f.write("0 5000")
	f.close()
	return sum_ads

def real_estate_call(url,cate_name,cat_link,min_price,nr_ads):
	# min_re = 1
	# max_re = 10
	f = open('real_estate.txt','r+')
	line = f.readline()#.split(' ')[0]
	min_re = int(line.split(' ')[0])
	min_re = int(line.split(' ')[1])
	f.close()

	price_index = 0
	url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price) + "&search[filter_float_m:from]="+str(min_re)+"&search[filter_float_m:to]="+str(max_re)

	sum_ads = 0
	while (sum_ads<nr_ads) and max_re < 30000:
		print cate_name,url,"INSIDEEE"
		# print url
		try:
			while True:
				try:
					source_code = requests.get(url,headers=header)
					break
				except:
					pass
			plain_text = source_code.text
			soup = BeautifulSoup(plain_text,"lxml")
			
			f = open("real_estate.txt","w+")
			f.write(str(min_re)+" "+str(max_re))
			f.close()

			total_ads = int(plain_text.split('totalAds')[1].split("'")[1].replace('.','').replace(',','').replace(' ',''))
			
			print total_ads,"INSIDEE",sum_ads,url

			if total_ads <= 5000:
				tmp = max_re - min_re
				min_re += tmp + 1
				max_re += tmp + 1
				sum_ads += collect_ads(url)
				url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price) + "&search[filter_float_m:from]="+str(min_re)+"&search[filter_float_m:to]="+str(max_re)


			elif max_re-min_re == 1:
				sum_ads += collect_ads(url+"&search%5Border%5D=filter_float_price%3Adesc")
				sum_ads += collect_ads(url+"&search%5Border%5D=filter_float_price%3Aasc")
			
			else:
				max_re = max_re - 1
				url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price) + "&search[filter_float_m:from]="+str(min_re)+"&search[filter_float_m:to]="+str(max_re)


		except:
			print url,"re except"
			tmp = max_re - min_re
			min_re += tmp + 1
			max_re += tmp + 10
			url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price) + "&search[filter_float_m:from]="+str(min_re)+"&search[filter_float_m:to]="+str(max_re)
			
	f = open("real_estate.txt","w+")
	f.write("1 10")
	f.close()		
	
	return sum_ads

# def normal(url,cate_name,cat_link,min_price,nr_ads):
# 	min_re = 1
# 	max_re = 10
# 	price_index = 0
# 	end_price = float(min_price) + float(0.99)
# 	min_price = float(min_price)
# 	max_price = float(min_price) + float(0.10)

# 	url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(max_price)

# 	sum_ads = 0
# 	while (sum_ads<nr_ads) and min_price <= end_price:
# 		print cate_name,cat_link,"INSIDEEE"
# 		print url
# 		try:
# 			while True:
# 				try:
# 					source_code = requests.get(url,headers=header)
# 					break
# 				except:
# 					pass
# 			plain_text = source_code.text
# 			soup = BeautifulSoup(plain_text,"lxml")
			
# 			f = open("price.txt","w+")
# 			f.write(str(min_price)+" "+str(price_index))
# 			f.close()

# 			total_ads = int(soup.find('div',{"class":"result-text"}).findAll('span')[-1].text.strip().split(' ')[0].replace('.',''))
			
# 			print total_ads,"INSIDEE",sum_ads,url

# 			if total_ads <= 5000:
# 				min_price = max_price + 0.01
# 				max_price += 0.10
# 				sum_ads += collect_ads(url)
# 				url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(max_price)


# 			else:
# 				sum_ads += collect_ads(url+"&sorting=price_desc")
# 				sum_ads += collect_ads(url+"&sorting=price_asc")

# 				min_price = max_price + 0.01
# 				max_price += 0.10
# 				url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(max_price)

# 		except:
# 			min_price = max_price + 0.01
# 			max_price += 0.10
# 			url = cat_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(max_price)
# 			print url,"normal except"
			
	
# 	return sum_ads

def get_cat(cate_name,cate_link):
	global same_numbers
	source_code = requests.get(cate_link,headers=header)
	plain_text = source_code.text
	soup = BeautifulSoup(plain_text,"lxml")
	current_round = int(open("round.txt").readline())

	try:
		total_ads_main = int(plain_text.split('totalAds')[1].split("'")[1].replace('.','').replace(',','').replace(' ',''))
	except:
		total_ads_main = 0

	if 'param_m"' in plain_text:
		status = 'real estate'
	elif 'param_milage' in plain_text:
		status = 'cars'
	else:
		status = 'normal'
	so_far_total = 0
	if total_ads_main <= 5000:
		so_far_total += collect_ads(cate_link)
	
	else:
		f = open('price.txt','r+')
		line = f.readline()#.split(' ')[0]
		min_price = int(line.split(' ')[0])
		price_index = int(line.split(' ')[1])
		f.close()

		# url = cate_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price)
		# source_code = requests.get(url,headers=header)
		# plain_text = source_code.text
		# soup = BeautifulSoup(plain_text,"lxml")

		while True: #same_numbers<=500:
			try:
				if same_numbers>= 1500 and current_round>1:
					print "1500 same numbers in a row, breaking olx"
					same_numbers = 0
					break
				if total_ads_main*2 < so_far_total:
					break
				
				f = open("price.txt","w+")
				f.write(str(min_price)+" "+str(price_index))
				f.close()

				url = cate_link  + "&search[filter_float_price:from]="+str(min_price)+"&search[filter_float_price:to]="+str(price_inc[price_index]+min_price)
				print url,cate_name,so_far_total,total_ads_main
				
				while True:
					try:
						source_code = requests.get(url,headers=header)
						break
					except:
						pass
				plain_text = source_code.text
				soup = BeautifulSoup(plain_text,"lxml")

				total_ads = int(plain_text.split('totalAds')[1].split("'")[1].replace('.','').replace(',','').replace(' ',''))
				print "total on this page",url,cate_name,total_ads
				if total_ads <= 5000: 
					min_price = price_inc[price_index]+min_price+1
					price_index += 1
					so_far_total += collect_ads(url)
					
					
				else:
					print price_index,status,"price index i status"
					if price_index > 0:

						price_index -= 1

					elif status == 'real estate':
						so_far_total += real_estate_call(url,cate_name,cate_link,min_price,total_ads)

						min_price = price_inc[price_index]+min_price+1

					elif status == 'cars':
						print "avtoo"
						so_far_total += cars(url,cate_name,cate_link,min_price,total_ads)

						min_price = price_inc[price_index]+min_price+1

					else:
						print "going to normal"
						so_far_total += collect_ads(url+"&search%5Border%5D=filter_float_price%3Adesc")
						so_far_total += collect_ads(url+"&search%5Border%5D=filter_float_price%3Aasc")
						min_price = price_inc[price_index]+min_price+1
						# so_far_total += normal(url,cate_name,cate_link,min_price,total_ads)
						# min_price += 1.01


			except:
				print "basic except"
				min_price += 1
				price_index += 1
				
				if price_index >= 20:
					break
		f = open("cat_done.txt","a+")
		f.write("Total for " + cate_link  + "####"+str(so_far_total)+"\n")
		f.close()

		
		
		f = open("price.txt","w+")
		f.write("1 0")
		f.close()

while True:

	# source_code = requests.get("https://www.olx.ua",headers=header)
	# plain_text = source_code.text
	# soup = BeautifulSoup(plain_text,"lxml")

	# cat_link = []
	# cat_name = []
	# for a in soup.findAll('div',{"class":"subcategories-list"}):
	# 	for b in a.findAll('a')[1:]:
	# 		if b.get('href')+"?search%5Bprivate_business%5D=private" not in cat_link:
	# 			cat_name.append(a.find('div',{"class":"subcategories-title"}).text.strip().split(' w ')[-1].replace('\n','')+"----"+b.find('span').text.strip())
	# 			cat_link.append(b.get('href')+"?search%5Bprivate_business%5D=private")

	# stat_len = len(cat_link)
	# for i in range(0,stat_len):
	# 	source_code = requests.get(cat_link[i],headers=header)
	# 	plain_text = source_code.text
	# 	soup = BeautifulSoup(plain_text,"lxml")
	# 	changed = 0
	# 	try:
	# 		for a in soup.find('div',{"class":"toplinks"}).findAll('a'):
	# 			if a.get("href") not in cat_link:
	# 				cat_name.append(cat_name[i]+"----"+a.text.strip())
	# 				cat_link.append(a.get("href"))
	# 				changed = 1
	# 	except:
	# 		pass
	# 	if changed == 1:
	# 		cat_name[i] = ''
	# 		cat_link[i] = ''
	# stat_len = len(cat_link)	
	# for i in range(0,stat_len):
	# 	if len(cat_link[i]) < 5:
	# 		continue
	# 	# print cat_name[i]
	# 	# print cat_link[i]
	# 	source_code = requests.get(cat_link[i],headers=header)
	# 	plain_text = source_code.text
	# 	soup = BeautifulSoup(plain_text,"lxml")
	# 	changed = 0
	# 	try:
	# 		for a in soup.find('div',{"class":"locationlinks"}).findAll('a'):
	# 			if a.get("href") not in cat_link:
	# 				cat_name.append(cat_name[i]+"----"+a.text.strip())
	# 				cat_link.append(a.get("href"))
	# 				changed = 1
	# 	except:
	# 		pass
	# 	if changed == 1:
	# 		cat_name[i] = ''
	# 		cat_link[i] = ''
	# 		#print 'changed'

	# i = 0
	# while i < len(cat_link):
	# 	if cat_name[i] == '' or cat_link[i]=='#':
	# 		cat_name.remove(cat_name[i])
	# 		cat_link.remove(cat_link[i])
	# 		i = 0
	# 	else:
	# 		i += 1
	# # cat_name.append('https://www.olx.ua/motoryzacja/car-audio/?search%5Bprivate_business%5D=private')
	# # cat_link.append('https://www.olx.ua/motoryzacja/car-audio/?search%5Bprivate_business%5D=private')
	# for i in range(0,len(cat_link)):
	# 	print cat_name[i]+'###'+cat_link[i]
	# 	# print cat_link[i]
	# sys.exit()
	cat_name = []
	cat_link = []
	f = open("cats_refined.txt","r+")
	for m in f.readlines():
		cat_name.append(m.split('#')[0].replace(' ',''))
		cat_link.append(m.split('#')[-1].replace(' ','').replace('\n',''))

	f.close()
	ad_updated_on = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	run_id = scraping_history('new')
	domain = scraper.split('www.')[-1]
	started_on = datetime.now()
	print len(cat_link),"total links"
	cat = open("category_done.txt").readline()
	try:
		start_cat = cat_link.index(cat)+1
	except:
		start_cat = 0
	print "Start cat index",start_cat
	for i in range(start_cat,len(cat_link)):
		same_numbers = 0
		print cat_name[i],cat_link[i],"STARTEED"
		get_cat(cat_name[i],cat_link[i])

		f = open('category_done.txt',"w+")
		f.write(cat_link[i])
		f.close()
	
	current_round = int(open("round.txt").readline())+1
	f = open("round.txt","w+")
	f.write(str(current_round))
	f.close()

	f = open('category_done.txt',"w+")
	f.write("will start again")
	f.close()

	ended_on = datetime.now()
	sec_to_wait = 604800 - (ended_on - started_on).total_seconds()
	if sec_to_wait < float(1):
		sec_to_wait = 1
	print "done scraping mobile.de, now sleeping",sec_to_wait
	ads = []
	ad_updated_on = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	run_id = scraping_history('new')
	domain = scraper.split('www.')[-1]
	started_on = datetime.now()
	same_numbers = 0
	time.sleep(sec_to_wait)
