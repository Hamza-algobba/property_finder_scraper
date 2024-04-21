#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
import pandas as pd
pd.options.display.max_rows = None
pd.options.display.max_columns = None
import datetime 
from dateutil.relativedelta import relativedelta
import requests
import time
import re
from bs4 import BeautifulSoup


# In[2]:


HEADERS = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/106.0.1370.42'}
initial_URL = "https://www.propertyfinder.eg/en/search?c=1&ob=nd&page=1&pf=1&pt=3000000"
URL = initial_URL
base_url = "https://www.propertyfinder.eg/en/search?c=1&ob=nd&page="
url_tail = "&pf=1&pt=3000000"


# In[4]:


def get_soup(URL):
    page_response = requests.get(URL,headers=HEADERS)
    #print(page_response.url)
    soup = BeautifulSoup(page_response.content, "html.parser")
    return soup
    #print(soup.prettify())


# In[5]:


def increment_page(counter):
    URL = f"{base_url}{str(counter + 1)}{url_tail}"
    return URL
    


# In[6]:


def get_past_date(str_days_ago): #code I found online to convert x days ago to listing date
    TODAY = datetime.date.today()
    splitted = str_days_ago.split()
    if len(splitted) == 1 and splitted[0].lower() == 'today':
        return str(TODAY.isoformat())
    elif len(splitted) == 1 and splitted[0].lower() == 'yesterday':
        date = TODAY - relativedelta(days=1)
        return str(date.isoformat())
    elif splitted[1].lower() in ['minutes', 'minute', 'min']:
        date = datetime.datetime.now() - relativedelta(minutes=int(splitted[0]))
        return str(date.date().isoformat())
    elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
        date = datetime.datetime.now() - relativedelta(hours=int(splitted[0]))
        return str(date.date().isoformat())
    elif splitted[1].lower() in ['day', 'days', 'd']:
        date = TODAY - relativedelta(days=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
        date = TODAY - relativedelta(weeks=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
        date = TODAY - relativedelta(months=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
        date = TODAY - relativedelta(years=int(splitted[0]))
        return str(date.isoformat())
    else:
        return "Wrong Argument format"
    


# In[7]:


#function that doesn't allow duplicates in the csv file
def csv_exist(facts_list, file):
    pk = facts_list[0]
    file_reader = csv.reader(file)
    for line in file_reader:
        if line[0] == pk:
            return True;
            break;
    return False;



# In[8]:


def price_exists(div): #returns false if property price is not specified
    p = div.find("p", {"class": "card-intro__price"}).text
    pattern = re.compile(r'\bEGP\b')
    if(len(re.findall(pattern, p)) == 0):
        return False
    else :
        return True


# In[9]:


properties_facts = [["null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null"]]
amenities_facts = [["null", "null"]]
listing_agents_facts = [["null", "null", "null"]]
broker_companies_facts = [["null", "null", "null", "null"]]
dev_projects_facts = [["null", "null"]] 


# In[10]:


def clear_dataframes(lst1,lst2,lst3,lst4,lst5):
    lst1.clear()
    lst2.clear()
    lst3.clear()
    lst4.clear()
    lst5.clear()
    lst1.append(["null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null"])
    lst2.append(["null", "null"])
    lst3.append(["null", "null", "null"])
    lst4.append(["null", "null", "null", "null"])
    lst5.append(["null", "null"])
    


# In[11]:


def update_df(df, x_facts):
    df = df.append(pd.DataFrame(x_facts))
    df = df.drop_duplicates()
    


# In[126]:


#devprojects_df.drop(devprojects_df.index,inplace=True)
devprojects_df = pd.DataFrame(dev_projects_facts)
devprojects_df.columns = ['Project Name', 'Project Location']
devprojects_df = devprojects_df.drop_duplicates()
devprojects_df
#devprojects_df.to_csv(r'D:\property_finder_db\Development_projects - Sheet1.csv')


# In[107]:


#amenities_df.drop(amenities_df.index,inplace=True)
amenities_df = pd.DataFrame(amenities_facts)
amenities_df.columns = [ 'Reference Number','amenity description']
amenities_df = amenities_df.drop_duplicates()
amenities_df
amenities_df.to_csv(r'D:\property_finder_db\amenities - Sheet1.csv')


# In[108]:


#agents_df.drop(agents_df.index,inplace=True)
agents_df = pd.DataFrame(listing_agents_facts)
agents_df.columns = ['Listing agent phone number', 'Agent name', 'Broker company phone number']
agents_df = agents_df.drop_duplicates()
agents_df
agents_df.to_csv(r'D:\property_finder_db\Listing_agents - Sheet1.csv')


# In[144]:


#broker_df.drop(broker_df.index,inplace=True)
broker_df = pd.DataFrame(broker_companies_facts)
broker_df.columns = ['Broker Phone Number', 'Broker Company Name', 'Company Address', 'Number of listed properties']
broker_df = broker_df.drop_duplicates()

for ind in broker_df.index:
    corrupted_address = broker_df.at[ind,'Company Address']
    corrupted_address = corrupted_address.replace('\n', '')
    corrupted_address = corrupted_address.replace(',', '')
    corrupted_address = corrupted_address.replace('،', '')
    
    print(corrupted_address)
    broker_df.at[ind,'Company Address'] = corrupted_address
broker_df.drop(['Company Address'], axis = 1, inplace = True)
broker_df.to_csv(r'D:\property_finder_db\Broker_companies - Sheet1.csv')
broker_df


# In[142]:


#properties_df.drop(properties_df.index,inplace=True)
properties_df = pd.DataFrame(properties_facts)
properties_df.columns = ['Reference Number', 'Property Type', 'Location', 'Area', 'Description', 'Number of Bathrooms', 'Price','Listing Date','Number of Bedrooms', 'Deveopment Project Name',  'Listing Agent Phone Number']
properties_df = properties_df.drop_duplicates()

for ind in properties_df.index:
    corrupted_address = properties_df.at[ind,'Location']
    corrupted_address = corrupted_address.replace(',', '')
    
    print(corrupted_address)
    properties_df.at[ind,'Location'] = corrupted_address

properties_df.drop(['Description'], axis = 1, inplace = True)
properties_df.to_csv(r'D:\property_finder_db\Properties - Sheet1.csv')
properties_df


# In[35]:


def get_property_info(page_soup):
    
    property_facts = []
    listing_agent_facts = []
    separator = '' #used to join lists
    
    reference_no = page_soup.find("div", {"class": "property-page__legal-list-content"})
    property_facts.append(reference_no.text)
    print("reference number = ", reference_no.text)
    
    if(page_soup.find("span", text = "Property type:")):
        ptype = page_soup.find("span", text = "Property type:")
        property_type = ptype.parent.next_sibling.text
        pptypepattern = re.compile(r'\w+') 
        pptype = separator.join(re.findall(pptypepattern, property_type))
        property_facts.append(pptype)
        print("property type: ", pptype)
    else:
        property_facts.append("null")
    
    location = "\"" #adding quotation marks to ignore any commas in the Location text 
    location += page_soup.find("div", {"class": "property-location__detail-area"}).text
    location += "\""
    property_facts.append(location)
    print( "\nlocation = ", location)
    
    if(page_soup.find("span", text = "Property size:")):
        area = page_soup.find_all("span", {"class": "property-facts__text"}) #look ahead not working for some reason
        area_pattern = re.compile(r'\b[0-9]+\s')
        area_pure = int(re.findall(area_pattern, area[1].text)[0]) #to eliminate sqm
        property_facts.append(area_pure)
        print("area: ", area_pure)
    else:
        property_facts.append("null")
    
    if(page_soup.find("div", {"class": "property-page__description"})):
        unedited_description = page_soup.find("div", {"class": "property-page__description"})
        description = "\"" #adding quotation marks to ignore any commas in the descrption text 
        description += re.sub(r'.', '', unedited_description.text, count = 11) #remove the word description from the body
        description += "\""
        property_facts.append(description)
        print( "\ndescription: ", description)
    else:
        property_facts.append("null")
    
    if(page_soup.find("span", text = "Bathrooms:")):
        bathrooms = page_soup.find("span", text = "Bathrooms:")
        no_of_bathrooms = bathrooms.parent.next_sibling.text
        property_facts.append(no_of_bathrooms)
        print( "\nnumber of bathrooms: ", no_of_bathrooms)
    else:
        property_facts.append("null")
    
    prices = page_soup.find("div", {"class": "property-price__price"})
    price_pattern = re.compile(r'[0-9]+')
    pure_price = re.findall(price_pattern, prices.text) 
    price = separator.join(pure_price) #to eliminate thousand separators
    property_facts.append(price)
    print("price: ", price)
    
    listing_date = page_soup.find_all("div", {"class": "property-page__legal-list-content"})[1].text
    listing_date = get_past_date(listing_date)
    property_facts.append(listing_date)
    print("Listing date: ", listing_date)
    
    if(page_soup.find("div", {"class": "property-payment-plan"})):
        print("here\n\n\n\n")
        down_payment = page_soup.find_all("div", {"class": "property-payment-plan__value"})[0].text
        monthly_payment = page_soup.find_all("div", {"class": "property-payment-plan__value"})[1].text
        paid_over = page_soup.find_all("div", {"class": "property-payment-plan__value"})[2].text
        #property_facts.append(down_payment)
        #property_facts.append(monthly_payment)
        #property_facts.append(paid_over)
        print("down payment: ", down_payment)
        print("monthly payment: ", monthly_payment)
        print("paid over ", paid_over)
    
    if(page_soup.find("span", text = "Bedrooms:")):
        bedrooms = page_soup.find("span", text = "Bedrooms:")
        no_of_bedrooms = bedrooms.parent.next_sibling.text
        no_of_bedrooms = no_of_bedrooms.replace(" ", "")
        no_of_bedrooms = no_of_bedrooms.replace("\t", "")
        no_of_bedrooms = no_of_bedrooms.replace("\n", "")
        property_facts.append(no_of_bedrooms)
        print("number of bedrooms = ", no_of_bedrooms)
    else:
        property_facts.append("null")
        
    if(page_soup.find("div", {"class": "property-project-details__title"})):
        dev_project_facts = []
        dev_project_name = page_soup.find("div", {"class": "property-project-details__title"}).text
        dev_project_location = page_soup.find("div", {"class": "property-project-details__location"}).text
        property_facts.append(dev_project_name)
        dev_project_facts.append(dev_project_name)
        dev_project_facts.append(dev_project_location)
        print("development project name: ", dev_project_name)
        print("development project location: ", dev_project_location)
        dev_projects_facts.append(dev_project_facts)
    else:
        property_facts.append("null")

        
        
    
    
    script_tag = page_soup.find_all("script", {"type": "application/ld+json"})[1]
    telepattern = re.compile(r'(?<=telephone":").?[0-9]+')
    listing_agent_phone = re.findall(telepattern, script_tag.text)[1]
    property_facts.append(listing_agent_phone)
    listing_agent_facts.append(listing_agent_phone)
    print("Listing agent phone number: ",listing_agent_phone)
    
    listing_agent_name = page_soup.find("h4").text
    listing_agent_facts.append(listing_agent_name)
    print("name agent", listing_agent_name)
    
    if(page_soup.find("picture", {"class": "property-agent__broker-image"}) ):
        broker_company_facts = []
        broker_company_name = page_soup.find("div", {"class": "property-agent__position-broker-name"}).text
        broker_company_name = broker_company_name.replace('\n', '')
        if(page_soup.find("a", {"class": "link link--underline"}, {"text": re.compile(r'[0-9]+(?=(\spropert(y|ies)\b))')})):
            number_of_listed_properties_tag = page_soup.find("a", {"class": "link link--underline"}, {"text": re.compile(r'[0-9]+(?=(\spropert(y|ies)\b))')})
            number_of_listed_properties = number_of_listed_properties_tag.text
            number_of_listed_properties = number_of_listed_properties.split(' ')[0]
            number_of_listed_properties = number_of_listed_properties.replace(',','')
            
            print("no listed: ", number_of_listed_properties)
            broker_company_link = "https://www.propertyfinder.eg"
            broker_company_link += number_of_listed_properties_tag['href']
            broker_response = requests.get(broker_company_link, headers=HEADERS)
            broker_soup = BeautifulSoup(broker_response.content, "html.parser")
            broker_info = broker_soup.find("div", {"class": "bio-info__details"})
            broker_address = "\""
            broker_address += broker_info.find_all("span")[1].text
            broker_address += "\""
            broker_phone = broker_soup.find("span", {"class": "button__text button__text-value button__phone-ltr button__text--is-hidden"}).text
            listing_agent_facts.append(broker_phone)
            broker_company_facts.append(broker_phone)
            broker_company_facts.append(broker_company_name)
            broker_company_facts.append(broker_address)
            broker_company_facts.append(number_of_listed_properties)
            print("broker company phone: ",  broker_phone)
            print("broker company address: ",  broker_address)
            print("broker company link: ",  broker_company_link)
            print("broker name: ", broker_company_name)
        else:
            listing_agent_facts.append("null")
            broker_company_facts.append("null")
            broker_company_facts.append("null")
            broker_company_facts.append("null")
        broker_companies_facts.append(broker_company_facts)
    
    
    
    loa = page_soup.find_all("div", {"class": "property-amenities__list"})
    for x in loa: #loa is an abbreviation for list of amenities the raw version
        amenity = []
        amenity_description = x.text
        amenity_description = amenity_description.replace('\n', '')
        amenity.append(reference_no.text)
        print("amenity refno: ",reference_no.text)
        amenity.append(amenity_description)
        print("amenity length: ", len(amenity) )
        print("amenity: ",amenity_description)
        amenities_facts.append(amenity)
    
    print("length = ", len(property_facts))
    properties_facts.append(property_facts)
    listing_agents_facts.append(listing_agent_facts)
    


# In[36]:


def get_divs(p_soup):
    divs = p_soup.find_all("div", {"class": "card-list__item"})
    return divs


# In[94]:


def get_page_properties(divs): 
    counter = 0
    for div in divs:
        if ((len(div.find_all("footer")) != 0) and (price_exists(div)) ):
            #above conditions used to eliminate properties with no price listed and properties that are on the page as paid promotions
            counter += 1
            print(counter)
            list_date = div.find("p", {"class": "card-footer__publish-date"})
            regexp = re.compile(r'\bhour\b|\bhours\b|\bminute\b|\bminutes\b|\bday\b|\b([2-9]|1[0-3])\b\s\bdays\b')
            if(regexp.search(list_date.text)):
                a_tag = div.find("a")
                property_link = "https://www.propertyfinder.eg"
                property_link += a_tag['href']
                print(property_link)
                page = requests.get(property_link, headers=HEADERS)
                page_soup = BeautifulSoup(page.content, "html.parser")
                if(page_soup.find("img", {"class": "property-gone__image property-gone__image--en"})): #for properties that have been delisted
                    continue
                if(property_link == r'https://www.propertyfinder.eg/en/plp/buy/chalet-for-sale-suez-al-ain-al-sokhna-la-vista-la-vista-6-3504992.html'): #edge case
                    continue
                    
                if(property_link == r'https://www.propertyfinder.eg/en/plp/buy/apartment-for-sale-cairo-ring-road-tag-sultan-3498032.html'): #edge case
                    continue
                if(property_link == r'https://www.propertyfinder.eg/en/plp/buy/apartment-for-sale-cairo-new-cairo-city-al-rehab-el-rehab-extension-3492776.html'): #edge case
                    continue
                    
                if (property_link == r'https://www.propertyfinder.eg/en/plp/buy/chalet-for-sale-suez-al-ain-al-sokhna-la-vista-la-vista-3-3492278.html'): #edge case
                    continue
                    
                if(property_link == r'https://www.propertyfinder.eg/en/plp/buy/chalet-for-sale-suez-al-ain-al-sokhna-azha-3491347.html'): #edge case
                    continue
                    
                if(property_link == r'https://www.propertyfinder.eg/en/plp/buy/chalet-for-sale-suez-al-ain-al-sokhna-la-vista-la-vista-topaz-3490254.html'): #edge case
                    continue
                    
                if(property_link == r'https://www.propertyfinder.eg/en/plp/buy/apartment-for-sale-cairo-new-cairo-city-north-investors-area-fifth-square-3487437.html'): #edge case
                    continue
                  
                if(property_link == r'https://www.propertyfinder.eg/en/plp/buy/apartment-for-sale-cairo-hay-el-maadi-maadi-cornish-el-nile-st-3481717.html'): #edge case
                    continue
                get_property_info(page_soup)
                print(len(properties_facts))
            else:
                in_date_range = False
                break
     
    update_df(devprojects_df, dev_projects_facts )
    #copy_df_to_csv(r'D:\property_finder_db\Development_projects - Sheet1.csv',   devprojects_df)
    update_df(amenities_df, amenities_facts)
    #copy_df_to_csv(r'D:\property_finder_db\amenities - Sheet1.csv',  amenities_df)
    update_df(agents_df, listing_agents_facts)
    #copy_df_to_csv(r'D:\property_finder_db\Listing_agents - Sheet1.csv',  agents_df)
    update_df(broker_df, broker_companies_facts)
    #copy_df_to_csv(r'D:\property_finder_db\Broker_companies - Sheet1.csv',  broker_df)
    update_df(properties_df, properties_facts)
    #copy_df_to_csv(r'D:\property_finder_db\Properties - Sheet1.csv',  properties_df)
    #clear_dataframes(properties_facts, amenities_facts, listing_agents_facts, broker_companies_facts, dev_projects_facts)
    


# In[95]:


page_counter = 302#I update this number whenever connection is lost, if you're building the program for the first time, set it to 1
in_date_range = True
while(in_date_range):
    soup = get_soup(URL)
    my_divs = get_divs(soup)
    get_page_properties(my_divs)
    URL = increment_page(page_counter)
    page_counter += 1
    
    


# In[88]:


print(page_counter) #to know the page number where the


# In[112]:


from faker import Faker


# In[115]:


faker_obj = Faker()
gender = 'M'
lst = []
for i in range(1000): # code I found online to generate fake user info
    male_profile = faker_obj.simple_profile(sex=gender)
    print("Male profile generated is as follows:\n", male_profile)
    lst.append(male_profile)
    gender = 'F'
    male_profile = faker_obj.simple_profile(sex=gender)
    print("Female profile generated is as follows:\n", male_profile)
    lst.append(male_profile)
    


# In[122]:


user_df.drop(user_df.index,inplace=True)


# In[124]:


user_df = pd.DataFrame(lst)
user_df.drop(['name', 'address'], axis = 1, inplace = True)
user_df = user_df.drop_duplicates()
user_df.to_csv(r'D:\property_finder_db\Users - Sheet1.csv')
user_df


# In[146]:


import random


# In[252]:


ref_agent = random.choice(properties_facts)
print(ref_agent[0], ref_agent[10])
print(random.choice(lst)['name'])
print(random.randint(0,10))
fake = Faker()
print(fake.date_between_dates(date_start=datetime.date(2015,1,1), date_end=datetime.date(2021,12,31)))


# In[256]:


saves_df.drop(saves_df.index,inplace=True)


# In[266]:


saves_fact = []
for i in range(500):
    ref_agent = random.choice(properties_facts)
    ref_agent_2 = []
    ref_agent_2.append(random.choice(lst)['name'])
    ref_agent_2.append(ref_agent[0])
    
    saves_fact.append(ref_agent_2)
    
    
    
saves_df = pd.DataFrame(saves_fact)
saves_df.to_csv(r'D:\property_finder_db\Saves - Sheet1.csv')
saves_df


# In[262]:


contacts_facts = []
for i in range(200):
    ref_agent = random.choice(properties_facts)
    ref_agent_2 = []
    ref_agent_2.append(random.choice(lst)['name'])
    ref_agent_2.append(ref_agent[0])
    ref_agent_2.append(ref_agent[10])
    
    contacts_facts.append(ref_agent_2)
    
    
contacts_df = pd.DataFrame(contacts_facts)
contacts_df.to_csv(r'D:\property_finder_db\Contacts - Sheet1.csv')
contacts_df


# In[264]:


reviews_facts = []
for i in range(200):
    ref_agent = random.choice(properties_facts)
    ref_agent_2 = []
    ref_agent_2.append(random.choice(lst)['name'])
    ref_agent_2.append(ref_agent[10])
    ref_agent_2.append(fake.date_between_dates(date_start=datetime.date(2015,1,1), date_end=datetime.date(2021,12,31)))
    ref_agent_2.append(random.randint(0,10))
    reviews_facts.append(ref_agent_2)
reviews_df = pd.DataFrame(reviews_facts)
reviews_df.to_csv(r'D:\property_finder_db\Reviews - Sheet1.csv')
reviews_df
    

