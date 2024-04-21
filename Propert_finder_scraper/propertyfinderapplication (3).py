#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pypyodbc as odbc
import datetime as dt
from datetime import datetime
from dateutil import parser
import time


# In[2]:


DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = 'sql8001.site4now.net'
DATABASE_NAME = 'db_a8ff60_propfinder'
UID = 'db_a8ff60_propfinder_admin'
PASSWORD = 'milestone3db'


# In[3]:


connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trust_Connection=yes;
    uid={UID};
    pwd={PASSWORD};
"""


# In[4]:


conn = odbc.connect(connection_string)
print(conn)


# In[5]:


def read(conn, table):
    print("Read")
    cursor = conn.cursor()
    cursor.execute(f"select * from {table};")
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()


# In[6]:


def register_user(conn,UID,name,email,birthdate,gender):
    print("Register User")
    cursor = conn.cursor()
    cursor.execute(
        'insert into users values (?,?, ?, ? ,?);',
        (UID,name,email,datetime.strptime(birthdate, '%Y-%m-%d'),gender))
    conn.commit()
    read(conn,'users')


# In[7]:


def add_review(conn,reviewid, email,agentphone, rating):
    print("Add review")
    cursor = conn.cursor()
    cursor.execute(
        'insert into reviews values (?,?, ?, ? ,?);',
        (reviewid,email,agentphone,dt.date.today(), rating))
    conn.commit()
    read(conn,'reviews')


# In[8]:


def view_reviews(conn,agent_phone):
    print(f"View reviews on agent {agent_phone}")
    cursor = conn.cursor()
    cursor.execute('select * from reviews where agent_phone = ?;', [agent_phone])
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()
    


# In[9]:


def view_broker_rating(conn, broker_phone):
    print(f"View average broker rating of broker {broker_phone}")
    cursor = conn.cursor()
    cursor.execute('''select b.broker_phone, avg(r.rating) as "average broker rating" from broker_companies b inner join listing_agents l on l.broker_company_phone_number = b.broker_phone inner join reviews r on l.agent_phone_number = r.agent_phone
    where b.broker_phone = ?
    group by b.broker_phone
    order by avg(r.rating);''', [broker_phone])
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()


# In[10]:


def get_dev_project_ppsqm(conn, project_name):
    print(f"View Location, average price per sqm for each unit type by {project_name}")
    cursor = conn.cursor()
    cursor.execute('''select d.project_location, p.unit_type, avg(cast(price as bigint)/cast(area as int)) as "price per sqm"
    from development_projects d inner join properties p on p.development_project = d.dev_project_name
    where d.dev_project_name = ?
    group by d.project_location, p.unit_type
    order by "price per sqm" desc;''', [project_name])
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()


# In[11]:


def get_top_5_brokers(conn):
    print(f"View top 5 broker companies with respect to amount of listings,average price/sqm, number of agents, number of listings/agent")
    cursor = conn.cursor()
    cursor.execute('''select top 5 b.broker_company_name , count(p.agent_phone) as "number of listings", avg(cast(p.price as bigint)/cast(p.area as int)) as "average price/sqm", count(l.agent_phone_number) as "number of listing agents", count(p.agent_phone)/count(distinct l.agent_phone_number) as "average listings per agent" from broker_companies b inner join listing_agents l on l.broker_company_phone_number = b.broker_phone inner join properties p on l.agent_phone_number = p.agent_phone
    group by b.broker_company_name
    order by count(p.agent_phone) desc,
     avg(cast(p.price as bigint)/cast(p.area as int))  desc, 
    count(l.agent_phone_number) desc,
    count(p.agent_phone)/count(distinct l.agent_phone_number) desc;''')
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()


# In[12]:


def get_properties_by_agent(conn, agent_phone, agent_name = ''):
    if len(agent_name) != 0:
        agent_info = '\''+ agent_phone + '\'  and l.agent_name = \'' + agent_name +'\''
        print(agent_info)
    else:
        agent_info = "'"+agent_phone + "'"
    print(agent_name)
    print(f"View properties listed by agent {agent_phone}")
    cursor = conn.cursor()
    cursor.execute(
    "select p.id, p.reference_number, p.unit_type, p.location, p.area, p.number_of_bathrooms, p.price, p.listing_date, p.number_of_bedrooms, p.development_project, p.agent_phone from properties p inner join listing_agents l on l.agent_phone_number = p.agent_phone where p.agent_phone = %s;" % (agent_info)
    )
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()


# In[13]:


def view_city_properties(conn, city_name):
    city_name = r'%' + city_name + r'%'
    print(f"View properties and and average price/sqm for each unit type in {city_name}")
    cursor = conn.cursor()
    cursor.execute('''select * from properties join
    (select [Unit_type] as "unit_type",  avg(cast(price as bigint)/cast(area as int)) as "average price/sqm" from properties
     where properties.location like ?
     group by [Unit_Type]) t  on properties.unit_type = t.unit_type
    where properties.location like ?;''', [city_name,city_name])
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()


# In[14]:


def get_property_amenities(conn,amenitieslst, city_name, lower_bound, upper_bound):
    city_name = "'"+ r'%' + city_name + r'%'+ "'"
    lower_bound = f'cast({lower_bound} as bigint)' 
    upper_bound = f'cast({upper_bound} as bigint)' 
    amenities = "'"+ "', '".join(amenitieslst) + "'"
    print(amenities)
    size = len(amenitieslst)
    print(f"your results:")
    cursor = conn.cursor()
    cursor.execute("select p.id, p.reference_number, p.location, p.number_of_bathrooms, p.price, p.area, p.listing_date, p.development_project, p.unit_type, p.number_of_bedrooms from properties p inner join (select reference_number,  count(amenity_description) as \"number of amenities\" from amenities where amenity_description in ( %s ) group by reference_number having count(amenity_description) >= %s) as a on a.reference_number = p.reference_number where p.price > %s and p.price < %s and p.location like %s;"% ( amenities, size,lower_bound, upper_bound, city_name))
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()


# In[15]:


def get_top_10_areas(conn, unit_type, city):
    city =  r'%' + city + r'%'
    print(f"View top 10 areas in {city}")
    cursor = conn.cursor()

    cursor.execute('''select top 10 p.location , avg(cast(p.price as bigint)/cast(p.area as int)) as "average price/sqm" , count(p.id) as "inventory" from properties p
    where p.unit_type = ? and p.location like ?
    group by p.location
    order by count(p.id) desc, avg(cast(p.price as bigint)/cast(p.area as int)) desc''', [unit_type,city])
    columns = [column[0] for column in cursor.description]
    
    print(','.join(columns))
    for row in cursor:
        print(f'row = {row}')
    print()


# In[16]:


def menu_1():
    UID = input("Please enter your ID")
    email = input("Please enter your email")
    name = input("Please enter your name")
    birthdate = input("Please enter your birthdate")
    gender = input("Please enter your gender")
    register_user(conn,UID,name,email,birthdate,gender)
    choice = input('''0: exit
                      1:return to main menu
                      ''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")


# In[17]:


def menu_2():
    reviewid = input("enter review ID")
    email = input("enter your email")
    agent_phone = input("enter the listing agent's phone number")
    rating = input("enter a rating from 1 to 10 for that agent")
    add_review(conn,reviewid, email,agent_phone, rating)
    choice = input('''0: exit
                      1:return to main menu
                      2: add another rating''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    elif choice == '2':
        menu_2()
    


# In[18]:


def menu_3():
    agent_phone = input("please enter the agent's phone number")
    view_reviews(conn,agent_phone)
    choice = input('''0: exit
                      1:return to main menu
                      2: see another rating''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    elif choice == '2':
        menu_3()


# In[19]:


def menu_4():
    broker_phone = input("please enter the broker's phone number")
    view_broker_rating(conn, broker_phone)
    choice = input('''0: exit
                      1:return to main menu
                      2: see another rating''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    elif choice == '2':
        menu_4()


# In[20]:


def menu_5():
    project_name = input("please enter the name of the development project")
    get_dev_project_ppsqm(conn, project_name)
    choice = input('''0: exit
                      1:return to main menu
                      2: see another project''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    elif choice == '2':
        menu_5()


# In[21]:


def menu_6():
    city_name = input("please enter city name")
    view_city_properties(conn, city_name)
    choice = input('''0: exit
                      1:return to main menu
                      2: see another city''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    elif choice == '2':
        menu_6()


# In[22]:


def menu_7():
    size = int(input("How many amenities would you like"))
    amenitieslst = []
    for i in range(size):
        temp = input("please enter an amenity")
        amenitieslst.append(temp)
    city_name = input("please enter the city name")
    lower_bound = input("please enter the minimum price")
    upper_bound = input("please enter the maximum price")
    
    get_property_amenities(conn,amenitieslst, city_name, lower_bound, upper_bound)
    choice = input('''0: exit
                      1:return to main menu
                      2: search for other amenities''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    elif choice == '2':
        menu_7()


# In[23]:


def menu_8():
    unit_type = input("please enter the unit type")
    city = input("please enter the city name")
    get_top_10_areas(conn, unit_type, city)
    choice = input('''0: exit
                      1:return to main menu
                      2: search for other top 10''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    elif choice == '2':
        menu_8()


# In[24]:


def menu_9():
    get_top_5_brokers(conn)
    choice = input('''0: exit
                      1:return to main menu
                      ''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    


# In[25]:


def menu_10():
    agent_phone = input("Please enter the agent phone number")
    agent_name = input("Enter the agent's name (optional), to continue without entering a name, press the enter key")
    get_properties_by_agent(conn, agent_phone, agent_name = '')
    choice = input('''0: exit
                      1:return to main menu
                      2: search for another agent''')
    if choice == '0':
        exit()
    elif choice == '1':
        print("Loading Main menu")
    elif choice == '2':
        menu_10()


# In[26]:


def Display_main_menu():
    print("Welcome to Property Finder Database Application")
    print('''Please select one of the following commands:
    0:  Exit Application
    1:  Register New User
    2:  Review a Listing Agent
    3:  View Ratings of a Listing Agent
    4:  View Broker Company Average Rating
    5:  View Development Projects Information
    6:  View Properties in any City of Your Choice
    7:  View Properties That Suit your Price Range and Amenities preference
    8:  View Top 10 Areas by Number of Properties and Average Price/Sqm
    9:  View Top 5 Broker Companies
    10: Show All properties listed by a Listing Agent''')
    choice = input("Enter Your Choice Here")
    if choice == '0':
        exit()
    elif choice == '1':
        menu_1()
        Display_main_menu()
    elif choice == '2':
        menu_2()
        Display_main_menu()
    elif choice == '3':
        menu_3()
        Display_main_menu()
    elif choice == '4':
        menu_4()
        Display_main_menu()
    elif choice == '5':
        menu_5()
        Display_main_menu()
    elif choice == '6':
        menu_6()
        Display_main_menu()
    elif choice == '7':
        menu_7()
        Display_main_menu()
    elif choice == '8':
        menu_8()
        Display_main_menu()
    elif choice == '9':
        menu_9()
        Display_main_menu()
    elif choice == '10':
        menu_10()
        Display_main_menu()


# In[ ]:


Display_main_menu()


# In[ ]:




