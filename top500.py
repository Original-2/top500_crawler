import pandas as pd # used for making csv files
from selenium import webdriver # used for getting the web driver (the chrome window)
from selenium.webdriver.support.ui import Select # we will be using a dropdown
from selenium.webdriver.chrome.options import Options # dropdown
from queue import Queue # multi threading
from threading import Thread # multi threading

chrome_options = Options()
#chrome_options.add_argument("--headless") # arguments to improve performance
chrome_options.add_argument("--disable-composited-antialiasing")
chrome_options.add_argument("--disable-font-subpixel-positioning")

prefs = {"profile.managed_default_content_settings.images": 2} # stop images rendering - this reduces network usage
chrome_options.add_experimental_option("prefs", prefs)

PATH1 = "/home/waffleman69/Documents/code/chromedriver" # this is the driver location
driver = webdriver.Chrome(PATH1, options=chrome_options) # use the driver

driver.get("https://www.top500.org/statistics/sublist/") # open the top500 website


lists = driver.find_elements_by_xpath("//select[@name='t500list']/option") # get all of the lists (from 1993)

to_be_selected = []

for i in lists:
    to_be_selected.append(i.get_attribute('innerHTML')) # get the date / name of the list (eg. November 2013)
links = {}
for i in to_be_selected:
    links[i] = {}
    select = Select(driver.find_element_by_name('t500list')) # get the dropdown
    select.select_by_visible_text(i) # select the  dropdown option
    python_button = driver.find_elements_by_xpath("//button[@class='btn btn-primary']")[0]
    python_button.click()


    print('now')
    current_list = driver.find_elements_by_xpath("//table/tbody/tr/td/a") # get links to all of the computers
    rank = 1

    for j in current_list:
        if "system" in j.get_attribute('href'): # there are 2 links in each table division, we want the link to the system
            links[i][rank] = j.get_attribute('href') # the link is at the date and rank in the dict
            rank+=1
driver.quit() # close initial driver - it will not be needed

threads = 20 # the amount of threads to be used (these are not physical threads, I have an 8 thread cpu). This allows many web pages to load at once

drivers = [webdriver.Chrome(PATH1, options=chrome_options) for i in range(threads)] # create 1 driver per thread
workers = []
done = [False for i in range(threads)] # to check if it is done

def info(start, interval, driver_used): # parses the html
    for i in links:
        print(i) # print the current list to see how far along we are
        for j in range(start,500,interval): # goes over the top 500 computers per list
            driver_used.get(links[i][j]) # submit an HTML rerquest
            links[i][j] = {} # get all of the info on a particular system

            name = driver_used.find_element_by_xpath("//div[@class='col-sm-12 col-md-9 col-lg-9']/h1")
            links[i][j]["Name"] = name.get_attribute('innerHTML')

            rows = driver_used.find_elements_by_xpath("//table[@class='table table-condensed']/tbody/tr")
            for row in rows:
                if len(row.find_elements_by_xpath('.//*')) == 2: # parse through the table
                    stuff = row.find_elements_by_xpath('.//*')
                    category = stuff[0].get_attribute('innerHTML').strip()
                    value = stuff[1].get_attribute('innerHTML').strip()
                    links[i][j][category] = value # add info to a dictionary
    driver_used.quit() # after all of the info is collected we do not need the browser window open
    done[start-1] = True # update the done list so the next part can run


queue = Queue()

class DownloadWorker(Thread): # create a class to create a worker
    def __init__(self, queue, starting, interval, driver_used):
        Thread.__init__(self)
        self.queue = queue # create variables for the info function
        self.starting = starting
        self.interval = interval
        self.driver_used = driver_used

    def run(self):
        info(self.starting, self.interval, self.driver_used) # call the parsing function

for x in range(threads):
    worker = DownloadWorker(queue, x+1, threads, drivers[x]) # initiate a worker
    worker.start() # run the worker

uniques = []
while True:
    if done == [True for i in range(threads)]:
        for i in links: # this loop will get all of the unique categories
            for j in links[i]:
                for k in links[i][j]:
                    if k not in uniques and len(k) !=1: # only if new
                        uniques.append(k)

        for i in links:
            yearlist = {} # everything from the current list
            for j in uniques:
                temp = []
                for k in links[i]:
                    try:
                        temp.append(links[i][k][j].split("\n")[0]) # add the current category and computer to the list
                    except:
                        temp.append(False) # if it doesnt exist then add 'False'
                yearlist[j] = temp # add this category to the dict
            df = pd.DataFrame(yearlist) # turn the dict to a pandas dataframe
            print(df.head()) # head - this is a sort of sanity check
            df.to_csv(f"~/Documents/code/top500/{i}.csv") # export to a CSV file
        break # end the infinite loop and subsequently the program