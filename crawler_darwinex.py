import pandas as pd
import numpy as np
import math
import datetime
import time
import requests
import json
from bs4 import BeautifulSoup
from selenium import webdriver 
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import os
from tqdm import tqdm
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import multiprocessing
import re
from os import listdir
from os.path import isfile, join

# get broswer
def get_browser():
    ''' 
    Initiate and return firefox browser using gecko driver/chrome driver.
    '''
    opts = webdriver.chrome.options.Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument('disable-notifications')
    opts.add_argument("window-size=1280,720")
    opts.add_argument("--disable-notifications")
    path = os.getcwd()
    opts.headless = True
    browser = webdriver.Chrome(executable_path=r'{}/chromedriver'.format(path),
                        options=opts)
    
    # opts = webdriver.firefox.options.Options()
    # path = os.getcwd()
    # opts.headless = True
    # browser = webdriver.Firefox(executable_path=r'{}/geckodriver'.format(path),
    #                     options=opts)
    return browser

# get darwin traders list
def crawl_all_usrs():
    driver = get_browser()
    driver.get('https://www.darwinex.com/all-darwins') 
    time.sleep(5)

    driver.find_element(By.CLASS_NAME, 'icon-th-list').click()
    time.sleep(2)

    usernames = []
    done=False
    ix = 0
    while done==False:
        driver.find_element(By.CLASS_NAME, 'body').send_keys(Keys.PAGE_DOWN)
        time.sleep(3)
        usr = driver.find_elements_by_class_name('darwin-name-container')
        usr = [i.text for i in usr]
        usernames.extend(usr)
        usernames = list(set(usernames))
        ix += 1
        
        if ix % 200 == 0:
            np.save('usernames.npy', usernames)
    
        if len(usernames)==5715:
            done = True


# get information for one user
def get_basics(soup):
    '''
    dictionary:
    ReturnSinceInception; AnnualizedReturn; TrackRecord; MaximumDrawdown; BestMonth; WorstMonth; VaR; InvestableDate; StartDate
    '''
    basics = soup.find('ul',class_='stats stats--border-effect')
    basics = basics.find_all('span')
    basics = [basic.get('data-inc-value') for basic in basics]
    
    basics_dict = {}
    for i,key in enumerate(['ReturnSinceInception','AnnualizedReturn','TrackRecord','MaximumDrawdown','BestMonth','WorstMonth']):
        basics_dict[key] = basics[i]
    
    # VaR
    var = soup.find_all(class_='container')[0].find_all('p')[2].text.strip().split(' ')[-1][:-1]
    basics_dict['VaR'] = var
    
    # beginning date of trading, and darwin 
    begdates = soup.find(class_='font-size-sm mt-1').text
    begdates = re.findall('\d+/\d+/\d+',begdates)
    try:
        basics_dict['InvestableDate'] = begdates[0]
    except:
        pass
    try:
        basics_dict['StartDate'] = begdates[1]
    except:
        pass
        
    return basics_dict
def get_monthly_ret(soup):
    '''
    Cols: Years*Months, Years Total
    Rows: Monthly Returns
    '''
    table = soup.find(attrs={'id':'table-return-container'})
    mons = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Total']
    yrs = table.find_all(class_='text-left font-weight-bold')
    yrs = [yr.get_text() for yr in yrs]
    rets = table.find_all('td',attrs={'class':'text-right'})
    rets = [ret.text for ret in rets]

    cols = []
    for yr in yrs:
        for mon in mons:
            cols.append(f'{yr}{mon}')  
    return_dict = {}
    for i,key in enumerate(cols):
        return_dict[key] = rets[i]
    
    return return_dict
def get_daily_ret(browser,username):
    return_dict = {}
    for fold in tqdm(range(32)): 
        ''' 
        total: 1057
        0: 1~35
        1: 34~68
        2: 67~101
        3: 100~134
        4: 133~167
        5: 166~200
        6: 199~233
        7: 232~266
        8: 265~299
        9: 298~332
        10: 331~365
        11: 364~398
        12: 397~431
        13: 430~464
        14: 463~497
        15: 496~530
        16: 529~563
        17: 562~596
        18: 595~629
        19: 628~662
        20: 661~695
        21: 694~728
        22: 727~761
        23: 760~794
        24: 793~827
        25: 826~860
        26: 859~893
        27: 892~926
        28: 925~959
        29: 958~992
        30: 991~1025
        31: 1024~1058
        '''
        browser.get(f'https://www.darwinex.com/invest/{username}') 
        time.sleep(3)

        action = webdriver.ActionChains(browser)

        element = browser.find_element(By.XPATH,"//*[local-name() = 'svg' and @class='highcharts-root']/*[local-name() = 'g' and @class='highcharts-grid highcharts-yaxis-grid']")
        browser.execute_script("window.scrollBy(0, 500);")
        action.move_to_element_with_offset(element,fold*33,10).click().perform()
        time.sleep(2)

        pace = 1
        for ix in tqdm(range(34)):
            action.move_by_offset(pace, 0).click().perform()
            time.sleep(1)
            try:
                values = browser.find_element(By.XPATH,"//*[local-name() = 'table' and @class='table table-sm']").text
                date = values.split('\n')[0]
                ret = values.split('\n')[1].split(' ')[1]
                return_dict[date] = ret
            except:
                pass
    
    np.save(f'/home/ubuntu/SocialTrading/Data/daily_returns/{username}.npy', return_dict)  
    
def extract_invest(browser, username:str):
    browser.get(f'https://www.darwinex.com/all-darwins') 
    time.sleep(2)

    # accept cookies
    browser.find_element_by_xpath("//a[@id='CybotCookiebotDialogBodyLevelButtonAccept']").click()
    time.sleep(2)
    
    # select order by investors
    browser.find_element_by_xpath("//*[local-name()='div' and @class='dropdown dropdown-order-darwins']").click()
    time.sleep(2)
    browser.find_element_by_xpath("//a[@class='dropdown-item' and text()='Investors']").click()
    time.sleep(3)
    
    capdict, invdict = {},{}
    for fold in tqdm(range(32)):
        browser.get(f'https://www.darwinex.com/all-darwins') 
        time.sleep(4)
        
        # select order by investors
        browser.find_element_by_xpath("//*[local-name()='div' and @class='dropdown dropdown-order-darwins']").click()
        time.sleep(2)
        browser.find_element_by_xpath("//a[@class='dropdown-item' and text()='Investors']").click()
        time.sleep(3)
        
        # click for one user
        done = False
        while done == False:
            try:
                usr = browser.find_element_by_xpath(f"//*[text()='{username}']")
                try:
                    usr.click() 
                    time.sleep(4)
                except:
                    browser.execute_script("arguments[0].scrollIntoView()", usr)
                    time.sleep(3)
                    usr.click() 
                    time.sleep(4)
                done = True
                break
            except:
                lastusr = browser.find_elements_by_xpath("//table[@class='table darwin-card-table']")[-1]
                browser.execute_script("arguments[0].scrollIntoView()", lastusr)
                time.sleep(4)
                

        # click: Investor
        action = webdriver.ActionChains(browser)
        investors = browser.find_element_by_xpath("//a[@id='graph-container-tab-tab-investors']")
        browser.execute_script("arguments[0].scrollIntoView()", investors)
        time.sleep(2)
        action.move_to_element(investors).click().perform()
        time.sleep(4)

        # click: ALL
        all = browser.find_element_by_xpath("//*[text()='ALL']")
        action.move_to_element(all).click().perform()
        time.sleep(4)

        # extract investors information
        action = webdriver.ActionChains(browser)
        time.sleep(2)
        element = browser.find_elements(By.XPATH,"//*[local-name() = 'g' and @class='highcharts-axis']")[1]
        time.sleep(2)
        action.move_to_element_with_offset(element,fold*36,30).click().perform()
        time.sleep(2)
        
        for ix in tqdm(range(37)):
            action.move_by_offset(1, 0).click().perform()
            time.sleep(1)
            try:
                values = browser.find_element(By.XPATH,"//*[local-name() = 'table' and @class='table']").text
                date = values.split('\n')[0]
                cap = ''.join(values.split('\n')[1].split(' ')[2:])
                inv = values.split('\n')[2].split(' ')[1]
                capdict[date] = cap
                invdict[date] = inv
            except:
                pass
            
    np.save(f'/home/ubuntu/SocialTrading/Data/capital/{username}.npy', capdict)
    np.save(f'/home/ubuntu/SocialTrading/Data/investors/{username}.npy', invdict)        
  
# def extract_invest(browser, username:str):
#     action = webdriver.ActionChains(browser)
#     browser.get(f'https://www.darwinex.com/all-darwins') 
#     time.sleep(2)

#     # accept cookies
#     browser.find_element_by_xpath("//a[@id='CybotCookiebotDialogBodyLevelButtonAccept']").click()
#     time.sleep(2)

        
#     # click for one user
#     element = browser.find_element_by_xpath(f"//*[text()='{username}']")
#     try:
#         element.click() 
#         time.sleep(4)
#     except:
#         browser.execute_script("arguments[0].scrollIntoView()", element)
#         time.sleep(2)
#         element.click() 
#         time.sleep(4)


#     # click: Investor
#     element = browser.find_element_by_xpath("//a[@id='graph-container-tab-tab-investors']")
#     browser.execute_script("arguments[0].scrollIntoView()", element)
#     time.sleep(2)
#     action.move_to_element(element).click().perform()
#     time.sleep(2)

#     # click: ALL
#     element = browser.find_element_by_xpath("//*[text()='ALL']")
#     action.move_to_element(element).click().perform()
#     time.sleep(4)

#     # extract investors information
#     element = browser.find_elements(By.XPATH,"//*[local-name() = 'g' and @class='highcharts-axis']")[1]
#     action.move_to_element_with_offset(element,1095,30).click().perform()
    
#     capdict, invdict = {},{}
#     for ix in tqdm(range(45)): #1140
#         action.move_by_offset(1, 0).click().perform()
#         time.sleep(1)
#         try:
#             values = browser.find_element(By.XPATH,"//*[local-name() = 'table' and @class='table']").text
#             date = values.split('\n')[0]
#             cap = ''.join(values.split('\n')[1].split(' ')[2:])
#             inv = values.split('\n')[2].split(' ')[1]
#             capdict[date] = cap
#             invdict[date] = inv
#         except:
#             pass
        
#         # if ix%100==0:
#         #     np.save(f'/home/ubuntu/SocialTrading/Data/capital/{username}_2.npy', capdict)
#         #     np.save(f'/home/ubuntu/SocialTrading/Data/investors/{username}_2.npy', invdict)        
  
                
#     # np.save(f'/home/ubuntu/SocialTrading/Data/capital/{username}_2.npy', capdict)
#     np.save(f'/home/ubuntu/SocialTrading/Data/investors/{username}_2.npy', invdict)        
  
  
# for one user
def process_user(username):
    print(f'{username}')
    
    browser = get_browser()
    
    # # get basics and monthly returns
    # browser.get(f'https://www.darwinex.com/invest/{username}') 
    # time.sleep(3)
    # html = browser.page_source
    # soup = BeautifulSoup(html, 'html.parser')
    # monthly_ret_dict = get_monthly_ret(soup)
    # basics_dict = get_basics(soup)
    # np.save(f'/home/ubuntu/SocialTrading/Data/darwinex/basics/{username}.npy', basics_dict)
    # np.save(f'/home/ubuntu/SocialTrading/Data/darwinex/monthly_returns/{username}.npy', monthly_ret_dict)
    # browser.close()
    
    # # get daily returns    
    # get_daily_ret(browser,username)
    
    # get investors information
    extract_invest(browser, username)
    
    browser.quit()

def process_user_multiprocessing():
    inv = [f for f in listdir('/home/ubuntu/SocialTrading/Data/investors') if isfile(join('/home/ubuntu/SocialTrading/Data/investors', f))]
    inv = [i[:-4] for i in inv]
    tocrawl = np.load(f'/home/ubuntu/SocialTrading/Data/darwinex/darwin_usrs_with_investors.npy',allow_pickle=True).item()
    tocrawl = list(tocrawl.keys())
    list_of_users = [i for i in tocrawl if i not in inv]

    pool = multiprocessing.Pool(processes=8)
    pool.map(process_user, list_of_users)
    pool.close()
    pool.join()    
    # process_user(list_of_users[0])
                   


if __name__=='__main__':
    process_user_multiprocessing()