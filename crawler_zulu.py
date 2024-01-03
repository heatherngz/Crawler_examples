import numpy as np
import time
from bs4 import BeautifulSoup
from selenium import webdriver 
from selenium.webdriver.common.action_chains import ActionChains
import os
import time
import multiprocessing
import re
from os import listdir
from os.path import isfile, join
from tqdm import tqdm
from selenium.webdriver.common.by import By

def get_browser():
    ''' 
    Initiate and return firefox browser using gecko driver/chrome driver.
    '''
    # opts = webdriver.chrome.options.Options()
    # path = os.getcwd()
    # opts.headless = True
    # browser = webdriver.Chrome(executable_path=r'{}/chromedriver'.format(path),
    #                     options=opts)
    
    opts = webdriver.firefox.options.Options()
    path = os.getcwd()
    opts.headless = True
    browser = webdriver.Firefox(executable_path=r'{}/geckodriver'.format(path),
                        options=opts)
    return browser

def get_trading_history(browser, usercode):
    '''
    the last page cannot be loaded???
    Performs options to scrape traders' trading history
    Columns: currency, type, std_lots, date_open, date_closed, open_close, high, low, roll, profit, total 
    '''

    # Declare variables
    trading_history = []
    ix = 0
    done = False

    # the number of all trades
    # totalnumber = browser.find_element_by_xpath('//span[@class="slds-m-right--large"]').text
    # totalnumber = re.findall('of \d+ trades',totalnumber)[0]
    # totalnumber = int(totalnumber[3:-7])
    
    # This loop iterates through each row, pulls data from each column then moves onto the next page to continue if neccessary
    # done represents whether we want to stop scraping
    while done == False:
        html = browser.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        try:
            # Find the trading history table
            table = soup.find('zl-trading-history-table').find('tbody')
            
            # Create a list of all rows present on the screen
            rows = table.find_all('tr')

            # Iterate through each row of earnings reports and pull date from each column
            for row in rows:
                
                ix += 1
                # Pull results from each column
                col = row.find_all('td')[1:]
                col = [i.get_text() for i in col]
                trading_history.append(col)

            # if ix == totalnumber:
            #     done = True
            #     break
            
            # Scroll next page button into view
            browser.execute_script("window.scrollTo(0, 1600)")
            time.sleep(1)
            
            # Find next page button
            next_btn = browser.find_element_by_xpath("//button[contains(.,'Next')]")
        
            # Click on next page button
            actions = ActionChains(browser)
            actions.move_to_element(next_btn).click().perform()
            time.sleep(2)
        
        except:
            done = True
            break
    
    np.save(f'/home/ubuntu/SocialTrading/Data/zulutrade/zulu_trading_history/zulu_{usercode}.npy',trading_history)

def get_basics(browser, usercode):
    basics = {}
    rank = browser.find_element_by_xpath('//div[@class="zl-trader-profile slds-col slds-small-size--1-of-5 slds-p-right--medium slds-p-bottom--small"]').text
    rank = re.findall('#\d+',rank)[0]
    basics['rank'] = rank
    strategy = browser.find_element_by_xpath('//div[@class="zl-trader-status-strategy zl-trader-status-strategy-off"]').text
    basics['strategy'] = strategy
    
    np.save(f'/home/ubuntu/SocialTrading/Data/zulutrade/zulu_basics/zulu_{usercode}.npy',basics)

def get_daily_profit(browser, usercode):
    return_dict = {}
        
    # iterate
    # locate the chart
    for fold in tqdm(range(9)):
        browser.get(f"https://old.zulutrade.com/trader/{usercode}/trading")
        time.sleep(3)
        browser.get_screenshot_as_file('screenshot.png')
        action = webdriver.ActionChains(browser)
        browser.execute_script("window.scrollBy(0, 300);")
        time.sleep(2)
        try:
            option = browser.find_element(By.XPATH,"//*[local-name() = 'span' and text()='Pips']")
            option.click()
            time.sleep(2)
        except:
            pass
        browser.get_screenshot_as_file('screenshot.png')
        element = browser.find_element(By.XPATH,"//*[local-name() = 'svg' and @class='highcharts-root']")
        action.move_to_element_with_offset(element,fold*100+70,300).click().perform()
        time.sleep(2)

        # move 1 pace by 1 pace
        pace = 1
        for ix in tqdm(range(101)):
            action.move_by_offset(pace, 0).click().perform()
            time.sleep(2)
            try:
                values = browser.find_element(By.XPATH,"//*[local-name() = 'div' and @class='highcharts-label highcharts-tooltip']").text
                ret = values.split('\n')[0]
                date = values.split('\n')[1].split(':')[1].strip()
                return_dict[date] = ret
            except:
                pass
        
        np.save(f'/home/ubuntu/SocialTrading/Data/zulutrade/zulu_daily/zulu_{usercode}.npy', return_dict) # save daily returns   
                

def get_investors(browser, usercode): 
    browser.get(f"https://old.zulutrade.com/trader{usercode}/trading/investors") 
    time.sleep(5)


def parse_user(usercode):
    '''
    https://old.zulutrade.com/trader/code/trading
    '''
    print(usercode)
    # Start firefox browser in selenium
    browser = get_browser()
    
    # # Open zulutrade.com
    # url = f"https://old.zulutrade.com/trader/{usercode}/trading"
    # browser.get(url)
    # time.sleep(5)
    # browser.get_screenshot_as_file('screenshot.png')
    
    # # Get rank and strategy introduction
    # print('getting basics...')
    # get_basics(browser, usercode)
    
    # # Passess browser into get trading history
    # print('getting trading history...')
    # get_trading_history(browser,usercode)
    
    # Get daily return
    print('getting daily returns...')
    get_daily_profit(browser,usercode)
    
    
    # quit browser
    browser.quit()


def parse_user_multiprocessing():
    # get usercodes
    list_of_users = np.load(f'/home/ubuntu/SocialTrading/Data/zulutrade/zulu_usr_href.npy',allow_pickle=True).item()
    list_of_users = list(list_of_users.values())
    list_of_users = [i[8:-8] for i in list_of_users]
    
    basics = [f for f in listdir('/home/ubuntu/SocialTrading/Data/zulutrade/zulu_daily') if isfile(join('/home/ubuntu/SocialTrading/Data/zulutrade/zulu_daily', f))]
    basics = [i[5:-4] for i in basics]
    list_of_users = [i for i in list_of_users if i not in basics]
    
    # multiprocessing
    pool = multiprocessing.Pool()
    pool.map(parse_user, list_of_users)
    pool.close()
    pool.join()    
        

if __name__=='__main__':
    parse_user_multiprocessing()