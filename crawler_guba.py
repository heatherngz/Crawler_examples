import pandas as pd
import numpy as np
import math
import time
import requests
import json
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import os
import geckodriver_autoinstaller
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import time
from tqdm import tqdm



def get_soup(url, proxies=None):
    option=2
    if option==1:
        opts = Options()
        path = os.getcwd()
        opts.headless = True
        browser = Firefox(executable_path=r'{}/geckodriver'.format(path), options=opts)
        try:
            browser.get(url)
            html=browser.page_source
            soup=BeautifulSoup(html, 'html.parser')
        except:
            soup=' '
        browser.close()

    else:
        done = False
        soup = url
        fail_count = 0
        fail_limit = 10
        while done==False and fail_count < fail_limit:
            try:
                requests.adapters.DEFAULT_RETRIES=5
                headers = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:105.0) Gecko/20100101 Firefox/105.0" } 
                r = requests.get(url, headers=headers, proxies=proxies)

                if r.status_code == 502:
                    fail_count += 1
                    if proxies is not None:
                        # If using proxies, send a requirement to change IP address (Not implemented yet)
                        continue
                    else:
                        time.sleep(600)
                        done = False
                else:
                    r.encoding=r.apparent_encoding
                    html=r.text
                    soup=BeautifulSoup(html, 'html.parser')

                    done=True
            except Exception as e:
                print(e)
                fail_count += 1
                time.sleep(1)

        return done, soup
    return soup

def get_title(soup):
    try:
        try:
            title=soup.find('h1',class_='article-title').text #获取帖子标题
        except:
            title=soup.find('div',id=re.compile('zwTitle|post_content')).text.strip() #获取帖子标题
            title=re.findall('.*\n',title)[0][:-1].strip()
    except:
        title=' '
    return title

def get_time(soup):
    try:
        try:
            times=soup.find_all('span',class_=re.compile('(txt)|(post_time fl)')) #获取帖子时间
            if times[0].text =='来自':
                time=times[1].text
            else:
                time=times[0].text
        except:
            time=soup.find('div',class_='zwfbtime').text.strip()
    except:
        time=' '
    return time

def get_zan(soup):
    try:
        try:
            zan=soup.find('span',class_=re.compile('(zancout text-primary)|likenum|like_num')).text #获取点赞数量
        except:
            zan=soup.find('div',id='like_wrap').get('data-like_count')
    except:
        zan=' '
    return zan

def get_body_stock(soup):
    try:
        body_all=soup.find('div',class_=re.compile('xeditor_content|zwconbody|article-body|(stockcodec .xeditor)'))
        body=body_all.text.strip() #获取正文内容
        try:
            quoted_stocks=body_all.find_all('a')
            stocks=''
            for stock in quoted_stocks:
                stocks=stocks+','+stock.text
        except:
            stocks=' '
    except:
        body=' '
        stocks=' '
    return body, stocks

def get_content(url, proxies=None):
    if 'luyan' in url:
        done = True

        title='luyan'
        time=' '
        zan=' '
        body=' '
        stocks=' '

        data=[title,time,zan,body,stocks,url]
    else:
        done, soup = get_soup(url, proxies=proxies)
        if done:
            title=get_title(soup)
            time=get_time(soup)
            zan=get_zan(soup)
            body,stocks=get_body_stock(soup)
            data=[title,time,zan,body,stocks,url]
        else:
            data = soup

    return done, data

def get_author_info(urls): #获取作者的信息
    #url: 作者url
    print(f'There are {len(urls)} authors.')
    author=[]
    for i,url in enumerate(urls):
        print(f'parsing {i}th author...')
        soup=get_soup(url)
           
        name=soup.find('div',class_='others_username').text #name
        others_level=soup.find('div',class_='others_title')
        others_p=others_level.find_all('p')
        influence=others_p[0].find('span').get('class')  #influence
        age=others_p[1].text #age
        try:
            ip=others_p[2].text #ip
        except:
            ip=' '
        follow=soup.find('a',id='tafollownav').text #follow
        fans=soup.find('a',id='tafansa').text #fans
        others_info=soup.find('div',class_='others_info')
        visit=others_info.find_all('p')[0].text #visit
        try:        
            bio=others_info.find_all('p')[1].text #bio
        except: 
            bio=' '
        info=soup.find_all('li',class_='head_nav')
        post=info[0].text #posts
        stock=info[1].text #stock
        portfolio=info[2].text #portfolio
        
        data=[name,influence,age,ip,follow,fans,visit,bio,post,stock,portfolio]
        author.append(data)
        
    author_data=pd.DataFrame(author,columns=['作者姓名','影响力','吧龄','ip属地','关注','粉丝','访问人数','简介','发言数','股票数','组合数'])
    return author_data

def get_content_multiprocessing(list_of_urls, n_proc=5, save_per=1000, save_dir='.', proxies=None, initial_save_fn_index=1):
    """ Get contents in a multi-processing fashion by maintaining a process pool.
    """
    import multiprocessing as mp

    # create shared list among sub-processes
    global result_list 
    global fail_list
    global save_fn_index
    result_list = mp.Manager().list()
    fail_list = mp.Manager().list()
    save_fn_index = initial_save_fn_index
    

    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)

    def save_to_disk(save_fn_index, result_list):
        """ Save to disk and update the name for the file to save """
        fn = os.path.join(save_dir, '{}.csv'.format(save_fn_index))
        # save to file
        save_data = list(result_list)
        save_data=pd.DataFrame(save_data,columns=['帖子标题','帖子时间','点赞数','帖子正文','提及股票','帖子url'])  
        save_data.to_csv(fn,index=False)
        save_fn_index += 1
        # reallocate result list, update params
        result_list = mp.Manager().list()
        return save_fn_index, result_list
    
    with mp.Pool(processes=n_proc) as pool, tqdm(total=len(range(0, len(list_of_urls)))) as pbar:
        def callback(result):
            """ Upon finish of each job, do the following """
            global result_list
            global fail_list
            global save_fn_index
            # update the progress bar
            pbar.update(1)
            # save to disk if the number of results exceedes save_per
            done, result = result
            if done:
                result_list.append(result)
                if len(result_list) >= save_per:
                    save_fn_index, result_list = save_to_disk(save_fn_index, result_list)
            else:
                # failed, add the url to fail_list
                fail_list.append(result)

        # assign jobs to the pool
        for i in range(0, len(list_of_urls)):
            pool.apply_async(func=get_content, args=(list_of_urls[i], proxies), callback=callback)

        pool.close()
        pool.join()
    
    # save to disk before ending
    save_fn_index, result_list = save_to_disk(save_fn_index, result_list)
    # save fail list
    with open('failed.json', 'w') as f:
        f.write(json.dumps(list(fail_list), ensure_ascii=False))

""" crawl forum lists"""
def get_forums_list(url): 
    '''获取所有概念吧的信息及其url'''
    
    soup=get_soup(url)

    # forums' name list, forum‘s url list
    forum_name_list = []
    forum_url_list = []
    forums = soup.find_all('div',class_='ngblistitem')
    
    for forum in forums:
        a = forum.find_all('a')
        for i in a:
            forum_name_list.append(i.text)
            forum_url_list.append(i.get('href'))
    
    return forum_name_list, forum_url_list

def get_all_posts(number): 
    '''获取某概念吧内的所有页的帖子的基本信息
    number: 概念吧的代号'''
    
    url=f'https://guba.eastmoney.com/list,{number},f.html'
    soup=get_soup(url)
    posts=[]
    
    try:
        sumpage=soup.find('span',class_='sumpage').text
        print(f'{number} has {sumpage} pages.')

        for i in range(1,int(sumpage)+1):
            print(f'parsing the {i}th page...')
            suburl=f'https://guba.eastmoney.com/list,{number},f_{i}.html'
            one_page_data=get_one_page(suburl)
            posts.extend(one_page_data)
    
    except:
        print(f'{number} has 1 page.')
        one_page_data=get_one_page(url)
        posts.extend(one_page_data)
    
    posts_data=pd.DataFrame(posts,columns=['阅读数','评论数','标题','作者','发帖时间','作者url','帖子url'])
    return posts_data

def get_one_page(suburl): 
    '''获取某概念吧一页内的所有帖子的基本信息'''
    
    soup=get_soup(suburl)

    one_page_data=[]
    list=soup.find('div',id="articlelistnew")
    post_list=list.find_all('div')
    for post in post_list[1:-2]:
        read_counts=post.find('span',class_='l1 a1').text #获取帖子阅读数
        comment_counts=post.find('span',class_='l2 a2').text #获取帖子评论数
        title=post.find('span',class_='l3 a3') 
        headline=title.text #获取帖子标题
        post_url=title.find('a').get("href") #获取帖子url
        if post_url.endswith('.html'):
            post_url='https://guba.eastmoney.com'+post_url
        else:
            post_url='https:'+post_url
        author=post.find('span',class_='l4 a4')
        author_id=author.text #获取作者id  
        author_url=author.find('a').get("href") #获取作者url  
        author_url='https:'+author_url
        time=post.find('span',class_='l5 a5').text #获取发帖时间
        data=[read_counts,comment_counts,headline,author_id,time,author_url,post_url]
        one_page_data.append(data)
    
    return one_page_data


""" Methods for retrieving proxies. """

def get_proxy(tunnel='t255.kdltps.com:15818', username='t16593784677487', password='5m6rghbx'):
    """ Return proxies dict for requests.get (tunnel) 
    """
    proxies = {
    "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
    "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
    }
    return proxies

def get_secret_token(secret_id='', secret_key=''):
    """ Return signature for processing other posts.
    """
    api_url = 'https://auth.kdlapi.com/api/get_secret_token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'secret_id': secret_id, 'secret_key': secret_key}
    response = requests.post(api_url, headers=headers, params=data, data=data)
    assert response.status_code == 200 
    response = response.json()
    assert 'data' in response and 'secret_token' in response['data']
    return response['data']['secret_token']

def change_ip(secret_id='', secret_token=''):
    """ Only orders with cycle > 1min support this function. """
    api_url = 'https://tps.kdlapi.com/api/changetpsip?secret_id={}&signature={}'.format(secret_id, secret_token)
    response = requests.get(api_url)
    return response




if __name__ == '__main__':
    folder_path = '/home/zhqq/Jiayang/crawler'
    save_path = folder_path+'/posts'

    # urls = pd.read_csv(f'{folder_path}/urls/urls.csv')
    # added_urls = pd.read_csv(f'{folder_path}/urls/added_urls.csv')
    urls = list(set(list(urls['帖子url'])+list(added_urls['0'])))
    # urls = pd.read_csv(f'{folder_path}/urls/to_crawl.csv')
    # urls = list(urls['帖子url'])

    proxy_params = {
        'tunnel': 'k252.kdltps.com:15818',
        'username': 't16598489277946',
        'password': 'qspd7g7y',
        'secret_id': 'od95brj5mvv9hcitxneu',
        'secret_key': '5ebm9kldombbp62espl41aiu9tb29d9t',
    }
    # secret_token = get_secret_token(proxy_params['secret_id'], proxy_params['secret_key'])
    proxies = get_proxy(tunnel=proxy_params['tunnel'], username=proxy_params['username'], password=proxy_params['password'])

    # Multiprocessing crawling
    print('Total number of urls: {}'.format(len(urls)))
    get_content_multiprocessing(urls, n_proc=30, save_per=10000, save_dir='posts', proxies=proxies, initial_save_fn_index=1)
