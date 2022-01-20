import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime,timedelta
import json


#=====================================================================
#                              Fidelity
#=====================================================================
def fetch_Fidelity(month,year):

    allowed_months = [1,2,3,4,5,6,7,8,9,10,11,12] 
    allowed_years =  [2021,2022,2023]

    if month not in allowed_months:
        return 400
    elif year not in allowed_years:
        return 400
    else:
        pass

    url = "https://eresearch.fidelity.com/eresearch/conferenceCalls.jhtml?tab=splits&begindate={}/1/{}".format(month,year)
    #print(url)
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    r = requests.get(url,headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("tbody")
    rows = table.find_all("tr")
    
    local_db = []
    
    for i in rows:
        values = i.find_all("td")

        new_symbol = ''.join(values[0].text.split())

        try:
            new_symbol = new_symbol.split(":")[0]
        except:
            pass
        
       
        try:
            data = {"symbol" : new_symbol,
                    "split" : values[1].text,
                    "effective_date" : values[4].text,
                    "source" : "Fidelity"}
            local_db.append(data)
        except:
            noval = values[0].text
            """
            if "No Splits for this month" in noval:
                #print("No Splits for {}/{} - Fidelity".format(month,year))
            else:
                pass
            """
    return local_db

#=====================================================================
#                            Investing.com
#=====================================================================
def fetch_Investing():

    url = 'https://www.investing.com/stock-split-calendar/'
    #print(url)
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    r = requests.get(url,headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.find_all("tbody")[1]

    rows = table.find_all("tr")

    for i in rows:
        details = i.find_all("td")
        local_db = []
        name = ''.join(details[1].text.split())
        name = name.split("(")[1].replace(")","")
        data = {
            "effective_date" : details[0].text,
            "symbol" : name,
            "split" : details[2].text,
            "source" : "Investing.com"
        }

        local_db.append(data)

    return local_db

#=====================================================================
#                            Yahoo Finance
#=====================================================================
def fetch_Yahoo():
    start_date = datetime.today().strftime("%m/%d/%Y")
    end_date = (datetime.today()+timedelta(days=30)).strftime("%m/%d/%Y")
    dates = pd.date_range(start=start_date, end=end_date, freq="B")

    inspect_urls = []
    local_db = []

    for DATE in dates:
        url = "https://finance.yahoo.com/calendar/splits?from=2020-06-08&to=2020-06-13&day={}".format(
            DATE.strftime("%Y-%m-%d"))
        inspect_urls.append(url)

        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        r = requests.get(url,headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")
        try:
            TABLE = soup.find("table")
            rows = TABLE.find_all("tr")
            #print(url)
            for i in rows:

                cols = i.find_all("td")

                if (len(cols) != 0):
                    results = {"symbol" : (cols[0].text),
                               "split" : (cols[4].text),
                               "effective_date" : (cols[2].text),
                               "source" : "YahooFinance"}

                    local_db.append(results)
        except:
            #print("""
            #YahooFinance Exception:
            #Error on {}
            #URL: {}
            #""".format(DATE.strftime("%Y-%m-%d"),
            #          url))
            pass

    return local_db

#=====================================================================
#                            Zacks Scraper
#=====================================================================
def fetch_Zacks():
    
    api_key = json.load(open("params.json"))["ZACKS_API_KEY"]
    url = "https://www.quandl.com/api/v3/datatables/ZACKS/USP.json?api_key={}".format(api_key)
    r = requests.get(url)
    output = (json.loads(r.text))["datatable"]["data"]
    local_storage = []
    for i in output:
        data = {
            "symbol" : i[0],
            "split" : i[3],
            "effective_date" : i[2],
            "source" : "Zacks"
        }
        local_storage.append(data)

    return local_storage