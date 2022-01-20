import requests
import pandas as pd
import json
from time import mktime, sleep
import telegram
import feedparser
from datetime import datetime,timedelta
import csv
import stock_split_parsers as SPP

#=============================================================================
# Debug Paramaters
#=============================================================================

test_mode = False
config = json.load(open("params.json"))
Reporting_Bot = config["TELEGRAM_BOT_USERNAME"]

headers = {"User-Agent" : config["USER_AGENT"],
           "Accept-Encoding" : "gzip, deflate",
           "Host" : "www.sec.gov"}


#=============================================================================
# Telegram API Function
#=============================================================================

def bot_send(msg, bot_id, mode):
    """
    Send a message to a telegram user or group specified on chatId
    chat_id must be a number!

    bot_id == bot_username FYI!!!
    """

    if mode == False:
        bot = telegram.Bot(token=config["TELEGRAM_BOT_API_TOKEN"])
        
        sent = False
        while sent == False:
            try:
                bot.sendMessage(chat_id=config['TELEGRAM_GROUP_CHAT_ID'], text=msg)
                sent = True
                print(msg)
                
            except:
                print("Console: Rate Limit (sleep 30)")
                sleep(30)
            
    else:
        print(msg)

    return None

#=============================================================================
# This will read the python file that contains a dictionary of symbols
#=============================================================================

bot_send("Console: Parsing markets.",Reporting_Bot, test_mode)

markets = pd.read_csv("Market_Hashes.csv")["market_id"].to_list()
all_markets = []
for i in markets:
    if "STOCKB_" in i:
        pass
    elif "STOCK_" in i:
        my_stock = i.split("STOCK_")[1]
        all_markets.append(my_stock)
    else:
        pass

if test_mode == True:
    bot_send("Console: Test mode is enabled.",Reporting_Bot, test_mode)

bot_send("Console: {} markets identified".format(len(all_markets)),Reporting_Bot, test_mode)

#=============================================================================
# Logging Script
#=============================================================================
logs = pd.read_csv("logs.csv")["Filing Link"].to_list()

def log(payload):
    timestamp = datetime.now().strftime("%d/%m/%Y--%H:%M:%S")
    object_ = {"timestamp" : timestamp} 
    object_.update(payload)
    
    fields = [
        object_["timestamp"],
        object_["Ticker"],
        object_["CIK"],
        object_["Link"],
        object_["Filing"],
        object_["Filing Link"],
        object_["Time"],
            ]
    
    f = open(r'logs.csv', 'a', newline='')
    writer = csv.writer(f)
    writer.writerow(fields)
    f.close()
    return None
#=============================================================================
# EDGAR Script
#=============================================================================

EDGAR_symbol_db = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers).json().items()

if len(EDGAR_symbol_db) > 1000:
    bot_send("Console: EDGAR Connection Succesful",Reporting_Bot, test_mode)

else:
    bot_send("Console: EDGAR Connection Failure",Reporting_Bot, test_mode)


markets = []
for i in EDGAR_symbol_db:
        data= i[1]
        if data["ticker"] in all_markets:
            markets.append(data)

bot_send("Console: {} markets on Morpher, {} matched with EDGAR".format(len(all_markets),len(markets)),
        Reporting_Bot, 
        test_mode)

supported_forms = ['Form 425', 'Form DEFM14A', 'Form S-4']

counter = 0
filing_db = []
for data in markets:
        counter += 1
        divisible = (counter/50).is_integer()
        
        if divisible == True:
            bot_send("Console: {}% loaded from EDGAR.".format(100*round(counter/len(markets),3)),
                     Reporting_Bot, 
                     test_mode)
            
        elif counter == len(markets):
            bot_send("Console: 100% loaded from EDGAR.",
                     Reporting_Bot, 
                     test_mode)
        else:
            pass
        
        
        feed_url = "https://sec.report/CIK/0000{}.rss".format(str(data["cik_str"]))
        NewsFeed = feedparser.parse(feed_url).entries

        for i in NewsFeed:
            """
            
            This is where you can customize what data you need out of the filing payload.
            
            """
            filing_type = i["summary"].split(" filed by")[0]
            
            payload = {
                "Ticker" : data["ticker"],
                "CIK" : data["cik_str"],
                "Link" : feed_url,
                "Filing" : filing_type,
                "Filing Link" : i["link"],
                "Time" : (datetime.fromtimestamp(mktime(i["published_parsed"])).strftime("%m/%d/%Y, %H:%M:%S"))
                }
            if filing_type in supported_forms:
                filing_db.append(payload)

        sleep(0.4)

for filing in filing_db:
        if filing["Filing Link"] not in logs:
            message = '''
            {} - CIK: {}
            *{}*
            {}
            Link: {} (sc.:{})
            '''.format(filing["Ticker"], filing["CIK"],
                       filing["Filing"],
                       filing["Time"],
                       filing["Filing Link"],
                       filing["Link"])
            bot_send(message,Reporting_Bot,test_mode)
            
            if test_mode != True:
                log(filing)
                
#=============================================================================
# Stock Splits Script
#=============================================================================
                
bot_send("Console: Now investigating relevant stock splits.",Reporting_Bot,test_mode)

date_month = datetime.now().month
date_year = datetime.now().year

splits = []
if date_month == 12:
    date_month_next = 1
    date_year_next = date_year+1
else:
    date_month_next = date_month+1
    date_year_next = date_year

#-----------------------------------------------------------------------------
#Fetching from Data Sources
#-----------------------------------------------------------------------------

bot_send("Console: Fetching from Fidelity.",Reporting_Bot,test_mode)
splits.extend(SPP.fetch_Fidelity(date_month, date_year))
splits.extend(SPP.fetch_Fidelity(date_month_next, date_year_next))

bot_send("Console: Fetching from Investing(.)com.",Reporting_Bot,test_mode)
splits.extend(SPP.fetch_Investing())

bot_send("Console: Fetching from YahooFinance.",Reporting_Bot,test_mode)
splits.extend(SPP.fetch_Yahoo())

bot_send("Console: Fetching from Zacks.",Reporting_Bot,test_mode)
splits.extend(SPP.fetch_Zacks())

delivery_db = []

for i in splits:
    yesterday = (datetime.today()+timedelta(days=-2))
    
    #Parse dates from various sources
    if i["source"] == "Fidelity":
        real_date = datetime.strptime(i["effective_date"], "%m/%d/%Y")
    
    elif i["source"] == "YahooFinance":
        real_date = datetime.strptime(i["effective_date"], "%b %d, %Y")
    
    elif i["source"] == "Investing.com":
        real_date = datetime.now()
    
    elif i["source"] == "Zacks":
        if i["effective_date"] is not None:
            real_date = datetime.strptime(i["effective_date"], "%Y-%m-%d")
        else:
            real_date = datetime.now()

    #Check recency and market inclusion    
    if (real_date > yesterday) and (i["symbol"] in all_markets):
        delivery_db.append(i)
    else:
        pass

reported_splits = []
troublesome_sources = ["Investing.com", "Zacks"]

#-----------------------------------------------------------------------------
#Reporting findings
#-----------------------------------------------------------------------------

if len(delivery_db) > 0:
    bot_send("Reported stock splits:",Reporting_Bot,test_mode)
    data_delay_flag = False
    
    for split in delivery_db:
        
        if split["symbol"] not in reported_splits:
            
            reported_splits.append(split["symbol"])
            payload_message = '''Source: {} \nSymbol: {} \nEffective Date: {} \nExpected Ratio: {}
                    '''.format(split["source"], split["symbol"], split["effective_date"], split["split"])
                    
            bot_send(payload_message,Reporting_Bot,test_mode)
            
            if split["source"] in troublesome_sources:
                data_delay_flag = True
    
        if data_delay_flag == True:
            bot_send("Console: At least one of these alerts is from Investing.com or Zacks. Please check details for accuracy.",Reporting_Bot,test_mode)

else:
    bot_send("No results found for upcoming stock splits.",Reporting_Bot,test_mode)


bot_send("Console: Done.",Reporting_Bot,test_mode)
