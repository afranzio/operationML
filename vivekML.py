from datetime import *
import investpy
import pandas as pd
import yfinance as yf
import os
from ta import add_all_ta_features
import pandas as pd
from ta.utils import dropna


def five_years_before():
    five_years = (datetime.now() - timedelta(days=5*365)).strftime("%d/%m/%Y")
    return five_years

def get_today():
    return datetime.now().strftime("%d/%m/%Y")

def add_latest_quote(filename,suffix):
    df = pd.read_csv('stocks\\' + filename + '.csv')
    df = df[['Date','Open','High','Low','Close','Volume']]

    csvdate = df.iloc[-1]["Date"]
    df2 = yf.Ticker(filename +"." +  suffix)
    quote = df2.history(period="1d")

    quote.to_csv("stockstemp\\" + filename +"temp.csv")
    quote=pd.read_csv("stockstemp\\" + filename +"temp.csv")
    quote= quote[['Date','Open','High','Low','Close','Volume']]
    quote['Date']=pd.to_datetime(quote['Date'].astype(str), format='%Y-%m-%d')
    quote['Date'] = quote['Date'].dt.strftime('%Y-%m-%d')
    quotedate = quote["Date"][0]
    if(quotedate != csvdate):
        df = df.append(quote)
        df= add_all_ta_features(df,open="Open",high="High",low="Low",close="Close",volume="Volume",fillna=True)
        df.to_csv("stocks\\" + filename + ".csv")
        print(filename + " latest quote added")
    else:
        print(filename, " data already present")
        pass

def add_latest_quote_index(filename,yfinance):
    df = pd.read_csv('indices\\' + filename + '.csv')
    df = df[['Date','Open','High','Low','Close','Volume']]
    csvdate = df.iloc[-1]["Date"]
    df2 = yf.Ticker(yfinance)
    quote = df2.history(period="1d")

    quote.to_csv("indicestemp\\" + filename +"temp.csv")
    quote=pd.read_csv("indicestemp\\" + filename +"temp.csv")
    quote= quote[['Date','Open','High','Low','Close','Volume']]
    quote['Date']=pd.to_datetime(quote['Date'].astype(str), format='%Y-%m-%d')
    quote['Date'] = quote['Date'].dt.strftime('%Y-%m-%d')
    quotedate = quote["Date"][0]
    if(quotedate != csvdate):
        df = df.append(quote)
        df= add_all_ta_features(df,open="Open",high="High",low="Low",close="Close",volume="Volume",fillna=True)
        df.to_csv("indices\\" + filename + ".csv")
        print(filename + " latest quote added")
    else:
        print(filename, " data already present")
        pass

def generate_stock_csv(stock,file_name, stock_country):
    df= pd.DataFrame()


    if not os.path.exists("stocks"):
        os.makedirs("stocks")
    if not os.path.exists("stockstemp"):
        os.makedirs("stockstemp")
    if not os.path.exists('stocks\\' + file_name + '.csv'):
        df = investpy.get_stock_historical_data(stock=stock, country=stock_country, from_date=five_years_before(), to_date=get_today())
        df= add_all_ta_features(df,open="Open",high="High",low="Low",close="Close",volume="Volume",fillna=True)
        df.to_csv('stocks\\' + file_name + '.csv')
        print(file_name, "created successfully", stock)
    else:
        add_latest_quote(file_name,"NS")

def generate_index_csv(index,yfinanceindex, index_country):

    df= pd.DataFrame()

    if not os.path.exists("indices"):
        os.makedirs("indices")
    if not os.path.exists("indicestemp"):
        os.makedirs("indicestemp")
    if not os.path.exists('indices\\' + index + '.csv'):
        df = investpy.get_index_historical_data(index=index, country=index_country, from_date=five_years_before(), to_date=get_today())
        df= add_all_ta_features(df,open="Open",high="High",low="Low",close="Close",volume="Volume",fillna=True)
        df.to_csv('indices\\' + index + '.csv')
        print(index, "created successfully", index)
    else:

        add_latest_quote_index(index,yfinanceindex)


def generate_stock_csv_yf(stock, suffix):
    df = yf.Ticker(stock +"." +  suffix)
    hist = df.history(period="5y")
    #file_name=stock  + '.csv'

    if not os.path.exists("stocks"):
        os.makedirs("stocks")
    if not os.path.exists('stocks\\' + stock + '.csv'):
        hist= add_all_ta_features(hist,open="Open",high="High",low="Low",close="Close",volume="Volume",fillna=True)
        hist.to_csv('stocks\\' + stock + '.csv')
        print(stock, "created successfully", stock)
    else:
        add_latest_quote(stock,"NS")
        #print(stock, " data already present")

def generate_index_csv_yf(yfinance,filename):
    df = yf.Ticker(yfinance)
    hist = df.history(period="5y")
    #file_name=stock  + '.csv'

    if not os.path.exists("indices"):
        os.makedirs("indices")
    if not os.path.exists('indices\\' + filename + '.csv'):
        hist= add_all_ta_features(hist,open="Open",high="High",low="Low",close="Close",volume="Volume",fillna=True)
        hist.to_csv('indices\\' + filename + '.csv')
        print(filename, "created successfully", filename)
    else:
        add_latest_quote_index(filename,yfinance)
        #print(filename, " data already present")

def stock_operator():
    data = pd.read_csv('stockBook.csv')
    list_of_stocks = data['InvestpyId']
    counter =0
    for stock in list_of_stocks:
        scripname = data['scrip'].iloc[counter]
        #check file present or not
        #if present generate the laast quote and add if not present
        #not not present get the whole history from below
        #print(scrip)
        search_country = investpy.search_quotes(text=stock, products=['stocks'], n_results=1)
        try:
            generate_stock_csv(stock, scripname, search_country.country)
            counter+=1
        except:
            scripname = data['scrip'].iloc[counter]
            counter+=1
            generate_stock_csv_yf(scripname, "NS")

            continue
    print('all stocks historical processed')

def stock_operator_yf():
    data = pd.read_csv('stockBook.csv')
    list_of_stocks = data['scrip']
    counter =0
    for stock in list_of_stocks:
        #scripname = data['scrip'].iloc[counter]
        counter+=1
        #print(scrip)

        generate_stock_csv_yf(stock, "NS")
    print('all stocks historical processed')

def index_operator_yf():
    data = pd.read_csv('indexBook.csv')
    list_of_indices = data['yfinance']
    counter =0
    for yfinance in list_of_indices:
        filename = data['IndexId'].iloc[counter]
        counter+=1
        #print(scrip)

        generate_index_csv_yf(yfinance,filename)
    print('all indices historical processed')

def index_operator():
    data = pd.read_csv('indexBook.csv')
    list_of_indices = data['IndexName']
    counter =0
    for index in list_of_indices:
        indexname = data['IndexId'].iloc[counter]
        yfinance = data['yfinance'].iloc[counter]
        #check file present or not
        #if present generate the laast quote and add if not present
        #not not present get the whole history from below
        #print(scrip)
        #search_country = investpy.search_quotes(text=index, products=['stocks'], n_results=1)
        try:
            generate_index_csv(indexname,yfinance, 'india')
            counter+=1
        except:
            print(1)
            indexname = data['yfinance'].iloc[counter]
            counter+=1
            generate_index_csv_yf(indexname,yfinance)

        continue
    print('all indices historical processed')

def stock_operator_add_calc_columns():
    data = pd.read_csv('stockBook.csv')
    list_of_stocks = data['scrip']
    counter =0
    for stock in list_of_stocks:
        df= pd.read_csv('stocks\\' + stock +'.csv')
        df= add_all_ta_features(df,open="Open",high="High",low="Low",close="Close",volume="Volume",fillna=True)
        dfcopy = df[['Date','Open','High','Low','Close']]
        df_ha = dfcopy.copy()
        for i in range(df_ha.shape[0]):
            if i > 0:
                df_ha.loc[df_ha.index[i],'HAOpen'] = (df['Open'][i-1] + df['Close'][i-1])/2

                df_ha.loc[df_ha.index[i],'HAClose'] = (df['Open'][i] + df['Close'][i] + df['Low'][i] +  df['High'][i])/4
                df_ha.loc[df_ha.index[i],'HAHigh'] =df['High'][i]
                df_ha.loc[df_ha.index[i],'HALow'] =df['Low'][i]
        df_ha = df_ha.iloc[1:,:][['Date','HAOpen','HAHigh','HALow','HAClose']]
        df_ha= pd.concat([df,df_ha],axis=1)
        df_ha = df_ha.loc[:,~df_ha.columns.duplicated()].copy()
        df_ha.to_csv("stocks\\" + stock + ".csv")
        print(stock + "HA created")

def stock_operator_add_calc_columns_last_row():
    data = pd.read_csv('stockBook.csv')
    list_of_stocks = data['scrip']
    counter =0
    for stock in list_of_stocks:
        df= pd.read_csv('stocksha\\' + stock +'.csv')

        df.loc[df.index[-1],'HAOpen'] = (df.iloc[-2]['Open'] + df.iloc[-2]['Close'])/2

        df.loc[df.index[-1],'HAClose'] = (df.iloc[-1]['Open'] + df.iloc[-1]['Close'] + df.iloc[-1]['Low'] +  df.iloc[-1]['High'])/4
        df.loc[df.index[-1],'HAHigh'] =df.iloc[-1]['High']
        df.loc[df.index[-1],'HALow'] =df.iloc[-1]['Low']

        df.to_csv("stocksha\\" + stock + ".csv")
        print(stock + "HA created")


def apply_ta():
    folders = ['stocks', 'indices']
    for folder in folders:
        files = os.listdir(folder)
        for file in files:
            try:
                df = pd.read_csv(folder + '/' + file, sep=',')
                df = dropna(df)
                df = add_all_ta_features(df, open="Open", high="High", low="Low", close="Close", volume="Volume", fillna=True)
                df.to_csv(folder + '/' + file)
            except:
                print(folder, "ERROR", file)
