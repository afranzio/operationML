from datetime import *
import investpy
import pandas as pd

def five_years_before():
    five_years = (datetime.now() - timedelta(days=5*365)).strftime("%d/%m/%Y")
    return five_years

def get_today():
    return datetime.now().strftime("%d/%m/%Y")

def generate_stock_csv(stock, stock_country):
    df = investpy.get_stock_historical_data(stock=stock, country=stock_country, from_date=five_years_before(), to_date=get_today())
    file_name = stock + "_" + five_years_before().replace('/', '') + "_" + get_today().replace('/', '') + '.csv'
    print(file_name, "created successfully", stock)
    df.to_csv(file_name + '.csv')
    
def generate_index_csv(index, index_country):
    df = investpy.get_index_historical_data(index=index, country=index_country, from_date=five_years_before(), to_date=get_today())
    file_name = index + "_" + five_years_before().replace('/', '') + "_" + get_today().replace('/', '') + '.csv'
    print(file_name, "created successfully", index)
    df.to_csv(file_name + '.csv')

def stock_operator():
    data = pd.read_csv('stockBook.csv')
    list_of_stocks = data['StockId']

    for stock in list_of_stocks:
        search_country = investpy.search_quotes(text=stock, products=['stocks'], n_results=1)
        if stock=='RELI':
            generate_stock_csv(stock, 'india')
        else:
            generate_stock_csv(stock, search_country.country)

def index_operator():
    data = pd.read_csv('indexBook.csv')
    list_of_indexs = data['IndexName']

    for index in list_of_indexs:
        search_country = investpy.search_quotes(text=index, products=['indices'], n_results=1)
        generate_index_csv(index, search_country.country)
    