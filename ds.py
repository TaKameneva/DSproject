from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import matplotlib.pyplot as plt
import sqlite3
import re
import geopandas as gpd
import folium
import webbrowser
import plotly.express as px
import plotly.graph_objects as go

#Так, мы имеем проект по анализу данных мировых путешествий. В самом начале, для сбора данных, надо спарсить их с какого-либо сайта. Самым удобным в этом плане
# (больше всего собранной структурированной информации в одном месте) оказался Tripadvisor, но он, к сожалению, через BeautifulSoup не позволяет спарсить, поэтому надо делать через Selenium
###FROM: (https://habr.com/ru/articles/656609/) + (https://www.google.com/search?q=%D0%BF%D0%B0%D1%80%D1%81%D0%B8%D0%BD%D0%B3+selenium+python&oq=%D0%BF%D0%B0%D1%80%D1%81%D0%B8%D0%BD%D0%B3+sele&aqs=chrome.0.0i20i263i512j69i57j0i512j0i22i30l3j0i15i22i30j0i22i30l3.5592j0j7&sourceid=chrome&ie=UTF-8#fpstate=ive&vld=cid:a6b9240a,vid:5xpTuPmOnwk) (1:20:00 - )
options = Options()
options.add_argument('--headless') 
driver = webdriver.Chrome(options=options)
driver.get('https://www.selenium.dev/')


url =  "https://www.tripadvisor.com/TravelersChoice-Beaches" 
url2 = "https://www.tripadvisor.com/TravelersChoice-Hotels"
url3 = "https://www.tripadvisor.com/TravelersChoice-Destinations"
  


data = pd.DataFrame()


driver.get(url)
time.sleep(2) #Ставим две секунды, чтобы сайт прогрузился

    
beaches = driver.find_elements(By.CLASS_NAME, "NYniz") #Находим элементы в первом сайте по имени класса 
beaches_text = [beach.text for beach in beaches]
###END FROM
for i, beach in enumerate(beaches_text): 
    rank = i + 1
    name = beach.split("\n")[1].strip() #Так как на сайте класс "NYniz" соответствует не только названию пляжа, но и всей секции с текстом, отзывами, местоположением и тд, разобьем на части
    country = beach.split("\n")[2].strip()
    ###FROM: https://habr.com/ru/articles/349860/
    reviews = int(re.sub("[^\d]", "", beach.split("\n")[3:4][0].strip())) 
    #делим количество reviews до начала слова (чтобы были только цифры)
    ###END FROM
    new_row = pd.DataFrame({"Rank": [rank], "Beach Name": [name], "City and Country of the Beach": [country], "Reviews Beaches": [reviews]})
    data = pd.concat([data, new_row], ignore_index=True)

data[['City Beach', 'Country Beach']] = data['City and Country of the Beach'].str.rsplit(', ', n=1, expand=True) #в самом цикле трудно разделить город и страну, поэтому разделим по колонкам сейчас

index = data.columns.get_loc('City and Country of the Beach')
data.drop('City and Country of the Beach', axis=1, inplace=True)
data.insert(index, 'City Beach', data.pop('City Beach'))
data.insert(index+1, 'Country Beach', data.pop('Country Beach'))

data.to_csv("output.csv", index=False)
#Проделываем похожее с второй и третьей ссылкой, просто по другому делим на столбцы
driver.get(url2)
time.sleep(2)
hotels = driver.find_elements(By.CLASS_NAME, "NYniz")
hotels_text = [hotel.text for hotel in hotels]
data1 = pd.DataFrame()
for i, hotel in enumerate(hotels_text):
    rank = i + 1
    name = hotel.split("\n")[1].strip()
    country = hotel.split("\n")[2].strip()
    reviews = int(re.sub("[^\d]", "", beach.split("\n")[3:4][0].strip()))
    new_row1 = pd.DataFrame({"Rank": [rank], "Hotel Name": [name],"City and Country of the Hotel": [country], "Amount of Reviews": [reviews]})
    data1 = pd.concat([data1, new_row1], ignore_index=True)

data1[['City Hotel', 'Country Hotel']] = data1['City and Country of the Hotel'].str.rsplit(', ', n=1, expand=True)

index = data1.columns.get_loc('City and Country of the Hotel')
data1.drop('City and Country of the Hotel', axis=1, inplace=True)
data1.insert(index, 'City Hotel', data1.pop('City Hotel'))
data1.insert(index+1, 'Country Hotel', data1.pop('Country Hotel'))
data1.to_csv("output1.csv", index=False)

driver.get(url3)
time.sleep(2)
destinations = driver.find_elements(By.CLASS_NAME, "NYniz")
destinations_text = [destination.text for destination in destinations]
data2 = pd.DataFrame()
for i, destinations in enumerate(destinations_text):
    rank = i + 1
    name = destinations.split("\n")[1].strip()
    new_row2 = pd.DataFrame({"Rank": [rank], "Destination Name": [name]})
    data2 = pd.concat([data2, new_row2], ignore_index=True)
data2.to_csv("output2.csv", index=False)
#Теперь соединим все дата фреймы в один
merged_df = pd.merge(data, data1, on="Rank")
merged_df = pd.merge(merged_df, data2, on="Rank")

merged_df = merged_df[['Rank', 'Beach Name', 'City Beach', 'Country Beach', 'Reviews Beaches',
                       'Hotel Name', 'City Hotel', 'Country Hotel',  'Amount of Reviews',
                       'Destination Name']]

#Теперь скачаем данные по средней цене "походов" в разных странах и, с помощью SQL, соедним с имеющимся датафреймом с популярными пляжами
#чтобы в новом дата фрейме сохранился и rank и пляж и стоимость в стране 
data_travel = merged_df
data_travel.to_csv("Data on travelling.csv", index = False)
prices_average = pd.read_csv('travel_price.csv', names=['Country', 'Price'], delimiter=';')
prices_average['Country'] = prices_average['Country'].str.strip()
prices_average['Price'] = prices_average['Price'].str.strip()

conn = sqlite3.connect(':memory:')

#Загружаем данные о топе мест в таблицу "top"
top_df = pd.read_csv('output.csv')
top_df['Country Beach'] = top_df['Country Beach'].str.strip()
top_df.to_sql('output', conn, index=False)

prices_average.to_sql('travel_price', conn, index=False)


#Выполняем SQL-запрос для объединения данных
###FROM: https://proglib.io/p/kak-podruzhit-python-i-bazy-dannyh-sql-podrobnoe-rukovodstvo-2020-02-27
query = '''
SELECT output.Rank, output.[Beach Name], output.[Country Beach], travel_price.Price
FROM output
JOIN travel_price ON output.[Country Beach] = travel_price.Country
'''

price_beach = pd.read_sql_query(query, conn)
###END FROM

price_beach.to_csv('priceandbeach.csv')

#Закрываем соединение с базой данных
conn.close()


prices_hotels = pd.read_csv('prices_hotel.csv', names=['Hotel', 'Price'], delimiter=';;', engine='python')
prices_hotels['Hotel'] = prices_hotels['Hotel'].str.strip()

conn = sqlite3.connect(':memory:')

#Теперь объединяем топовые отели с самыми низкими ценами за ночь в отеле
hotels_new = pd.read_csv('output1.csv')
hotels_new['Hotel Name'] = hotels_new['Hotel Name'].str.strip()
hotels_new.to_sql('output1', conn, index=False)

prices_hotels.to_sql('prices_hotel', conn, index=False)


#Выполняем SQL-запрос для объединения данных
query = '''
SELECT output1.Rank, output1.[Hotel Name], output1.[Country Hotel], prices_hotel.Price
FROM output1
JOIN prices_hotel ON output1.[Hotel Name] = prices_hotel.Hotel
'''

prices_hotels = pd.read_sql_query(query, conn)

prices_hotels.to_csv('priceandhotel.csv')

conn.close()

#Теперь аналогичное с погодой и топом мест                  
weather_places = pd.read_csv('weather_places.csv', names=['Place', 'July weather high', 'July weather low', 'January weather high', 'January weather low', 'July rainy days', 'January rainy days'], delimiter=';')
weather_places['Place'] = weather_places['Place'].str.strip()

conn = sqlite3.connect(':memory:')

places_new = pd.read_csv('output2.csv')
places_new['Destination Name'] = places_new['Destination Name'].str.strip()
places_new.to_sql('output2', conn, index=False)

weather_places.to_sql('weather_places', conn, index=False)

query = '''
    SELECT output2.Rank, output2.[Destination Name], weather_places.[July weather high], weather_places.[July weather low],
           weather_places.[January weather high], weather_places.[January weather low],
           weather_places.[July rainy days], weather_places.[January rainy days]
    FROM output2
    JOIN weather_places ON output2.[Destination Name] = weather_places.Place
'''

weather_places = pd.read_sql_query(query, conn)

weather_places.to_csv('weatherandplaces.csv')

conn.close()
#Соединим дата фрейм с пляжами с дата фреймом с температурой воды на этих пляжах по названию пляжа
watertemp = pd.read_csv('watertemp.csv', names = ['Beach Name', 'Temperature'], delimiter=';') 
merged_df = pd.merge(watertemp, data, on='Beach Name')

watertempbeach = merged_df[['Rank', 'Beach Name', 'Temperature']]
watertempbeach.to_csv('watertempbeach.csv')
#Теперь перейдем к визуализации имеющихся данных. В начале построим график с зависимостью популярности пляжа, от количества отзывов:
ranking = data["Rank"]
reviews = data["Reviews Beaches"]

reviews = reviews.replace(",", "").astype(int)
reviews = pd.to_numeric(reviews, errors="coerce")
ranking = pd.to_numeric(ranking, errors="coerce")

plt.figure(figsize=(10, 6))
plt.bar(ranking, reviews)
plt.xlabel("Ranking")
plt.ylabel("Number of Reviews")
plt.title("Number of Reviews vs Ranking")
plt.xticks(ranking)
plt.show()
#График с зависимостью цены за ночь в отеле и его рангом
ranking = prices_hotels["Rank"]
price = prices_hotels["Price"]

price = price.astype(int)
price = pd.to_numeric(price, errors="coerce")
ranking = pd.to_numeric(ranking, errors="coerce")

plt.figure(figsize=(10, 6))
plt.bar(ranking, price)
plt.xlabel("Ranking")
plt.ylabel("Price for the cheapest room for one night")
plt.title("Price vs Ranking")
plt.xticks(ranking)
plt.show()  
#Следующим построим график зависимости популярности пляжа от цены "походов" в этой стране
ranking = price_beach["Rank"]
price = price_beach["Price"]

price = price.str.replace("$", "",regex = False).astype(int)
price = pd.to_numeric(price, errors="coerce")
ranking = pd.to_numeric(ranking, errors="coerce")

plt.figure(figsize=(10, 6))
plt.bar(ranking, price)
plt.xlabel("Ranking")
plt.ylabel("Price for Backpacking in this Country")
plt.title("Price vs Ranking")
plt.xticks(ranking)
plt.show()
#Тут сделаем интерактивный график, где каждая точка на графике представляет пляж с определенным рейтингом и соответствующей ему температурой воды. 
ranking = watertempbeach["Rank"]
temp = watertempbeach["Temperature"]

pltx = go.Figure()

pltx.add_trace(go.Scatter(x=watertempbeach["Rank"], y=watertempbeach["Temperature"], mode='markers', marker=dict(size=12),
                         hovertext=watertempbeach["Temperature"], hovertemplate="Temperature: %{hovertext}°C",
                         line=dict(color='royalblue', width=1)))

pltx.update_layout(title="Water Temperature vs Beach Ranking",
                  xaxis_title="Beach Ranking",
                  yaxis_title="Highest Water Temperature (°C)",
                  xaxis=dict(type='category', dtick=1),
                  yaxis=dict(range=[min(watertempbeach["Temperature"]) - 1, max(watertempbeach["Temperature"]) + 1]),
                  showlegend=False)

pltx.show()

wp = pd.read_csv('weatherandplaces.csv')  

#График средней температуры в июле и январе, зависимость "ранга", от температур
fig = px.scatter(wp, x="July weather high", y="January weather high", 
                 size="Rank", color="Rank", hover_name="Destination Name",
                 labels={"July weather high": "Июль (максимальная)", "January weather high": "Январь (максимальная)"},
                 title="Средняя температура в июле и январе")
fig.show()

#График количества дождливых дней в июле и январе, зависимость "ранга", от дождливых дней
fig = px.scatter(wp, x="July rainy days", y="January rainy days", 
                 size="Rank", color="Rank", hover_name="Destination Name",
                 labels={"July rainy days": "Дождливые дни в июле", "January rainy days": "Дождливые дни в январе"},
                 title="Количество дождливых дней в июле и январе")
fig.show()
      
top = pd.read_csv('topcountries.csv', names = ['City', 'Country'], delimiter= ';')
popular_countries = top['Country'].unique() 

#Теперь сделаем интерактивную карту, на которой розовым будут отмечены самые посещяемые места мира, желтым - самые популярные отели, а голубым - самые популярные пляжи
###FROM: https://realpython.com/python-folium-web-maps-from-data/#create-and-style-a-map 
world_data = gpd.read_file('/Users/zhmihorka/Downloads/110m_cultural/ne_110m_admin_0_countries.shp')

m = folium.Map()

for country in popular_countries:
    country_geometry = world_data[world_data['SOVEREIGNT'] == country]['geometry']
    if not country_geometry.empty:
        folium.GeoJson(
            country_geometry,
            style_function=lambda x: {'fillColor': 'pink', 'color': 'pink'},
            highlight_function=lambda x: {'fillColor': 'pink', 'color': 'pink'}
        ).add_to(m)
popular_hotels = data1['Country Hotel'].unique()
###END FROM

for country in popular_hotels:
    country_geometry = world_data[world_data['SOVEREIGNT'] == country]['geometry']
    if not country_geometry.empty:
        folium.GeoJson(
            country_geometry,
            style_function=lambda x: {'fillColor': 'yellow', 'color': 'yellow'},
            highlight_function=lambda x: {'fillColor': 'yellow', 'color': 'yellow'}
        ).add_to(m)
popular_beaches = data['Country Beach'].unique()
for country in popular_beaches:
    country_geometry = world_data[world_data['SOVEREIGNT'] == country]['geometry']
    if not country_geometry.empty:
        folium.GeoJson(
            country_geometry,
            style_function=lambda x: {'fillColor': 'skyblue', 'color': 'skyblue'},
            highlight_function=lambda x: {'fillColor': 'skyblue', 'color': 'skyblue'}
        ).add_to(m)
###FROM: https://realpython.com/python-folium-web-maps-from-data/#create-and-style-a-map
browser_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

webbrowser.register('mybrowser', None, webbrowser.BackgroundBrowser(browser_path))

m.save("map.html")

webbrowser.get('mybrowser').open_new_tab("map.html")
###END FROM
