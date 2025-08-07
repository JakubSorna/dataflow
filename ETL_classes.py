import requests as re
import datetime as dt
import pyodbc
import os




class Extractor():
      def __init__(self):
        self.__city_name = "Prague"
        self.__limit = "2"
        self.__api_key1 = "a8927a7faf74c167f642816a31c8f5e6"
        self.__geoco = re.get(f"http://api.openweathermap.org/geo/1.0/direct?q={self.__city_name}&limit={self.__limit}&appid={self.__api_key1}").json()
        self.__lat = self.__geoco[0]["lat"]
        self.__lon = self.__geoco[0]["lon"]
        self.__api_key2 = "ecbcc925abf515166d0451d450af51de"
        self.__data = re.get(f"https://api.openweathermap.org/data/2.5/weather?lat={self.__lat}&lon={self.__lon}&appid={self.__api_key2}").json()



      def get_data(self):
        return self.__data


class Transformer():
      def __init__(self, raw_data):
          
        self.__weather = raw_data
        self.__day = self.__weather.pop('sys')
        self.__cleaner()

      def __cleaner(self):

        for key in ["type", "id", "country"]:
          self.__day.pop(key)

        temp = self.__weather.pop("main")
        self.__weather.update(temp)

        for key in ["sunrise", "sunset"]:
          self.__day[key] = dt.datetime.fromtimestamp(self.__day[key])


        self.__weather["dt"] = dt.datetime.fromtimestamp(self.__weather["dt"])
        self.__weather["date"] = self.__weather["dt"].date()
        self.__weather["time"] = self.__weather["dt"].time()
        self.__weather.pop("dt")
        self.__weather["weather"] = self.__weather["weather"][0]["main"]

        self.__day['date'] = self.__day['sunrise'].date()

        for key in ["base", "coord", "cod", "id", "name", "visibility", "sea_level", "grnd_level"]:
          self.__weather.pop(key)

        for key in ["temp", "feels_like", "temp_max", "temp_min"]:
          self.__weather[key] = int(self.__weather[key] - 273.15)

        self.__weather["clouds"] = self.__weather["clouds"]["all"]
        self.__weather["wind_speed"] = self.__weather["wind"]["speed"]
        self.__weather["wind_deg"] = self.__weather["wind"]["deg"]
        self.__weather.pop("wind")
        self.__weather.pop("timezone")
        
        if "rain" in list(self.__weather.keys()):
          self.__weather["rain_mm"]=self.__weather.pop("rain")["1h"]

        if "snow" in list(self.__weather.keys()):
          self.__weather["snow_mm"]=self.__weather.pop("snow")["1h"]
          
      def get_weather(self):
        return self.__weather

      def get_day(self):
        return self.__day


class Uploader():
    def __init__(self, weather, day):
        self.__weather = weather
        self.__day = day
        self.__conn = pyodbc.connect(os.environ["SQL_CONN"])
        self.__cursor = self.__conn.cursor()

    def upload_data(self):

        self.__cursor.execute("SELECT 1 FROM Day_ WHERE date_ = ?", self.__day["date"])
        if not self.__cursor.fetchone():
            columns = ["date_", "sunrise", "sunset"]
            values = (self.__day["date"], self.__day["sunrise"], self.__day["sunset"])
            query = f"INSERT INTO Day_ ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in columns])});"
            
            self.__cursor.execute(query, values)
            self.__conn.commit()
    
        
        columns = ['weather', 'clouds', 'temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', 'humidity', 'date_', 'time_', 'wind_speed', 'wind_deg']
        
        if "rain_mm" in list(self.__weather.keys()):
            columns.append("rain_mm")
            
        if "snow_mm" in list(self.__weather.keys()):
            columns.append("snow_mm")
        
        values = tuple(self.__weather[col.strip("_")] for col in columns)
        
        query = f"INSERT INTO Weather ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in columns])});"
        
        self.__cursor.execute(query, values)
        self.__conn.commit()

        self.__cursor.close()
        self.__conn.close()
            
            


    
