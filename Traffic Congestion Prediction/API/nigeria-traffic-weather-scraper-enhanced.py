import requests
import time
import json
from datetime import datetime
import pandas as pd
from googlemaps import Client
from pathlib import Path
import logging
import pytz

class NigeriaTrafficScraper:
    def __init__(self, google_api_key, weather_api_key, locations):
        """
        Initialize scraper with Google Maps API key and Nigerian locations
        """
        self.gmaps = Client(key=google_api_key)
        self.weather_api_key = weather_api_key
        self.locations = locations
        self.nigeria_tz = pytz.timezone('Africa/Lagos')
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            filename='nigeria_traffic_weather_scraper.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    def get_weather_data(self, city):
        """Get weather data for a city"""
        try:
            city_query = f"{city}, Nigeria"
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city_query}&appid={self.weather_api_key}&units=metric"
            
            response = requests.get(url)
            response.raise_for_status()
            weather_data = response.json()
            
            return {
                'temperature_c': weather_data['main']['temp'],
                'feels_like_c': weather_data['main']['feels_like'],
                'humidity_percent': weather_data['main']['humidity'],
                'pressure_hpa': weather_data['main']['pressure'],
                'weather_condition': weather_data['weather'][0]['main'],
                'weather_description': weather_data['weather'][0]['description'],
                'wind_speed_ms': weather_data['wind']['speed'],
                'wind_direction_degrees': weather_data.get('wind', {}).get('deg', 0),
                'cloud_coverage_percent': weather_data.get('clouds', {}).get('all', 0),
                'visibility_meters': weather_data.get('visibility', 0),
                'rain_1h_mm': weather_data.get('rain', {}).get('1h', 0),
                'rain_3h_mm': weather_data.get('rain', {}).get('3h', 0)
            }
            
        except Exception as e:
            logging.error(f"Error getting weather data for {city}: {str(e)}")
            return None

    def get_traffic_data(self, origin, destination, city):
        """Get enhanced traffic data between two points in Nigeria"""
        try:
            origin_full = f"{origin}, {city}, Nigeria"
            destination_full = f"{destination}, {city}, Nigeria"
            
            # Get current time in Nigeria
            now = datetime.now(self.nigeria_tz)
            
            # Get traffic data
            result = self.gmaps.directions(
                origin_full,
                destination_full,
                mode="driving",
                departure_time=now,
                traffic_model="best_guess",
                alternatives=True  # Get alternative routes if available
            )
            
            if not result:
                raise Exception(f"No route found between {origin} and {destination}")
                
            route = result[0]['legs'][0]
            
            # Get weather data
            weather_data = self.get_weather_data(city)
            
            # Combine traffic and weather data
            data = {
                'city': city,
                'origin': origin,
                'destination': destination,
                'distance_km': route['distance']['text'],
                'distance_meters': route['distance']['value'],
                'duration_normal_mins': route['duration']['value'] // 60,
                'duration_in_traffic_mins': route['duration_in_traffic']['value'] // 60,
                'traffic_ratio': route['duration_in_traffic']['value'] / route['duration']['value'],
                'timestamp': now.isoformat(),
                'day_of_week': now.strftime('%A'),
                'is_weekend': now.weekday() >= 5,
                'hour_of_day': now.hour,
                'peak_hour': self.is_peak_hour(now.hour),
                'time_period': self.get_time_period(now.hour),
                'num_alternative_routes': len(result) - 1 if len(result) > 1 else 0,
                'steps_count': len(route['steps']),
                'has_tolls': any('toll' in step.get('html_instructions', '').lower() for step in route['steps']),
                'route_complexity': len(route['steps']) / route['distance']['value'] * 1000  # steps per km
            }
            
            # Add weather data if available
            if weather_data:
                data.update(weather_data)
                
            return data
            
        except Exception as e:
            logging.error(f"Error getting traffic data for {city}: {str(e)}")
            return None

    def is_peak_hour(self, hour):
        """Determine if current hour is peak traffic time"""
        morning_peak = 6 <= hour <= 10
        evening_peak = 16 <= hour <= 20
        return morning_peak or evening_peak

    def get_time_period(self, hour):
        """Categorize time of day"""
        if 5 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 16:
            return 'Afternoon'
        elif 16 <= hour < 20:
            return 'Evening'
        else:
            return 'Night'

    def save_data(self, data, city):
        """Save traffic and weather data to CSV file organized by city"""
        filepath = Path(f"traffic_weather_data_{city.lower().replace(' ', '_')}.csv")
        
        try:
            if filepath.exists():
                df = pd.read_csv(filepath)
                df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            else:
                df = pd.DataFrame([data])
            
            df.to_csv(filepath, index=False)
            logging.info(f"Data saved for {city}: {data['origin']} to {data['destination']}")
            
        except Exception as e:
            logging.error(f"Error saving data for {city}: {str(e)}")

    def run(self, interval_minutes=15):
        """Run continuous scraping with specified interval"""
        while True:
            for location in self.locations:
                data = self.get_traffic_data(
                    location['origin'],
                    location['destination'],
                    location['city']
                )
                
                if data:
                    self.save_data(data, location['city'])
                    
            time.sleep(interval_minutes * 60)

# Example usage with expanded locations
if __name__ == "__main__":
    google_api_key = "Your API KEY"
    weather_api_key = "Your API KEY"
    
    locations = [
        # Lagos routes
        {
            "name": "lagos_mainland_island",
            "origin": "Ikeja",
            "destination": "Victoria Island",
            "city": "Lagos"
        },
        {
            "name": "lagos_lekki_vi",
            "origin": "Lekki Phase 1",
            "destination": "Victoria Island",
            "city": "Lagos"
        },
        {
            "name": "lagos_surulere_vi",
            "origin": "Surulere",
            "destination": "Victoria Island",
            "city": "Lagos"
        },
        {
            "name": "lagos_ajah_vi",
            "origin": "Ajah",
            "destination": "Victoria Island",
            "city": "Lagos"
        },
        {
            "name": "lagos_ikorodu_vi",
            "origin": "Ikorodu",
            "destination": "Victoria Island",
            "city": "Lagos"
        },
        {
            "name": "lagos_oshodi_apapa",
            "origin": "Oshodi",
            "destination": "Apapa",
            "city": "Lagos"
        },
        {
            "name": "lagos_festac_vi",
            "origin": "Festac",
            "destination": "Victoria Island",
            "city": "Lagos"
        },
        
        # FCT (Abuja) routes
        {
            "name": "abuja_central",
            "origin": "Wuse",
            "destination": "Central Business District",
            "city": "FCT"
        },
        {
            "name": "abuja_airport_route",
            "origin": "Central Business District",
            "destination": "Nnamdi Azikiwe International Airport",
            "city": "FCT"
        },
        {
            "name": "abuja_kubwa_cbd",
            "origin": "Kubwa",
            "destination": "Central Business District",
            "city": "FCT"
        },
        {
            "name": "abuja_nyanya_cbd",
            "origin": "Nyanya",
            "destination": "Central Business District",
            "city": "FCT"
        },
        {
            "name": "abuja_gwagwalada_cbd",
            "origin": "Gwagwalada",
            "destination": "Central Business District",
            "city": "FCT"
        },
        {
            "name": "abuja_karu_cbd",
            "origin": "Karu",
            "destination": "Central Business District",
            "city": "FCT"
        },
        {
            "name": "abuja_lugbe_cbd",
            "origin": "Lugbe",
            "destination": "Central Business District",
            "city": "FCT"
        },
        
        # Port Harcourt routes
        {
            "name": "ph_gra_waterlines",
            "origin": "GRA Phase 2",
            "destination": "Waterlines",
            "city": "Port Harcourt"
        },
        {
            "name": "ph_choba_waterfront",
            "origin": "Choba",
            "destination": "Tourist Beach",
            "city": "Port Harcourt"
        },
        {
            "name": "ph_eleme_town",
            "origin": "Eleme Junction",
            "destination": "Port Harcourt Town",
            "city": "Port Harcourt"
        },
        {
            "name": "ph_airforce_rumuola",
            "origin": "Air Force Junction",
            "destination": "Rumuola",
            "city": "Port Harcourt"
        },
        {
            "name": "ph_rukpokwu_cbd",
            "origin": "Rukpokwu",
            "destination": "Old GRA",
            "city": "Port Harcourt"
        }
    ]
    
    scraper = NigeriaTrafficScraper(google_api_key, weather_api_key, locations)
    scraper.run(interval_minutes=15)
