#!/usr/bin/env python3
"""
玉米病虫害天气数据采集程序
- 从 OpenWeatherMap One Call API 3.0 采集 daily 预报
- 同时产出：weather_history.csv（当日追加）+ forecasts.json（未来8天）
"""
import os, requests, json, csv, time
from datetime import datetime
import pytz

API_KEY = os.getenv('OWM_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"
BEIJING_TZ = pytz.timezone('Asia/Shanghai')

TOWNS_FILE = 'townsNE.csv'
HISTORY_FILE = 'weather_history.csv'
FORECAST_FILE = 'forecasts.json'

HISTORY_FIELDS = [
    'town_id', 'town_name', 'lat', 'lon', 'date',
    'temp_min', 'temp_max', 'temp_avg',
    'humidity_avg', 'humidity_max',
    'dew_point_avg',
    'wind_speed_avg', 'wind_speed_max',
    'precipitation_total',
    'clouds_avg'
]


def load_towns():
    """读取 townsNE.csv，返回 [(id, name, lat, lon, province), ...]"""
    towns = []
    with open(TOWNS_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            # 格式: 序号, 地名, "lat,lon", 省份
            seq = row[0].strip()
            name = row[1].strip()
            coords = row[2].strip().strip('"')
            province = row[3].strip() if len(row) > 3 else ''
            try:
                lat, lon = coords.split(',')
                towns.append((seq, name, lat.strip(), lon.strip(), province))
            except ValueError:
                continue
    return towns


def init_history():
    """如果 history 文件不存在，写入表头"""
    if not os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(HISTORY_FIELDS)
        print(f"[初始化] 创建 {HISTORY_FILE}")


def extract_daily_entry(day, name, town_id, lat, lon):
    """从 daily 预报的一天中提取标准化字段"""
    dt_utc = datetime.fromtimestamp(day['dt'], pytz.utc)
    dt_bj = dt_utc.astimezone(BEIJING_TZ)

    temp = day.get('temp', {})
    temp_min = temp.get('min')
    temp_max = temp.get('max')
    # daily 没有 temp.avg，用 (min+max)/2 近似
    temp_avg = round((temp_min + temp_max) / 2, 1) if temp_min is not None and temp_max is not None else None

    humidity = day.get('humidity')
    dew_point = day.get('dew_point')
    wind_speed = day.get('wind_speed')
    wind_gust = day.get('wind_gust', 0)
    clouds = day.get('clouds')
    rain = day.get('rain', 0)
    snow = day.get('snow', 0)
    precipitation = round(rain + snow, 1)

    weather_list = day.get('weather', [])
    desc = weather_list[0].get('description', '') if weather_list else ''
    uvi = day.get('uvi', 0)

    return {
        'town_name': name,
        'town_id': town_id,
        'lat': lat,
        'lon': lon,
        'date': dt_bj.strftime('%Y-%m-%d'),
        'temp_min': temp_min,
        'temp_max': temp_max,
        'temp_avg': temp_avg,
        'temp_morning': temp.get('morn'),
        'temp_day': temp.get('day'),
        'temp_evening': temp.get('eve'),
        'temp_night': temp.get('night'),
        'humidity': humidity,
        'dew_point': dew_point,
        'wind_speed': wind_speed,
        'wind_gust': wind_gust,
        'wind_deg': day.get('wind_deg'),
        'clouds': clouds,
        'uvi': uvi,
        'rain': rain,
        'snow': snow,
        'precipitation_total': precipitation,
        'weather_desc': desc,
        'pop': day.get('pop', 0),
    }


def fetch_all():
    towns = load_towns()
    print(f"[加载] 共 {len(towns)} 个监测点")

    init_history()

    today_str = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
    forecast_map = {}
    history_rows = []
    success = 0
    fail = 0

    for town_id, name, lat, lon, province in towns:
        params = {
            'lat': lat, 'lon': lon,
            'appid': API_KEY, 'units': 'metric',
            'exclude': 'minutely,hourly,alerts',
            'lang': 'zh_cn'
        }

        try:
            resp = requests.get(BASE_URL, params=params, timeout=10)
            if resp.status_code != 200:
                print(f"  ✗ {name} 请求失败 ({resp.status_code})")
                fail += 1
                continue

            data = resp.json()
            daily = data.get('daily', [])
            if not daily:
                print(f"  ✗ {name} 无 daily 数据")
                fail += 1
                continue

            entries = []
            for day in daily[:8]:
                entry = extract_daily_entry(day, name, town_id, lat, lon)
                entries.append(entry)

            # 保存预报（8天）
            forecast_map[town_id] = entries

            # 保存今天的历史记录
            today_entry = entries[0] if entries else None
            if today_entry:
                history_rows.append([
                    town_id, name, lat, lon, today_str,
                    today_entry['temp_min'], today_entry['temp_max'],
                    today_entry['temp_avg'],
                    today_entry['humidity'], today_entry['humidity'],
                    today_entry['dew_point'],
                    today_entry['wind_speed'], today_entry['wind_gust'],
                    today_entry['precipitation_total'],
                    today_entry['clouds']
                ])

            success += 1
            if success % 50 == 0:
                print(f"  ... 已完成 {success}/{len(towns)}")

            time.sleep(0.15)

        except Exception as e:
            print(f"  ✗ {name} 异常: {e}")
            fail += 1

    # 追加写入今日历史
    if history_rows:
        with open(HISTORY_FILE, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(history_rows)
        print(f"[写入] {HISTORY_FILE} 追加 {len(history_rows)} 行 ({today_str})")

    # 写入预报
    with open(FORECAST_FILE, 'w', encoding='utf-8') as f:
        json.dump(forecast_map, f, ensure_ascii=False, separators=(',', ':'))
    print(f"[写入] {FORECAST_FILE} ({len(forecast_map)} 个地点)")

    print(f"\n--- 采集完成 --- 成功: {success} | 失败: {fail}")


if __name__ == '__main__':
    if not API_KEY:
        print("错误：未设置 OWM_API_KEY 环境变量")
    else:
        fetch_all()
