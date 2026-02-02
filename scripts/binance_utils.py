import os
import requests
import zipfile
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time


def download_binance_bulk_zip(symbol: str, interval: str, date: str, output_dir: str) -> Optional[str]:
    url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
    
    os.makedirs(output_dir, exist_ok=True)
    zip_path = os.path.join(output_dir, f"{symbol}-{interval}-{date}.zip")
    csv_path = os.path.join(output_dir, f"{symbol}-{interval}-{date}.csv")
    
    if os.path.exists(csv_path):
        print(f"File already exists: {csv_path}")
        return csv_path
    
    print(f"Downloading: {url}")
    
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        
        print(f"Extracting: {zip_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        
        os.remove(zip_path)
        
        if os.path.exists(csv_path):
            print(f"Successfully downloaded and extracted: {csv_path}")
            return csv_path
        else:
            print(f"Warning: CSV file not found after extraction")
            return None
            
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Data not available for {symbol} on {date}")
        else:
            print(f"HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"Error downloading {symbol} for {date}: {str(e)}")
        return None


def download_binance_api_klines(
    symbol: str,
    interval: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 1000
) -> List[List]:
    url = "https://data-api.binance.vision/api/v3/klines"
    
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"Fetched {len(data)} klines for {symbol}")
        return data
        
    except Exception as e:
        print(f"Error fetching API data for {symbol}: {str(e)}")
        return []


def save_klines_to_csv(klines: List[List], output_path: str, symbol: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        
        for kline in klines:
            writer.writerow(kline[:12])
    
    print(f"Saved {len(klines)} klines to {output_path}")


def generate_date_range(start_date: datetime, end_date: datetime) -> List[str]:
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    return dates


def download_historical_range(
    symbol: str,
    interval: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: str
) -> List[str]:
    dates = generate_date_range(start_date, end_date)
    downloaded_files = []
    
    print(f"Downloading {len(dates)} days of data for {symbol}")
    
    for date in dates:
        csv_path = download_binance_bulk_zip(symbol, interval, date, output_dir)
        if csv_path:
            downloaded_files.append(csv_path)
        
        time.sleep(0.1)
    
    print(f"Successfully downloaded {len(downloaded_files)}/{len(dates)} files for {symbol}")
    return downloaded_files


def merge_csv_files(input_files: List[str], output_file: str):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    all_rows = []
    seen = set()
    
    for file_path in input_files:
        if not os.path.exists(file_path):
            continue
            
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 12:
                    key = f"{row[0]}_{row[6]}"
                    if key not in seen:
                        seen.add(key)
                        all_rows.append(row[:12])
    
    all_rows.sort(key=lambda x: int(x[0]))
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(all_rows)
    
    print(f"Merged {len(all_rows)} unique records into {output_file}")
    return output_file


def unix_ms_from_date(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)


def fetch_recent_klines_api(
    symbol: str,
    interval: str,
    days: int,
    output_path: str
) -> Optional[str]:
    end_time = int(datetime.utcnow().timestamp() * 1000)
    start_time = end_time - (days * 24 * 60 * 60 * 1000)
    
    all_klines = []
    current_start = start_time
    
    while current_start < end_time:
        klines = download_binance_api_klines(
            symbol=symbol,
            interval=interval,
            start_time=current_start,
            limit=1000
        )
        
        if not klines:
            break
        
        all_klines.extend(klines)
        
        last_time = int(klines[-1][6])
        if last_time >= end_time:
            break
        
        current_start = last_time + 1
        time.sleep(0.5)
    
    if all_klines:
        save_klines_to_csv(all_klines, output_path, symbol)
        return output_path
    
    return None