import pandas as pd
import requests
import yfinance as yf
from io import StringIO
from database import SessionLocal, TDCCData
import datetime
import random

TDCC_URL = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"

def _generate_mock_historical_data(db, df, latest_date, weeks=12):
    """
    Since actual historical data is not easily available via the simple URL,
    we mock the past `weeks` to demonstrate the 'continuous' feature.
    """
    # Mock date 1 to `weeks`
    mock_dates = []
    for w in range(1, weeks + 1):
        d = (datetime.datetime.strptime(latest_date, "%Y%m%d") - datetime.timedelta(days=7*w)).strftime("%Y%m%d")
        mock_dates.append((d, w))
    
    # We want some stocks to actually pass our filter!
    # Let's say stock '2330' will pass: Retail decreasing, Large increasing
    # So 12 weeks ago: Retail very high, Large very low
    
    records_to_insert = []
    
    for date_str, week_idx in mock_dates:
        df_mock = df.copy()
        df_mock['date'] = date_str
        
        # Add some random variations to make it look realistic
        def modify_people(row, w_idx):
            # Only make some stocks pass the filter (e.g. stock_id ends with '0')
            # to avoid overwhelming yfinance during backtests.
            if not row['stock_id'].endswith('0'):
                return row['people']
                
            # level 1-9 (retail)
            # Make retail higher in the past (so it's decreasing now)
            if row['level'] <= 9:
                return int(row['people'] * (1 + 0.05 * w_idx))
            return row['people']
            
        def modify_percent(row, w_idx):
            if not row['stock_id'].endswith('0'):
                return row['percent']
                
            # Make large lower in the past (so it's increasing now)
            if row['level'] >= 14:
                return max(0.01, row['percent'] * (1 - 0.05 * w_idx))
            return row['percent']
            
        df_mock['people'] = df_mock.apply(lambda r: modify_people(r, week_idx), axis=1)
        df_mock['percent'] = df_mock.apply(lambda r: modify_percent(r, week_idx), axis=1)
        records_to_insert.extend(df_mock.to_dict(orient='records'))
        
    # Insert in chunks to avoid locking the database for too long or using too much memory
    chunk_size = 50000
    for i in range(0, len(records_to_insert), chunk_size):
        db.bulk_insert_mappings(TDCCData, records_to_insert[i:i+chunk_size])
        db.commit()
        
    print("Mock historical data generated for demonstration.")

def download_and_update_tdcc(weeks=12):
    print("Downloading TDCC data...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # Disable SSL warnings and use verify=False due to TDCC certificate issues
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    res = requests.get(TDCC_URL, headers=headers, verify=False)
    res.encoding = 'utf-8'
    
    try:
        df = pd.read_csv(StringIO(res.text))
    except Exception as e:
        print(f"Error parsing CSV: {e}")
        return {"status": "error", "message": f"CSV parsing failed: {e}"}
    
    if df.empty:
        print("Error: Downloaded TDCC data is empty.")
        return {"status": "error", "message": "Downloaded TDCC data is empty."}
        
    # Check if column count matches
    if len(df.columns) < 6:
        print(f"Error: Unexpected column count. Found {len(df.columns)} columns. Raw: {df.columns.tolist()}")
        return {"status": "error", "message": f"Unexpected data format (columns: {len(df.columns)})"}

    # CSV Columns: 資料日期, 證券代號, 持股分級, 人數, 股數, 占集保庫存數比例%
    df.columns = ["date", "stock_id", "level", "people", "shares", "percent"]
    df['date'] = df['date'].astype(str)
    df['stock_id'] = df['stock_id'].astype(str)
    
    # Filter only 4-digit stock IDs to drastically reduce DB size and speed up mock generation
    # Also trim any whitespace
    df['stock_id'] = df['stock_id'].str.strip()
    df = df[df['stock_id'].str.len() == 4]
    
    if df.empty:
        print("Error: No 4-digit stock IDs found in the data.")
        # Debug: show some original stock IDs
        return {"status": "error", "message": "No valid stock data found after filtering."}
    
    db = SessionLocal()
    latest_date = df['date'].iloc[0]
    
    count = db.query(TDCCData.date).distinct().count()
    existing = db.query(TDCCData).filter(TDCCData.date == latest_date).first()
    
    if existing:
        # We already have the latest week.
        if count >= weeks + 1:
            print(f"Data for {latest_date} already exists and length is sufficient ({count} >= {weeks + 1}).")
            db.close()
            return {"status": "already_updated", "date": latest_date}
        else:
            print(f"Expanding mock history... User requested {weeks} weeks, but only {count-1} exist.")
            db.query(TDCCData).delete()
            db.bulk_insert_mappings(TDCCData, df.to_dict(orient='records'))
            db.commit()
            _generate_mock_historical_data(db, df, latest_date, weeks=weeks)
            db.close()
            return {"status": "success", "date": latest_date}
            
    else:
        # We don't have the latest week. This is a NEW update!
        print(f"Inserting new TDCC data for {latest_date} into database...")
        # Do NOT delete existing data! Just append the new week.
        db.bulk_insert_mappings(TDCCData, df.to_dict(orient='records'))
        db.commit()
        
        # If the DB was completely empty before this, generate initial mock data.
        if count == 0 and weeks > 0:
            print(f"Initial setup: Generating mock historical data for {weeks} weeks...")
            _generate_mock_historical_data(db, df, latest_date, weeks=weeks)
            
        db.close()
        print("Update complete.")
        return {"status": "success", "date": latest_date}

def batch_download_prices(stock_ids, period="2y"):
    """
    Downloads data for multiple stocks at once to speed up processing.
    """
    if not stock_ids:
        return {}
        
    import logging
    logging.getLogger('yfinance').setLevel(logging.CRITICAL)
    
    # Try both .TW and .TWO for all stocks
    all_tickers = []
    for sid in stock_ids:
        all_tickers.append(f"{sid}.TW")
        all_tickers.append(f"{sid}.TWO")
        
    all_data = {}
    chunk_size = 50
    for i in range(0, len(all_tickers), chunk_size):
        chunk = all_tickers[i:i+chunk_size]
        try:
            # download returns a DataFrame with MultiIndex if multiple tickers
            data = yf.download(chunk, period=period, group_by='ticker', progress=False, threads=True)
            for t in chunk:
                if t in data and not data[t].empty:
                    df = data[t].dropna(subset=['Close'])
                    if not df.empty:
                        df.index = df.index.tz_localize(None).normalize()
                        all_data[t] = df
        except Exception as e:
            print(f"Error in batch download chunk: {e}")

    # Map back to stock_id and pre-calculate MA
    final_cache = {}
    for sid in stock_ids:
        df = None
        if f"{sid}.TW" in all_data:
            df = all_data[f"{sid}.TW"]
        elif f"{sid}.TWO" in all_data:
            df = all_data[f"{sid}.TWO"]
            
        if df is not None and len(df) >= 20:
            # Pre-calculate MA to avoid repetitive work in Phase 3
            df['MA20'] = df['Close'].rolling(window=20).mean()
            final_cache[sid] = df
            
    return final_cache

def get_stock_price_and_ma(stock_id: str, ma_days=20, target_date=None, cache=None):
    # Silence yfinance
    import logging
    logging.getLogger('yfinance').setLevel(logging.CRITICAL)
    try:
        if cache is not None:
            if stock_id in cache:
                hist = cache[stock_id]
            else:
                return None, None
        else:
            ticker = f"{stock_id}.TW"
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2y")
            if hist.empty:
                ticker = f"{stock_id}.TWO"
                stock = yf.Ticker(ticker)
                hist = stock.history(period="2y")
            if not hist.empty:
                hist.index = hist.index.tz_localize(None).normalize()
                hist['MA20'] = hist['Close'].rolling(window=ma_days).mean()
            
        if hist is None or hist.empty:
            return None, None
            
        # Ensure index is normalized for lookup
        if target_date:
            target_dt = datetime.datetime.strptime(target_date, "%Y%m%d")
            # Use 'asof' to find the closest date <= target_dt
            # This is much faster than filtering and copying the DataFrame
            idx = hist.index.asof(target_dt)
            if pd.isna(idx):
                return None, None
            
            row = hist.loc[idx]
            close_val = row['Close']
            ma_val = row['MA20']
            
            if pd.isna(ma_val):
                return None, None
            return float(close_val), float(ma_val)
        else:
            # No target date, get latest
            last_row = hist.iloc[-1]
            if pd.isna(last_row['MA20']):
                return None, None
            return float(last_row['Close']), float(last_row['MA20'])

    except Exception as e:
        print(f"Error fetching price for {stock_id}: {e}")
        return None, None

