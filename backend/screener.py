import pandas as pd
from database import SessionLocal, TDCCData
from scraper import get_stock_price_and_ma, batch_download_prices

import time

def run_screener(retail_level=9, large_level=14, weeks=3, ma_diff_percent=5.0, start_date=None, end_date=None, progress_callback=None):
    db = SessionLocal()
    
    try:
        # Get all distinct dates ordered by desc
        all_dates = db.query(TDCCData.date).distinct().order_by(TDCCData.date.desc()).all()
        all_dates = [d[0] for d in all_dates]
        
        if not all_dates:
            return {"error": "No historical data found in database."}
            
        target_dates = all_dates.copy()
        if start_date:
            target_dates = [d for d in target_dates if d >= start_date]
        if end_date:
            target_dates = [d for d in target_dates if d <= end_date]
        
        if len(all_dates) < weeks:
            return {"error": f"Not enough historical data. Require {weeks} weeks, but only have {len(all_dates)} weeks."}
            
        all_candidates_by_date = {}
        unique_stock_ids = set()
        total_dates = len(target_dates)
        if total_dates == 0:
            return {"error": "No data matches the selected date range."}
        start_time = time.time()
        
        # Phase 1: Screen by TDCC data (Local DB)
        for idx, t_date in enumerate(target_dates):
            if progress_callback:
                # Local screening is fast, but let's show some progress
                percent = int((idx / total_dates) * 70) # Up to 70% for DB work
                progress_callback(percent, -1)

            t_idx = all_dates.index(t_date)
            if t_idx + weeks > len(all_dates):
                continue
                
            dates_for_target = all_dates[t_idx:t_idx+weeks]
            dates_for_target.reverse() 
            
            query = db.query(TDCCData).filter(TDCCData.date.in_(dates_for_target)).statement
            df = pd.read_sql(query, db.bind)
            df = df[df['stock_id'].str.len() == 4]
            
            df_retail = df[df['level'] <= retail_level].groupby(['stock_id', 'date'])['people'].sum().reset_index()
            df_large = df[df['level'] >= large_level].groupby(['stock_id', 'date'])['percent'].sum().reset_index()
            
            retail_pivot = df_retail.pivot(index='stock_id', columns='date', values='people').dropna()
            large_pivot = df_large.pivot(index='stock_id', columns='date', values='percent').dropna()
            
            candidates = []
            for stock_id in retail_pivot.index:
                if stock_id not in large_pivot.index:
                    continue
                r_vals = retail_pivot.loc[stock_id].values
                l_vals = large_pivot.loc[stock_id].values
                if len(r_vals) < weeks or len(l_vals) < weeks:
                    continue
                
                retail_decreasing = all(r_vals[i] > r_vals[i+1] for i in range(len(r_vals)-1))
                large_increasing = all(l_vals[i] < l_vals[i+1] for i in range(len(l_vals)-1))
                
                if retail_decreasing and large_increasing:
                    candidates.append({
                        "trigger_date": f"{t_date[:4]}-{t_date[4:6]}-{t_date[6:]}",
                        "stock_id": stock_id,
                        "retail_current": int(r_vals[-1]),
                        "retail_change": int(r_vals[-1] - r_vals[0]),
                        "large_current_pct": round(l_vals[-1], 2),
                        "large_change_pct": round(l_vals[-1] - l_vals[0], 2)
                    })
                    unique_stock_ids.add(stock_id)
            
            all_candidates_by_date[t_date] = candidates

        # Phase 2: Batch Download from yfinance
        if progress_callback:
            progress_callback(75, -1) # Stage: Downloading
            
        price_cache = batch_download_prices(list(unique_stock_ids))
        
        # Phase 3: Final Filtering by MA (Local Calculation)
        all_final_results = []
        total_candidates = sum(len(c) for c in all_candidates_by_date.values())
        processed_count = 0
        
        for t_date in target_dates:
            candidates = all_candidates_by_date.get(t_date, [])
            for stock in candidates:
                processed_count += 1
                if progress_callback and processed_count % 10 == 0:
                    # Move from 90% to 100%
                    p3_percent = 90 + int((processed_count / max(1, total_candidates)) * 10)
                    progress_callback(min(99, p3_percent), -1)

                stock_id = stock['stock_id']
                close_price, ma20 = get_stock_price_and_ma(stock_id, target_date=t_date, cache=price_cache)
                
                if close_price and ma20:
                    diff_pct = abs(close_price - ma20) / ma20 * 100
                    if diff_pct <= ma_diff_percent:
                        stock['close'] = round(close_price, 2)
                        stock['ma20'] = round(ma20, 2)
                        stock['ma_diff_pct'] = round((close_price - ma20) / ma20 * 100, 2)
                        all_final_results.append(stock)
                        
        if progress_callback:
            progress_callback(100, 0)
            
        return all_final_results
    finally:
        db.close()
