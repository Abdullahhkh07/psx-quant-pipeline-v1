import os
import time
import logging
import datetime
import pandas as pd
from tvDatafeed import TvDatafeed, Interval

# === 1. Setup ===
TICKERS = ['OGDC', 'HBL', 'LUCK', 'ENGROH', 'PSO']
EXCHANGE = 'PSX'
DATA_DIR = "data"
LOG_DIR = "logs"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# === 2. Create a Log File ===
log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.date.today()}.log")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s',
                    handlers=[logging.FileHandler(log_file), logging.StreamHandler()])

# === 3. Initialize TradingView Connection ===
logging.info("Connecting to TradingView...")
tv = TvDatafeed()

def update_stock(ticker):
    file_path = os.path.join(DATA_DIR, f"{ticker}.parquet")
    
    try:
        if os.path.exists(file_path):
            logging.info(f"[{ticker}] Data found. Updating...")
            existing_df = pd.read_parquet(file_path)
            
            new_df = tv.get_hist(symbol=ticker, exchange=EXCHANGE, interval=Interval.in_daily, n_bars=100)
            
            if new_df is not None and not new_df.empty:
                new_df.index = pd.to_datetime(new_df.index).normalize()
                combined_df = pd.concat([existing_df, new_df])
                combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                combined_df.sort_index(inplace=True)
                
                combined_df.to_parquet(file_path)
                logging.info(f"[{ticker}] Successfully updated!")
            else:
                logging.warning(f"[{ticker}] TradingView returned no new data.")
                
        else:
            logging.info(f"[{ticker}] No data found. Downloading 10 years of history...")
            df = tv.get_hist(symbol=ticker, exchange=EXCHANGE, interval=Interval.in_daily, n_bars=3000)
            
            if df is not None and not df.empty:
                df.index = pd.to_datetime(df.index).normalize()
                df.to_parquet(file_path)
                logging.info(f"[{ticker}] 10 years downloaded successfully! ({len(df)} rows)")
            else:
                logging.warning(f"[{ticker}] Failed to download history from TradingView.")

    except Exception as e:
        logging.error(f"[{ticker}] FAILED. Reason: {e}")

if __name__ == "__main__":
    for stock in TICKERS:
        update_stock(stock)
        # THE MAGIC ANTI-BOT SHIELD
        logging.info("Sleeping for 5 seconds to bypass rate limits...")
        time.sleep(5)
        
    logging.info("=== ALL DONE! ===")