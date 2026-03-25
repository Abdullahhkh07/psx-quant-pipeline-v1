from tvDatafeed import TvDatafeed, Interval

print("Connecting to TradingView...")
tv = TvDatafeed()

print("Testing ENGRO...")
df = tv.get_hist(symbol='ENGRO', exchange='PSX', interval=Interval.in_daily, n_bars=10)

if df is not None and not df.empty:
    print("SUCCESS! ENGRO exists.")
    print(df.head())
else:
    print("FAILED. TradingView could not find ENGRO under this symbol/exchange.")
    
    # Let's search for what it might actually be called
    print("\nSearching TradingView for 'ENGRO'...")
    search_results = tv.search_symbol('ENGRO')
    for result in search_results:
        if result.get('exchange') == 'PSX':
             print(f"Found on PSX: {result.get('symbol')} - {result.get('description')}")