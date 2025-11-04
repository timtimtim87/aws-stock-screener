import json
import boto3
import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from io import StringIO

def lambda_handler(event, context):
    """
    Daily Data Collector Lambda:
    1. Fetch yesterday's closing prices for Russell 1000
    2. Append to existing S3 CSV files
    3. Calculate updated drawdowns
    4. Get current portfolio snapshot
    5. Save all data to S3
    """
    
    print("🚀 Starting daily Russell 1000 data collection...")
    start_time = time.time()
    
    try:
        # Initialize clients
        ssm = boto3.client('ssm')
        s3 = boto3.client('s3')
        
        # Get credentials
        api_key = ssm.get_parameter(Name='/screener/alpaca/api_key', WithDecryption=True)['Parameter']['Value']
        secret_key = ssm.get_parameter(Name='/screener/alpaca/secret_key', WithDecryption=True)['Parameter']['Value']
        base_url = ssm.get_parameter(Name='/screener/alpaca/base_url')['Parameter']['Value']
        
        headers = {
            'APCA-API-KEY-ID': api_key,
            'APCA-API-SECRET-KEY': secret_key
        }
        
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        print(f"📅 Processing data for: {yesterday}")
        
        # Get Russell 1000 symbols
        symbols = get_russell_1000_symbols()
        print(f"📊 Processing {len(symbols)} Russell 1000 stocks")
        
        # Collect yesterday's closing prices
        daily_data = collect_daily_prices(headers, symbols, yesterday)
        print(f"✅ Collected prices for {len(daily_data)} stocks")
        
        # Append to historical price data
        if daily_data:
            append_to_csv(s3, bucket_name, 'data/russell_1000_daily_prices.csv', daily_data)
        
        # Recalculate drawdowns using updated data
        drawdown_data = calculate_current_drawdowns(s3, bucket_name)
        print(f"✅ Calculated drawdowns for {len(drawdown_data)} stocks")
        
        # Get top 10 candidates
        top_candidates = get_top_candidates(drawdown_data, today)
        save_to_csv(s3, bucket_name, 'data/top_candidates.csv', top_candidates)
        
        # Collect portfolio snapshot
        portfolio_data = collect_portfolio_data(headers, base_url, today)
        print(f"✅ Collected portfolio data for {len(portfolio_data)} positions")
        
        if portfolio_data:
            append_to_csv(s3, bucket_name, 'data/portfolio_snapshots.csv', portfolio_data)
        
        # Calculate execution metrics
        execution_time = time.time() - start_time
        
        summary = {
            'date': today.isoformat(),
            'yesterday_prices_collected': len(daily_data),
            'portfolio_positions': len(portfolio_data),
            'total_stocks_analyzed': len(drawdown_data),
            'worst_drawdown': min([s['drawdown_pct'] for s in drawdown_data]) if drawdown_data else 0,
            'best_candidate': top_candidates[0]['symbol'] if top_candidates else None,
            'execution_time_seconds': round(execution_time, 2),
            'status': 'success'
        }
        
        print(f"✅ Daily collection completed in {execution_time:.1f}s: {summary}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(summary, default=str)
        }
        
    except Exception as e:
        print(f"❌ Error in daily collection: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e), 'status': 'failed'})
        }

def get_russell_1000_symbols():
    """Return Russell 1000 symbols (from your working list)"""
    
    symbols = [
        'FI', 'FMC', 'INSP', 'MOH', 'BRBR', 'SFM', 'GLOB', 'DUOL', 'CHTR', 'CAVA',
        'IT', 'CNC', 'RHI', 'LBRDA', 'LULU', 'BAX', 'DJT', 'SRPT', 'FDS', 'KMX',
        'CMG', 'NWL', 'PRMB', 'MSTR', 'ENPH', 'FLO', 'DECK', 'KVUE', 'FRPT', 'TPL',
        'STZ', 'BAH', 'IRDM', 'AUR', 'COLD', 'HUN', 'BRO', 'KMPR', 'EEFT', 'CPRT',
        'WEN', 'SLGN', 'GPK', 'VRSK', 'LCID', 'PGR', 'AJG', 'MORN', 'HLNE', 'MKTX',
        'CHH', 'OGN', 'TW', 'MUSA', 'HRL', 'GDDY', 'CAG', 'DXCM', 'SMMT', 'ATR',
        'ONON', 'TAP', 'PCTY', 'ELV', 'WSO', 'SAIC', 'CZR', 'BIRK', 'BJ', 'CE',
        'CHE', 'EMN', 'MAN', 'G', 'PAYX', 'HUBS', 'LKQ', 'KBR', 'RYAN', 'KMB',
        'CI', 'KDP', 'MAA', 'ARE', 'PEN', 'OKTA', 'CMCSA', 'LNW', 'MMC', 'CNXC',
        'RLI', 'ROP', 'UDR', 'COLM', 'PRGO', 'ACI', 'AMT', 'SBAC', 'UA', 'BF.A',
        'BBWI', 'LOAR', 'IFF', 'DOW', 'FICO', 'EFX', 'ALGN', 'WING', 'HRB', 'TMUS',
        'NSA', 'CLX', 'PAYC', 'FND', 'PPC', 'INVH', 'WSC', 'CART', 'ACN', 'CROX',
        'PNFP', 'FIS', 'ALK', 'RSG', 'AMH', 'FOUR', 'LYB', 'CARR', 'OKE', 'TREX',
        'IP', 'FTNT', 'INGR', 'CL', 'DV', 'COTY', 'CPAY', 'ICE', 'PM', 'ALSN',
        'DVA', 'EQR', 'MDLZ', 'CBSH', 'CPB', 'EXLS', 'CPT', 'TYL', 'DPZ', 'VIRT',
        'SAM', 'WM', 'CSL', 'AVB', 'UHAL', 'GME', 'UNH', 'AWK', 'DXC', 'TEAM',
        'CLVT', 'CNH', 'ADP', 'CCI', 'BFAM', 'JHX', 'KNSL', 'GIS', 'CTAS', 'MKC',
        'KHC', 'AMCR', 'FNF', 'KD', 'GMED', 'CHWY', 'VRSN', 'SLM', 'COO', 'WLK'
        # Add all your working symbols here - shortened for brevity
    ]
    
    return sorted(list(set(symbols)))

def collect_daily_prices(headers, symbols, date):
    """Collect yesterday's closing prices for all symbols"""
    
    daily_data = []
    batch_size = 50  # Process in smaller batches for daily update
    
    print(f"📊 Collecting prices for {date}")
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        
        for symbol in batch:
            try:
                # Get just yesterday's price
                response = requests.get(
                    f"https://data.alpaca.markets/v2/stocks/{symbol}/bars",
                    headers=headers,
                    params={
                        'timeframe': '1Day',
                        'start': date.isoformat(),
                        'end': (date + timedelta(days=1)).isoformat(),
                        'adjustment': 'all',  # Split-adjusted
                        'feed': 'iex',
                        'limit': 1
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    bars = data.get('bars', [])
                    
                    if bars:
                        bar = bars[0]
                        daily_data.append({
                            'date': date.isoformat(),
                            'symbol': symbol,
                            'open': float(bar['o']),
                            'high': float(bar['h']),
                            'low': float(bar['l']),
                            'close': float(bar['c']),
                            'volume': int(bar['v'])
                        })
                        
            except Exception as e:
                print(f"⚠️  Error collecting {symbol}: {str(e)}")
                continue
            
            # Rate limiting
            time.sleep(0.05)  # 50ms between requests
    
    return daily_data

def append_to_csv(s3_client, bucket_name, file_key, new_data):
    """Append new data to existing CSV file in S3"""
    
    try:
        # Read existing CSV
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            existing_df = pd.read_csv(obj['Body'])
        except s3_client.exceptions.NoSuchKey:
            existing_df = pd.DataFrame()
        
        # Convert new data to DataFrame
        new_df = pd.DataFrame(new_data)
        
        # Append data
        if not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df
        
        # Save back to S3
        csv_buffer = StringIO()
        combined_df.to_csv(csv_buffer, index=False)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        print(f"✅ Appended {len(new_data)} rows to {file_key}")
        
    except Exception as e:
        print(f"❌ Error appending to {file_key}: {str(e)}")
        raise

def save_to_csv(s3_client, bucket_name, file_key, data):
    """Save data to CSV file in S3 (overwrite)"""
    
    try:
        df = pd.DataFrame(data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        print(f"✅ Saved {len(data)} rows to {file_key}")
        
    except Exception as e:
        print(f"❌ Error saving to {file_key}: {str(e)}")
        raise

def calculate_current_drawdowns(s3_client, bucket_name):
    """Recalculate drawdowns using updated price data"""
    
    try:
        # Read current price data
        obj = s3_client.get_object(Bucket=bucket_name, Key='data/russell_1000_daily_prices.csv')
        df = pd.read_csv(obj['Body'])
        
        results = []
        
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('date')
            
            if len(symbol_data) < 10:
                continue
            
            # Calculate rolling high
            symbol_data['rolling_high'] = symbol_data['high'].expanding().max()
            
            # Calculate drawdown
            symbol_data['drawdown_pct'] = (
                (symbol_data['close'] - symbol_data['rolling_high']) / 
                symbol_data['rolling_high'] * 100
            )
            
            # Get latest metrics
            latest = symbol_data.iloc[-1]
            
            # Find peak date
            peak_rows = symbol_data[symbol_data['rolling_high'] == latest['rolling_high']]
            peak_date = peak_rows['date'].iloc[0]
            days_since_peak = (pd.to_datetime(latest['date']) - pd.to_datetime(peak_date)).days
            
            results.append({
                'symbol': symbol,
                'current_price': latest['close'],
                'peak_price': latest['rolling_high'],
                'drawdown_pct': latest['drawdown_pct'],
                'days_since_peak': days_since_peak,
                'last_updated': datetime.now().isoformat()
            })
        
        # Save updated drawdowns
        save_to_csv(s3_client, bucket_name, 'data/russell_1000_drawdowns.csv', results)
        
        return results
        
    except Exception as e:
        print(f"❌ Error calculating drawdowns: {str(e)}")
        return []

def get_top_candidates(drawdown_data, date):
    """Get top 10 worst drawdown candidates"""
    
    if not drawdown_data:
        return []
    
    # Sort by drawdown (most negative first)
    sorted_data = sorted(drawdown_data, key=lambda x: x['drawdown_pct'])
    top_10 = sorted_data[:10]
    
    candidates = []
    for rank, stock in enumerate(top_10, 1):
        candidates.append({
            'date': date.isoformat(),
            'rank': rank,
            'symbol': stock['symbol'],
            'drawdown_pct': stock['drawdown_pct'],
            'current_price': stock['current_price'],
            'peak_price': stock['peak_price'],
            'days_since_peak': stock['days_since_peak']
        })
    
    return candidates

def collect_portfolio_data(headers, base_url, date):
    """Collect current portfolio snapshot"""
    
    portfolio_data = []
    
    try:
        response = requests.get(f"{base_url}/v2/positions", headers=headers, timeout=10)
        
        if response.status_code == 200:
            positions = response.json()
            
            for position in positions:
                try:
                    entry_price = float(position['avg_entry_price'])
                    current_price = float(position['current_price']) if position['current_price'] else entry_price
                    unrealized_return_pct = ((current_price - entry_price) / entry_price) * 100
                    
                    portfolio_data.append({
                        'date': date.isoformat(),
                        'symbol': position['symbol'],
                        'quantity': float(position['qty']),
                        'avg_entry_price': round(entry_price, 2),
                        'current_price': round(current_price, 2),
                        'unrealized_return_pct': round(unrealized_return_pct, 2),
                        'market_value': round(float(position['market_value']), 2),
                        'unrealized_pl': round(float(position['unrealized_pl']), 2)
                    })
                    
                except Exception as e:
                    print(f"⚠️  Error processing position: {str(e)}")
                    
    except Exception as e:
        print(f"⚠️  Error getting portfolio: {str(e)}")
    
    return portfolio_data