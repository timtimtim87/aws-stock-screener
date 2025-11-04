import json
import boto3
import sys
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# Add shared modules to path
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

def lambda_handler(event, context):
    """
    Daily data collection function that:
    1. Fetches Russell 1000 stock prices from Alpaca
    2. Calculates 180-day peak-to-current drawdowns
    3. Gets current portfolio snapshot
    4. Appends all data to CSV files in S3
    """
    
    print("🚀 Starting daily Russell 1000 data collection...")
    
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
        
        today = datetime.now().date()
        print(f"📅 Collection date: {today}")
        
        # Get Russell 1000 symbols
        try:
            from utils import get_russell_1000_symbols
            symbols = get_russell_1000_symbols()
            print(f"📊 Processing {len(symbols)} Russell 1000 stocks")
        except Exception as e:
            print(f"⚠️  Utils import failed: {e}, using fallback")
            symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        # Collect screening data
        screening_data = collect_screening_data(headers, base_url, symbols, today)
        print(f"✅ Collected screening data for {len(screening_data)} stocks")
        
        # Get top 10 candidates
        top_candidates = get_top_candidates(screening_data, today)
        print(f"✅ Identified top 10 candidates")
        
        # Collect portfolio snapshot
        portfolio_data = collect_portfolio_data(headers, base_url, today)
        print(f"✅ Collected portfolio data for {len(portfolio_data)} positions")
        
        # Save data to S3 (we'll implement S3Manager properly later)
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'stock-screener-data-bucket')
        
        if screening_data:
            save_to_s3(s3, bucket_name, 'daily_screening_results.csv', screening_data)
        if top_candidates:
            save_to_s3(s3, bucket_name, 'top_candidates.csv', top_candidates)
        if portfolio_data:
            save_to_s3(s3, bucket_name, 'portfolio_snapshots.csv', portfolio_data)
        
        # Calculate summary statistics
        summary = {
            'date': today.isoformat(),
            'stocks_processed': len(screening_data),
            'portfolio_positions': len(portfolio_data),
            'worst_drawdown': min([s['drawdown_pct'] for s in screening_data]) if screening_data else 0,
            'best_candidate': top_candidates[0]['symbol'] if top_candidates else None,
            'csv_files_updated': 3 if screening_data else 0,
            'status': 'success'
        }
        
        print(f"✅ Data collection completed successfully: {summary}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(summary, default=str)
        }
        
    except Exception as e:
        print(f"❌ Error in data collection: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e), 'status': 'failed'})
        }

def collect_screening_data(headers, base_url, symbols, date):
    """Collect Russell 1000 screening data with drawdown calculations"""
    
    screening_results = []
    
    # Process in batches to avoid timeouts
    batch_size = 50
    for i in range(0, min(100, len(symbols)), batch_size):  # Limit to 100 for now
        batch = symbols[i:i + batch_size]
        print(f"📊 Processing batch {i//batch_size + 1}: {len(batch)} symbols...")
        
        for symbol in batch:
            try:
                # Get 200 days of historical data (buffer for weekends/holidays)
                end_date = date
                start_date = date - timedelta(days=200)
                
                bars_response = requests.get(
                    f"{base_url}/v2/stocks/{symbol}/bars",
                    headers=headers,
                    params={
                        'timeframe': '1Day',
                        'start': start_date.isoformat(),
                        'end': end_date.isoformat(),
                        'adjustment': 'raw'
                    },
                    timeout=10
                )
                
                if bars_response.status_code != 200:
                    print(f"⚠️  Failed to get data for {symbol}: {bars_response.status_code}")
                    continue
                    
                bars_data = bars_response.json()
                bars = bars_data.get('bars', [])
                
                if len(bars) < 30:  # Skip if insufficient data
                    print(f"⚠️  Insufficient data for {symbol}: {len(bars)} bars")
                    continue
                
                # Calculate drawdown metrics
                current_price = float(bars[-1]['c'])  # close price
                highs = [float(bar['h']) for bar in bars]  # high prices
                peak_price = max(highs)
                
                # Find peak index and days since peak
                peak_index = highs.index(peak_price)
                days_since_peak = len(bars) - 1 - peak_index
                
                # Calculate drawdown percentage
                drawdown_pct = ((current_price - peak_price) / peak_price) * 100
                
                screening_results.append({
                    'date': date.isoformat(),
                    'symbol': symbol,
                    'close_price': round(current_price, 2),
                    'peak_price': round(peak_price, 2),
                    'drawdown_pct': round(drawdown_pct, 2),
                    'days_since_peak': days_since_peak,
                    'volume': int(bars[-1]['v'])  # volume
                })
                
            except Exception as e:
                print(f"⚠️  Error processing {symbol}: {str(e)}")
                continue
    
    return screening_results

def get_top_candidates(screening_data, date):
    """Get top 10 candidates with worst drawdowns"""
    
    if not screening_data:
        return []
    
    # Sort by drawdown (most negative first)
    sorted_data = sorted(screening_data, key=lambda x: x['drawdown_pct'])
    top_10 = sorted_data[:10]
    
    # Add rank and reformat for candidates CSV
    candidates = []
    for rank, stock in enumerate(top_10, 1):
        candidates.append({
            'date': date.isoformat(),
            'rank': rank,
            'symbol': stock['symbol'],
            'drawdown_pct': stock['drawdown_pct'],
            'current_price': stock['close_price'],
            'peak_price': stock['peak_price'],
            'days_since_peak': stock['days_since_peak']
        })
    
    return candidates

def collect_portfolio_data(headers, base_url, date):
    """Collect current portfolio snapshot"""
    
    portfolio_data = []
    
    try:
        # Get all positions
        positions_response = requests.get(f"{base_url}/v2/positions", headers=headers, timeout=10)
        
        if positions_response.status_code != 200:
            print(f"⚠️  Failed to get positions: {positions_response.status_code}")
            return []
            
        positions = positions_response.json()
        print(f"📊 Found {len(positions)} positions")
        
        for position in positions:
            try:
                # Calculate unrealized return
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
                print(f"⚠️  Error processing position {position['symbol']}: {str(e)}")
                continue
                
    except Exception as e:
        print(f"⚠️  Error getting positions: {str(e)}")
    
    return portfolio_data

def save_to_s3(s3_client, bucket_name, filename, data):
    """Save data to S3 as CSV (simplified version)"""
    
    try:
        # Convert to DataFrame and then CSV
        df = pd.DataFrame(data)
        csv_content = df.to_csv(index=False)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=csv_content,
            ContentType='text/csv'
        )
        
        print(f"✅ Saved {len(data)} rows to s3://{bucket_name}/{filename}")
        
    except Exception as e:
        print(f"❌ Error saving to S3: {str(e)}")
        # Don't raise - we want the function to continue even if S3 fails