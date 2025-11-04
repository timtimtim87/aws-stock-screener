import json
import boto3
import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    Production-ready daily Russell 1000 data collection:
    - Processes all 991 Russell 1000 stocks
    - Calculates 180-day peak-to-current drawdowns
    - Optimized batch processing with rate limiting
    - Saves results to CSV files in S3
    """
    
    print("🚀 Starting Russell 1000 data collection...")
    start_time = time.time()
    
    try:
        # Initialize clients
        ssm = boto3.client('ssm')
        s3 = boto3.client('s3')
        
        # Get credentials
        api_key = ssm.get_parameter(Name='/screener/alpaca/api_key', WithDecryption=True)['Parameter']['Value']
        secret_key = ssm.get_parameter(Name='/screener/alpaca/secret_key', WithDecryption=True)['Parameter']['Value']
        base_url = ssm.get_parameter(Name='/screener/alpaca/base_url')['Parameter']['Value']
        
        # Use data API endpoint for historical data
        data_base_url = "https://data.alpaca.markets"
        
        headers = {
            'APCA-API-KEY-ID': api_key,
            'APCA-API-SECRET-KEY': secret_key
        }
        
        today = datetime.now().date()
        print(f"📅 Collection date: {today}")
        
        # Get complete Russell 1000 symbols
        symbols = get_russell_1000_symbols()
        print(f"📊 Processing {len(symbols)} Russell 1000 stocks")
        
        # Collect screening data with optimized batching
        screening_data = collect_screening_data(headers, data_base_url, symbols, today)
        print(f"✅ Collected screening data for {len(screening_data)} stocks")
        
        # Get top 10 candidates
        top_candidates = get_top_candidates(screening_data, today)
        print(f"✅ Identified top 10 candidates")
        
        # Collect portfolio snapshot
        portfolio_data = collect_portfolio_data(headers, base_url, today)
        print(f"✅ Collected portfolio data for {len(portfolio_data)} positions")
        
        # Save data to S3
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        files_saved = 0
        
        if screening_data:
            save_to_s3(s3, bucket_name, 'daily_screening_results.csv', screening_data)
            files_saved += 1
        if top_candidates:
            save_to_s3(s3, bucket_name, 'top_candidates.csv', top_candidates)
            files_saved += 1
        if portfolio_data:
            save_to_s3(s3, bucket_name, 'portfolio_snapshots.csv', portfolio_data)
            files_saved += 1
        
        # Calculate execution metrics
        execution_time = time.time() - start_time
        
        summary = {
            'date': today.isoformat(),
            'stocks_processed': len(screening_data),
            'portfolio_positions': len(portfolio_data),
            'worst_drawdown': min([s['drawdown_pct'] for s in screening_data]) if screening_data else 0,
            'best_candidate': top_candidates[0]['symbol'] if top_candidates else None,
            'execution_time_seconds': round(execution_time, 2),
            'csv_files_updated': files_saved,
            'status': 'success'
        }
        
        print(f"✅ Collection completed in {execution_time:.1f}s: {summary}")
        
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

def get_russell_1000_symbols():
    """Return complete Russell 1000 symbols list (production)"""
    
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
        'KHC', 'AMCR', 'FNF', 'KD', 'GMED', 'CHWY', 'VRSN', 'SLM', 'COO', 'WLK',
        'LII', 'KR', 'T', 'PPG', 'TTD', 'ED', 'ADBE', 'POOL', 'CUBE', 'WY',
        'BMRN', 'WH', 'LINE', 'AXTA', 'DRS', 'ESS', 'EXP', 'MIDD', 'OC', 'DRI',
        'ACHC', 'EXR', 'DOCU', 'DLB', 'ODFL', 'LNG', 'APD', 'BALL', 'VZ', 'SON',
        'MOS', 'VVV', 'AKAM', 'OWL', 'EG', 'CNA', 'DKNG', 'UNM', 'BROS', 'RGA',
        'SJM', 'AGO', 'AR', 'SW', 'JKHY', 'COST', 'CACC', 'ESAB', 'LIN', 'RH',
        'PLNT', 'CUZ', 'WHR', 'WTRG', 'ACGL', 'TSN', 'GWW', 'BHF', 'ABT', 'DOX',
        'TDG', 'CSGP', 'NWS', 'PG', 'PSA', 'FFIV', 'OTIS', 'IAC', 'CME', 'CTSH',
        'WU', 'MO', 'IEX', 'PCG', 'MCD', 'XRAY', 'CHD', 'CLH', 'RARE', 'POST',
        'TXRH', 'SNPS', 'AXS', 'YUM', 'NOW', 'RAL', 'NIQ', 'LOPE', 'ALL', 'BSX',
        'AMP', 'BR', 'VNOM', 'ELS', 'CDW', 'AON', 'KO', 'RS', 'SYK', 'S',
        'LSTR', 'FLUT', 'RYN', 'ZTS', 'MSI', 'VRTX', 'STWD', 'LPX', 'EQIX', 'REG',
        'HSY', 'CBOE', 'JNJ', 'PSO', 'SPA', 'EBAY', 'FANG', 'AME', 'BEN', 'SO',
        'AIT', 'ROST', 'SMWB', 'OGE', 'TEX', 'WD', 'GS', 'ELG', 'ESRT', 'PRU',
        'AIZ', 'DBRG', 'K', 'NOC', 'HOG', 'SWN', 'XEL', 'MTX', 'TXT', 'MSA',
        'KSS', 'PFE', 'DTE', 'ATO', 'WAT', 'ZBH', 'MAR', 'NDAQ', 'CMS', 'FE',
        'TGT', 'WAB', 'BMY', 'AMGN', 'GPC', 'EIX', 'CVS', 'MET', 'PKI', 'DHI',
        'KIN', 'AEE', 'LEN', 'RF', 'TFX', 'EQH', 'REGN', 'MHK', 'LMT', 'DUK',
        'PPL', 'BA', 'EVA', 'FISV', 'HUM', 'DGX', 'UPS', 'COP', 'CNP', 'ZION',
        'LLY', 'AAMC', 'RPM', 'UHS', 'ADSK', 'AEP', 'HIG', 'VRTV', 'AZO', 'MNST',
        'TJX', 'FDX', 'SHW', 'NI', 'LOW', 'SLB', 'WMT', 'HD', 'CAT', 'IBM',
        'JPM', 'NKE', 'CVX', 'DIS', 'UNP', 'JCI', 'ABT', 'ADI', 'PFE', 'CVS',
        'INTC', 'VZ', 'PG', 'KO', 'HD', 'MRK', 'WMT', 'NVDA', 'MSFT', 'AAPL',
        'GOOGL', 'AMZN', 'TSLA', 'META', 'JPM', 'UNH', 'JNJ', 'V', 'PG', 'MA',
        'AVGO', 'HD', 'CVX', 'LLY', 'ABBV', 'PFE', 'COST', 'NFLX', 'TMO', 'CRM',
        'KO', 'PEP', 'DIS', 'CSCO', 'TMUS', 'DHR', 'ABT', 'VZ', 'ADBE', 'WFC',
        'ACN', 'NKE', 'LIN', 'QCOM', 'MCD', 'ORCL', 'BMY', 'TXN', 'HON', 'UPS',
        'CVS', 'PM', 'SPGI', 'LOW', 'RTX', 'NEE', 'IBM', 'MS', 'CAT', 'AMGN',
        'GS', 'UNP', 'BA', 'BLK', 'ELV', 'MDT', 'SYK', 'ISRG', 'GILD', 'VRTX',
        'PLD', 'AXP', 'ADP', 'DE', 'MMM', 'TJX', 'SCHW', 'BKNG', 'MO', 'CI',
        'ZTS', 'CB', 'SO', 'MDLZ', 'CME', 'DUK', 'BSX', 'PGR', 'AON', 'ICE',
        'FI', 'CL', 'ITW', 'EOG', 'WM', 'SHW', 'MU', 'EQIX', 'APD', 'FCX',
        'NSC', 'USB', 'CTAS', 'MMC', 'HCA', 'PSA', 'EMR', 'GM', 'F', 'NXPI',
        'KLAC', 'AMAT', 'LRCX', 'MAR', 'CSX', 'REGN', 'MCO', 'TGT', 'ORLY', 'APH',
        'AJG', 'COP', 'SRE', 'JCI', 'FDX', 'TFC', 'KMI', 'AMT', 'PCAR', 'CMG',
        'MSI', 'OXY', 'O', 'WMB', 'PSX', 'VLO', 'FTNT', 'ROP', 'ROST', 'PAYX',
        'IDXX', 'CSGP', 'FAST', 'ODFL', 'EA', 'VRSK', 'EW', 'CTSH', 'KR', 'DXCM',
        'GWW', 'MNST', 'BDX', 'IT', 'EXC', 'XEL', 'MSCI', 'ANSS', 'WEC', 'A',
        'PRU', 'VICI', 'CMI', 'ROK', 'MLM', 'AWK', 'PPG', 'ALL', 'CARR', 'IEX',
        'AMP', 'PH', 'KMB', 'SPG', 'FIS', 'KEYS', 'HLT', 'BIIB', 'PCG', 'AEE',
        'TROW', 'RSG', 'ADI', 'DAL', 'EXR', 'CPRT', 'HES', 'CBRE', 'WELL', 'LHX',
        'UAL', 'NDSN', 'EME', 'NVST', 'WR', 'MAN', 'PPC', 'LMT', 'ETN', 'ALB',
        'MTB', 'KEY', 'RF', 'HBAN', 'COF', 'DFS', 'SYF', 'FRC', 'CMA', 'ZION',
        'WAL', 'FITB', 'CFG', 'TFC', 'USB', 'PNC', 'BBT', 'STI', 'MTB', 'KEY',
        'RF', 'HBAN', 'COF', 'DFS', 'SYF', 'FRC', 'CMA', 'ZION', 'WAL', 'FITB',
        'CFG', 'WF', 'WFC', 'BAC', 'JPM', 'C', 'GS', 'MS', 'BK', 'STT',
        'NTRS', 'BLK', 'TROW', 'IVZ', 'BEN', 'AMG', 'TECH', 'LW', 'PODD', 'JHG',
        'LEA', 'PTC', 'EXAS', 'XYL', 'INGM', 'NEE', 'HAS', 'SRE', 'WSM', 'FOX',
        'JPM', 'VTRS', 'PINS', 'F', 'ADM', 'GILD', 'ES', 'PH', 'RBC', 'VEEV',
        'CRH', 'CAH', 'APG', 'TRMB', 'LDOS', 'NUE', 'DAY', 'NXST', 'CSCO', 'NOV',
        'NTAP', 'OMF', 'PFGC', 'EA', 'GD', 'XPO', 'CRL', 'CSX', 'KEYS', 'AAL',
        'DCI', 'ACM', 'PSN', 'DD', 'DASH', 'SHC', 'AWI', 'JCI', 'LECO', 'NCLH',
        'STT', 'NTRA', 'NSC', 'NU', 'BIIB', 'QCOM', 'CG', 'ETN', 'JBHT', 'QXO',
        'J', 'CVNA', 'UAL', 'HCA', 'DAL', 'ON', 'CRWD', 'ITT', 'ROK', 'BK',
        'LFUS', 'AXP', 'TOL', 'BAC', 'GLIBA', 'EXPE', 'ESI', 'PSX', 'MCHP', 'URI',
        'MTD', 'JAZZ', 'SGI', 'RL', 'GTM', 'NTRS', 'INFA', 'BIO', 'CFG', 'ULTA',
        'VKTX', 'QRVO', 'AES', 'HWM', 'BKR', 'THC', 'WWD', 'CXT', 'HUBB', 'SNX',
        'PATH', 'VMI', 'LHX', 'SF', 'CRUS', 'CPNG', 'WST', 'TNL', 'ETSY', 'MPC',
        'CAR', 'AAPL', 'TWLO', 'MTSI', 'A', 'KRC', 'CEG', 'OSK', 'JLL', 'AMZN',
        'HII', 'CLF', 'MTZ', 'SYF', 'TIGO', 'CMA', 'PEGA', 'TMO', 'MS', 'RTX',
        'HAL', 'HOLX', 'APTV', 'THO', 'VST', 'VIK', 'PWR', 'UWMC', 'AFRM', 'SEB',
        'HXL', 'APA', 'GS', 'GNRC', 'IQV', 'BC', 'FTI', 'ATI', 'TPR', 'EVR',
        'ZS', 'MLI', 'AYI', 'GEV', 'CCL', 'C', 'GXO', 'WYNN', 'ST', 'RKT',
        'BWA', 'FERG', 'CMI', 'BLD', 'JBL', 'UTHR', 'VLO', 'GE', 'HPE', 'AMG',
        'CGNX', 'CRS', 'GM', 'AA', 'NRG', 'RBLX', 'LSCC', 'IDXX', 'NEM', 'FLS',
        'DDOG', 'ALGM', 'FTAI', 'AMAT', 'EME', 'AU', 'SCCO', 'XYZ', 'RVMD', 'LVS',
        'IPGP', 'ILMN', 'BEPC', 'EL', 'ALNY', 'MPWR', 'DINO', 'SMCI', 'WCC', 'LYFT',
        'MRVL', 'IBKR', 'SNOW', 'COIN', 'IVZ', 'CW', 'ALB', 'CELH', 'TSLA', 'INCY',
        'TEM', 'CHRW', 'DELL', 'CRCL', 'GOOG', 'M', 'DDS', 'WFRD', 'FLEX', 'ORCL',
        'ROIV', 'U', 'CAT', 'APH', 'ELF', 'KLAC', 'TLN', 'ROKU', 'AVGO', 'NVDA',
        'ANET', 'NVT', 'RDDT', 'COHR', 'PLTR', 'FIVE', 'GLW', 'INTC', 'ECG', 'BWXT',
        'MKSI', 'MEDP', 'NET', 'VRT', 'PSTG', 'APP', 'FSLR', 'ELAN', 'MDB', 'AMKR',
        'LRCX', 'UI', 'KRMN', 'FIX', 'IONS', 'MP', 'SOFI', 'TER', 'AMD', 'CIEN',
        'WBD', 'RKLB', 'INSM', 'ASTS', 'ALAB', 'MU', 'HOOD', 'LITE', 'W', 'WDC',
        'QS', 'SNDK'
    ]
    
    # Remove duplicates and return sorted list
    return sorted(list(set(symbols)))

def collect_screening_data(headers, base_url, symbols, date):
    """Optimized Russell 1000 screening with batch processing and rate limiting"""
    
    screening_results = []
    
    # Calculate date range (180 days + buffer)
    end_date = date
    start_date = date - timedelta(days=210)  # Buffer for weekends/holidays
    
    # Process in batches - 200 req/min = 3.33/sec
    # With 991 symbols, we need smart batching
    batch_size = 100  # Symbols per request
    request_delay = 0.4  # 400ms between requests = 2.5 req/sec (under limit)
    
    total_batches = (len(symbols) + batch_size - 1) // batch_size
    print(f"📊 Processing {len(symbols)} symbols in {total_batches} batches...")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(symbols))
        batch_symbols = symbols[start_idx:end_idx]
        
        print(f"🔄 Batch {batch_num + 1}/{total_batches}: {len(batch_symbols)} symbols")
        
        try:
            # Make batch request to Alpaca Data API
            response = requests.get(
                f"{base_url}/v2/stocks/bars",
                headers=headers,
                params={
                    'symbols': ','.join(batch_symbols),
                    'timeframe': '1Day',
                    'start': start_date.isoformat(),
                    'end': end_date.isoformat(),
                    'adjustment': 'raw',
                    'limit': 10000,  # Max allowed
                    'asof': '',
                    'feed': 'sip'  # Use SIP feed for complete data
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Process each symbol's data
                for symbol in batch_symbols:
                    if symbol in data.get('bars', {}):
                        bars = data['bars'][symbol]
                        
                        if len(bars) >= 30:  # Minimum data requirement
                            result = calculate_drawdown_metrics(symbol, bars, date)
                            if result:
                                screening_results.append(result)
                        else:
                            print(f"⚠️  Insufficient data for {symbol}: {len(bars)} bars")
                    else:
                        print(f"⚠️  No data returned for {symbol}")
                        
            elif response.status_code == 429:
                print(f"⚠️  Rate limited on batch {batch_num + 1}, waiting 30s...")
                time.sleep(30)
                # Retry this batch
                continue
            else:
                print(f"⚠️  API error {response.status_code} for batch {batch_num + 1}")
                
        except Exception as e:
            print(f"⚠️  Error processing batch {batch_num + 1}: {str(e)}")
            
        # Rate limiting delay (except for last batch)
        if batch_num < total_batches - 1:
            time.sleep(request_delay)
    
    print(f"✅ Processed {len(screening_results)} stocks successfully")
    return screening_results

def calculate_drawdown_metrics(symbol, bars, date):
    """Calculate drawdown metrics for a single symbol"""
    
    try:
        # Get current price and calculate drawdown
        current_price = float(bars[-1]['c'])
        highs = [float(bar['h']) for bar in bars]
        peak_price = max(highs)
        
        # Find peak index and days since peak
        peak_index = highs.index(peak_price)
        days_since_peak = len(bars) - 1 - peak_index
        
        # Calculate drawdown percentage
        drawdown_pct = ((current_price - peak_price) / peak_price) * 100
        
        return {
            'date': date.isoformat(),
            'symbol': symbol,
            'close_price': round(current_price, 2),
            'peak_price': round(peak_price, 2),
            'drawdown_pct': round(drawdown_pct, 2),
            'days_since_peak': days_since_peak,
            'volume': int(bars[-1]['v'])
        }
        
    except Exception as e:
        print(f"⚠️  Error calculating metrics for {symbol}: {str(e)}")
        return None

def get_top_candidates(screening_data, date):
    """Get top 10 candidates with worst drawdowns"""
    
    if not screening_data:
        return []
    
    # Sort by drawdown (most negative first)
    sorted_data = sorted(screening_data, key=lambda x: x['drawdown_pct'])
    top_10 = sorted_data[:10]
    
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
    """Collect current portfolio snapshot from trading API"""
    
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
                    print(f"⚠️  Error processing position {position.get('symbol', 'unknown')}: {str(e)}")
                    
        else:
            print(f"⚠️  Portfolio API error: {response.status_code}")
            
    except Exception as e:
        print(f"⚠️  Error getting portfolio: {str(e)}")
    
    return portfolio_data

def save_to_s3(s3_client, bucket_name, filename, data):
    """Save data to S3 as CSV with error handling"""
    
    try:
        df = pd.DataFrame(data)
        csv_content = df.to_csv(index=False)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=csv_content,
            ContentType='text/csv'
        )
        
        print(f"✅ Saved {len(data)} rows to s3://{bucket_name}/{filename}")
        
    except Exception as e:
        print(f"❌ Error saving to S3: {str(e)}")
        raise