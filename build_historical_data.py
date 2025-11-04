#!/usr/bin/env python3
"""
Russell 1000 Historical Data Builder
====================================

This script builds a complete historical dataset of Russell 1000 stocks using
your Alpaca paper trading account (IEX data feed). It will:

1. Fetch 180+ days of daily closing prices for all 991 Russell 1000 stocks
2. Calculate drawdown metrics for each stock
3. Save clean CSV files ready for S3 upload
4. Handle rate limits and errors gracefully
5. Show progress and data quality metrics

Usage:
    python build_historical_data.py

Requirements:
    pip install requests pandas python-dotenv
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

class RussellDataBuilder:
    def __init__(self):
        """Initialize the data builder with Alpaca credentials"""
        
        # Load environment variables
        load_dotenv()
        
        self.api_key = os.getenv('ALPACA_API_KEY')
        self.secret_key = os.getenv('ALPACA_SECRET_KEY')
        
        if not self.api_key or not self.secret_key:
            raise ValueError("Please set ALPACA_API_KEY and ALPACA_SECRET_KEY in .env file")
        
        self.headers = {
            'APCA-API-KEY-ID': self.api_key,
            'APCA-API-SECRET-KEY': self.secret_key
        }
        
        self.base_url = "https://data.alpaca.markets"
        self.data_dir = "data"
        
        # Create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        print("🚀 Russell 1000 Historical Data Builder")
        print("=" * 50)
        print(f"📁 Data directory: {os.path.abspath(self.data_dir)}")
    
    def get_russell_1000_symbols(self):
        """Return complete Russell 1000 symbols list (991 stocks)"""
        
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
        unique_symbols = sorted(list(set(symbols)))
        print(f"📊 Complete Russell 1000 list: {len(unique_symbols)} unique stocks")
        return unique_symbols
    
    def fetch_historical_data(self, symbols, days=200):
        """Fetch historical data for all symbols (IEX feed compatible)"""
        
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=days)
        
        print(f"📅 Fetching data from {start_date} to {end_date}")
        print(f"🔄 Processing {len(symbols)} symbols using IEX feed...")
        print(f"⏱️  Estimated runtime: {len(symbols) * 0.1 / 60:.1f} minutes")
        
        all_data = []
        failed_symbols = []
        
        for i, symbol in enumerate(symbols):
            # Progress indicator every 50 symbols
            if i > 0 and i % 50 == 0:
                elapsed = i * 0.1 / 60
                remaining = (len(symbols) - i) * 0.1 / 60
                print(f"   📊 Progress: {i}/{len(symbols)} ({i/len(symbols)*100:.1f}%) - "
                      f"Elapsed: {elapsed:.1f}m, Remaining: {remaining:.1f}m")
            
            try:
                # Fetch daily bars for this symbol using IEX feed
                response = requests.get(
                    f"{self.base_url}/v2/stocks/{symbol}/bars",
                    headers=self.headers,
                    params={
                        'timeframe': '1Day',
                        'start': start_date.isoformat(),
                        'end': end_date.isoformat(),
                        'adjustment': 'all',
                        'feed': 'iex',  # Use IEX feed for paper trading compatibility
                        'limit': 1000
                    },
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    bars = data.get('bars', [])
                    
                    if len(bars) >= 30:  # Minimum data requirement
                        # Process each bar
                        for bar in bars:
                            all_data.append({
                                'date': bar['t'][:10],  # Extract date part
                                'symbol': symbol,
                                'open': float(bar['o']),
                                'high': float(bar['h']),
                                'low': float(bar['l']),
                                'close': float(bar['c']),
                                'volume': int(bar['v'])
                            })
                    else:
                        if i < 10:  # Only show first 10 failures to avoid spam
                            print(f"   ⚠️  {symbol}: Insufficient data ({len(bars)} bars)")
                        failed_symbols.append(symbol)
                        
                elif response.status_code == 403:
                    if i < 10:  # Only show first 10 failures
                        print(f"   ❌ {symbol}: 403 Forbidden")
                    failed_symbols.append(symbol)
                    
                else:
                    if i < 10:  # Only show first 10 failures
                        print(f"   ⚠️  {symbol}: API error {response.status_code}")
                    failed_symbols.append(symbol)
                    
            except Exception as e:
                if i < 10:  # Only show first 10 failures
                    print(f"   ❌ {symbol}: Error - {str(e)}")
                failed_symbols.append(symbol)
            
            # Rate limiting (conservative with IEX)
            time.sleep(0.1)  # 100ms between requests
        
        print(f"\n✅ Data collection complete!")
        print(f"   📊 Successful: {len(symbols) - len(failed_symbols)} symbols")
        print(f"   ❌ Failed: {len(failed_symbols)} symbols")
        print(f"   📈 Total data points: {len(all_data):,}")
        
        if failed_symbols and len(failed_symbols) <= 20:
            print(f"   Failed symbols: {', '.join(failed_symbols)}")
        elif len(failed_symbols) > 20:
            print(f"   Failed symbols (first 20): {', '.join(failed_symbols[:20])}...")
        
        return pd.DataFrame(all_data), failed_symbols
    
    def calculate_drawdowns(self, df):
        """Calculate 180-day drawdown metrics for each symbol"""
        
        print(f"📈 Calculating drawdowns for {df['symbol'].nunique()} symbols...")
        
        results = []
        
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('date')
            
            if len(symbol_data) < 30:
                continue
            
            # Calculate rolling high (expanding window for peak detection)
            symbol_data['rolling_high'] = symbol_data['high'].expanding().max()
            
            # Calculate drawdown from peak
            symbol_data['drawdown_pct'] = (
                (symbol_data['close'] - symbol_data['rolling_high']) / 
                symbol_data['rolling_high'] * 100
            )
            
            # Get latest metrics
            latest = symbol_data.iloc[-1]
            
            # Find when the peak occurred
            peak_rows = symbol_data[symbol_data['rolling_high'] == latest['rolling_high']]
            peak_date = peak_rows['date'].iloc[0]  # First occurrence of this peak
            days_since_peak = (pd.to_datetime(latest['date']) - pd.to_datetime(peak_date)).days
            
            results.append({
                'symbol': symbol,
                'current_price': latest['close'],
                'peak_price': latest['rolling_high'],
                'drawdown_pct': latest['drawdown_pct'],
                'days_since_peak': days_since_peak,
                'data_points': len(symbol_data),
                'date_range_start': symbol_data['date'].min(),
                'date_range_end': symbol_data['date'].max(),
                'last_updated': datetime.now().isoformat()
            })
        
        return pd.DataFrame(results)
    
    def save_datasets(self, price_data, drawdown_data):
        """Save datasets to CSV files"""
        
        print(f"💾 Saving datasets...")
        
        # Save raw price data
        price_file = os.path.join(self.data_dir, 'russell_1000_daily_prices.csv')
        price_data.to_csv(price_file, index=False)
        print(f"   ✅ Raw price data: {price_file} ({len(price_data):,} rows)")
        
        # Save drawdown analysis
        drawdown_file = os.path.join(self.data_dir, 'russell_1000_drawdowns.csv')
        drawdown_data.to_csv(drawdown_file, index=False)
        print(f"   ✅ Drawdown analysis: {drawdown_file} ({len(drawdown_data)} rows)")
        
        # Save top candidates (worst drawdowns)
        top_candidates = drawdown_data.nsmallest(50, 'drawdown_pct')
        candidates_file = os.path.join(self.data_dir, 'top_drawdown_candidates.csv')
        top_candidates.to_csv(candidates_file, index=False)
        print(f"   ✅ Top candidates: {candidates_file} ({len(top_candidates)} rows)")
        
        # Save metadata
        metadata = {
            'created': datetime.now().isoformat(),
            'total_symbols': len(drawdown_data),
            'date_range': f"{drawdown_data['date_range_start'].min()} to {drawdown_data['date_range_end'].max()}",
            'avg_data_points': round(drawdown_data['data_points'].mean(), 1),
            'data_source': 'Alpaca IEX Feed'
        }
        
        metadata_file = os.path.join(self.data_dir, 'dataset_metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"   ✅ Metadata: {metadata_file}")
        
        return price_file, drawdown_file, candidates_file
    
    def show_summary(self, drawdown_data):
        """Show summary statistics"""
        
        print(f"\n📊 RUSSELL 1000 DATASET SUMMARY")
        print("=" * 60)
        
        print(f"🎯 Total stocks analyzed: {len(drawdown_data)}")
        print(f"📅 Average data points per stock: {drawdown_data['data_points'].mean():.1f}")
        print(f"📈 Date range: {drawdown_data['date_range_start'].min()} to {drawdown_data['date_range_end'].max()}")
        
        print(f"\n🔻 Drawdown Statistics:")
        print(f"   📉 Worst drawdown: {drawdown_data['drawdown_pct'].min():.1f}%")
        print(f"   📊 Average drawdown: {drawdown_data['drawdown_pct'].mean():.1f}%")
        print(f"   📈 Best performing: {drawdown_data['drawdown_pct'].max():.1f}%")
        print(f"   🕐 Average days since peak: {drawdown_data['days_since_peak'].mean():.0f} days")
        
        print(f"\n🏆 Top 10 Worst Drawdowns (Contrarian Candidates):")
        top_10 = drawdown_data.nsmallest(10, 'drawdown_pct')
        for i, (_, row) in enumerate(top_10.iterrows(), 1):
            print(f"   {i:2d}. {row['symbol']:6s}: {row['drawdown_pct']:6.1f}% "
                  f"(${row['current_price']:6.2f} from ${row['peak_price']:6.2f}, "
                  f"{row['days_since_peak']} days ago)")
        
        print(f"\n🚀 Best Performers (Recent Momentum):")
        top_performers = drawdown_data.nlargest(5, 'drawdown_pct')
        for i, (_, row) in enumerate(top_performers.iterrows(), 1):
            print(f"   {i}. {row['symbol']:6s}: {row['drawdown_pct']:+6.1f}% "
                  f"(${row['current_price']:6.2f} from ${row['peak_price']:6.2f})")
    
    def run(self):
        """Run the complete data building process"""
        
        try:
            print(f"⏰ Starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Get complete Russell 1000 symbol list
            symbols = self.get_russell_1000_symbols()
            
            # Fetch historical data
            price_data, failed_symbols = self.fetch_historical_data(symbols)
            
            if price_data.empty:
                print("❌ No data collected! Check your API credentials and IEX access.")
                return
            
            # Calculate drawdowns
            drawdown_data = self.calculate_drawdowns(price_data)
            
            # Save datasets
            files = self.save_datasets(price_data, drawdown_data)
            
            # Show summary
            self.show_summary(drawdown_data)
            
            print(f"\n🎉 SUCCESS! Russell 1000 historical dataset built successfully.")
            print(f"📁 Files saved in: {os.path.abspath(self.data_dir)}")
            print(f"⏰ Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            print(f"\n📋 Next steps:")
            print(f"   1. Review the CSV files for data quality")
            print(f"   2. Upload to S3: aws s3 cp {self.data_dir}/ s3://your-bucket/data/ --recursive")
            print(f"   3. Build your daily Lambda function")
            print(f"   4. Create Telegram bot for instant access")
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise

def main():
    """Main entry point"""
    
    # Check for .env file
    if not os.path.exists('.env'):
        print("❌ Please create a .env file with your Alpaca credentials:")
        print("   ALPACA_API_KEY=your_paper_api_key_here")
        print("   ALPACA_SECRET_KEY=your_paper_secret_key_here")
        return
    
    # Run the data builder
    builder = RussellDataBuilder()
    builder.run()

if __name__ == "__main__":
    main()