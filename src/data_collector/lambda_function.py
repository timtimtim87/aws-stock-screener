import json
import boto3
import sys
import os
import requests
from datetime import datetime

# Add shared modules to path (this works in Lambda)
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

def lambda_handler(event, context):
    """
    Test function to verify AWS Parameter Store access and Alpaca API connection
    """
    
    print("🚀 Starting stock screener test...")
    
    try:
        # Test 1: Access Parameter Store
        ssm = boto3.client('ssm')
        
        # Get Alpaca credentials
        api_key = ssm.get_parameter(
            Name='/screener/alpaca/api_key',
            WithDecryption=True
        )['Parameter']['Value']
        
        secret_key = ssm.get_parameter(
            Name='/screener/alpaca/secret_key',
            WithDecryption=True
        )['Parameter']['Value']
        
        base_url = ssm.get_parameter(
            Name='/screener/alpaca/base_url'
        )['Parameter']['Value']
        
        print(f"✅ Parameter Store access successful")
        print(f"✅ API Key retrieved (first 5 chars): {api_key[:5]}...")
        print(f"✅ Base URL: {base_url}")
        
        # Test 2: Try to connect to Alpaca using direct HTTP
        try:
            headers = {
                'APCA-API-KEY-ID': api_key,
                'APCA-API-SECRET-KEY': secret_key
            }
            
            # Test account endpoint
            account_response = requests.get(f"{base_url}/v2/account", headers=headers, timeout=10)
            
            if account_response.status_code == 200:
                account_data = account_response.json()
                print(f"✅ Connected to Alpaca account: {account_data.get('status', 'Unknown')}")
                print(f"✅ Account equity: ${float(account_data.get('equity', 0)):,.2f}")
                connection_ok = True
                
                # Test getting a stock price
                try:
                    bars_response = requests.get(
                        f"{base_url}/v2/stocks/AAPL/bars",
                        headers=headers,
                        params={'timeframe': '1Day', 'limit': 1},
                        timeout=10
                    )
                    if bars_response.status_code == 200:
                        bars_data = bars_response.json()
                        if bars_data.get('bars'):
                            latest_price = bars_data['bars'][0]['c']  # close price
                            print(f"✅ AAPL latest price: ${latest_price}")
                        else:
                            print("⚠️  No AAPL price data available")
                    else:
                        print(f"⚠️  Could not fetch AAPL price: {bars_response.status_code}")
                except Exception as e:
                    print(f"⚠️  Price fetch error: {e}")
                    
            else:
                print(f"❌ Alpaca connection failed: {account_response.status_code}")
                connection_ok = False
                
        except Exception as e:
            print(f"⚠️  Alpaca connection test failed: {e}")
            connection_ok = False
        
        # Test 3: Stock symbols list
        try:
            # Import Russell 1000 symbols function
            from utils import get_russell_1000_symbols
            symbols = get_russell_1000_symbols()
            print(f"✅ Russell 1000 symbols loaded: {len(symbols)} stocks")
            print(f"   Sample: {symbols[:5]}")
        except Exception as e:
            print(f"⚠️  Utils import failed: {e}")
            # Fallback to hardcoded symbols
            symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
            print(f"✅ Using fallback symbols: {len(symbols)} stocks")
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Stock screener test successful!',
                'timestamp': datetime.now().isoformat(),
                'tests': {
                    'parameter_store': True,
                    'alpaca_connection': connection_ok,
                    'symbols_loaded': len(symbols) if 'symbols' in locals() else 0
                },
                'next_step': 'Ready for full deployment!'
            })
        }
        
        print("✅ All tests completed!")
        return response
        
    except Exception as e:
        print(f"❌ Error in test: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Test failed - check credentials and permissions'
            })
        }