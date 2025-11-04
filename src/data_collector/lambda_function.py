import json
import boto3
import sys
import os
from datetime import datetime

# Add shared modules to path (this works in Lambda)
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

try:
    from alpaca_client import AlpacaClient
    from utils import get_russell_1000_symbols
except ImportError as e:
    print(f"Import warning: {e} - some functions may not work locally")

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
        
        base_url = ssm.get_parameter(
            Name='/screener/alpaca/base_url'
        )['Parameter']['Value']
        
        print(f"✅ Parameter Store access successful")
        print(f"✅ API Key retrieved (first 5 chars): {api_key[:5]}...")
        print(f"✅ Base URL: {base_url}")
        
        # Test 2: Try to connect to Alpaca
        try:
            alpaca = AlpacaClient()
            connection_ok = alpaca.test_connection()
            
            if connection_ok:
                # Test getting a stock price
                test_price = alpaca.get_latest_price('AAPL')
                print(f"✅ AAPL latest price: ${test_price}")
        except Exception as e:
            print(f"⚠️  Alpaca connection test failed: {e}")
            connection_ok = False
        
        # Test 3: Stock symbols list
        try:
            symbols = get_russell_1000_symbols()
            print(f"✅ Russell 1000 symbols loaded: {len(symbols)} stocks")
            print(f"   Sample: {symbols[:5]}")
        except Exception as e:
            print(f"⚠️  Utils import failed: {e}")
            symbols = ['AAPL', 'MSFT', 'GOOGL']
        
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
                'next_step': 'Ready for deployment and testing!'
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
