import json
import boto3
import pandas as pd
import os
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):
    """
    Telegram Bot Lambda:
    Responds to Telegram commands by reading latest data from S3
    - /screen - Top 10 drawdown candidates
    - /portfolio - Current positions
    - /monitor - Check profit targets
    - /account - Account summary
    """
    
    print("🤖 Telegram bot webhook received")
    
    try:
        # Parse Telegram webhook
        body = json.loads(event.get('body', '{}'))
        
        if 'message' not in body:
            return {'statusCode': 200}
        
        message = body['message']
        chat_id = message['chat']['id']
        text = message.get('text', '').strip()
        
        print(f"Message from chat {chat_id}: {text}")
        
        # Initialize clients
        ssm = boto3.client('ssm')
        s3 = boto3.client('s3')
        
        # Get Telegram bot token
        bot_token = ssm.get_parameter(
            Name='/screener/telegram/bot_token',
            WithDecryption=True
        )['Parameter']['Value']
        
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        
        # Route commands
        if text.startswith('/'):
            command = text.split()[0].lower()
            
            if command == '/start' or command == '/help':
                response = get_help_message()
            elif command == '/screen':
                response = get_screening_results(s3, bucket_name)
            elif command == '/portfolio':
                response = get_portfolio_summary(s3, bucket_name)
            elif command == '/monitor':
                response = check_profit_targets(s3, bucket_name)
            elif command == '/account':
                response = get_account_summary()
            else:
                response = "Unknown command. Type /help for available commands."
        else:
            response = "Please use commands starting with /. Type /help for available commands."
        
        # Send response to Telegram
        send_telegram_message(bot_token, chat_id, response)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Success'})
        }
        
    except Exception as e:
        print(f"❌ Error in Telegram bot: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_help_message():
    """Return help message with available commands"""
    
    return """🤖 **Russell 1000 Stock Screener Bot**

📊 **Available Commands:**

*Data Commands:*
/screen - Top 10 worst drawdown stocks
/portfolio - Your current positions  
/monitor - Check profit-taking opportunities
/account - Account summary

*Info:*
/help - Show this help message

**Strategy:** Buy beaten-down Russell 1000 stocks (worst drawdowns), sell when top 5 average ≥100% gains.

Data updated daily at 6 AM ET. All prices are split-adjusted using IEX feed."""

def get_screening_results(s3_client, bucket_name):
    """Get latest top 10 screening results from S3"""
    
    try:
        # Read latest candidates from CSV
        obj = s3_client.get_object(Bucket=bucket_name, Key='data/top_candidates.csv')
        candidates_df = pd.read_csv(obj['Body'])
        
        if candidates_df.empty:
            return "No screening data available yet. Check back after 6 AM ET daily update."
        
        # Get latest date
        latest_date = candidates_df['date'].max()
        latest_candidates = candidates_df[candidates_df['date'] == latest_date]
        
        if latest_candidates.empty:
            return "No candidates found for latest date."
        
        # Format message
        message = f"📉 **TOP 10 WORST DRAWDOWNS** ({latest_date})\n\n"
        message += "_Contrarian value opportunities from Russell 1000:_\n\n"
        
        for _, row in latest_candidates.iterrows():
            rank = int(row['rank'])
            symbol = row['symbol']
            drawdown = row['drawdown_pct']
            current = row['current_price']
            peak = row['peak_price']
            days = int(row['days_since_peak'])
            
            message += f"*{rank}. {symbol}*: {drawdown:.1f}%\n"
            message += f"   ${peak:.2f} → ${current:.2f} ({days} days ago)\n\n"
        
        message += "_💡 These are the most beaten-down Russell 1000 stocks - potential contrarian plays._"
        
        return message
        
    except Exception as e:
        return f"Error getting screening results: {str(e)}"

def get_portfolio_summary(s3_client, bucket_name):
    """Get current portfolio summary from S3"""
    
    try:
        # Read latest portfolio snapshot
        obj = s3_client.get_object(Bucket=bucket_name, Key='data/portfolio_snapshots.csv')
        portfolio_df = pd.read_csv(obj['Body'])
        
        if portfolio_df.empty:
            return "📊 **PORTFOLIO SUMMARY**\n\nNo current positions."
        
        # Get latest date
        latest_date = portfolio_df['date'].max()
        current_positions = portfolio_df[portfolio_df['date'] == latest_date]
        
        if current_positions.empty:
            return "📊 **PORTFOLIO SUMMARY**\n\nNo current positions."
        
        # Calculate totals
        total_value = current_positions['market_value'].sum()
        total_unrealized = current_positions['unrealized_pl'].sum()
        position_count = len(current_positions)
        
        # Format message
        message = f"📊 **PORTFOLIO SUMMARY** ({latest_date})\n\n"
        message += f"*Positions:* {position_count}\n"
        message += f"*Total Value:* ${total_value:,.2f}\n"
        message += f"*Unrealized P&L:* ${total_unrealized:,.2f}\n\n"
        
        message += "*Current Positions:*\n"
        
        # Sort by unrealized return (best first)
        sorted_positions = current_positions.sort_values('unrealized_return_pct', ascending=False)
        
        for _, pos in sorted_positions.iterrows():
            symbol = pos['symbol']
            return_pct = pos['unrealized_return_pct']
            value = pos['market_value']
            pl = pos['unrealized_pl']
            
            emoji = "🟢" if return_pct > 0 else "🔴"
            message += f"{emoji} *{symbol}*: {return_pct:+.1f}% (${value:,.0f}, ${pl:+.0f})\n"
        
        return message
        
    except Exception as e:
        return f"Error getting portfolio summary: {str(e)}"

def check_profit_targets(s3_client, bucket_name):
    """Check if profit-taking conditions are met"""
    
    try:
        # Read latest portfolio snapshot
        obj = s3_client.get_object(Bucket=bucket_name, Key='data/portfolio_snapshots.csv')
        portfolio_df = pd.read_csv(obj['Body'])
        
        if portfolio_df.empty:
            return "No portfolio data available to check."
        
        # Get latest positions
        latest_date = portfolio_df['date'].max()
        current_positions = portfolio_df[portfolio_df['date'] == latest_date]
        
        if len(current_positions) < 5:
            return f"🎯 **PROFIT TARGET CHECK**\n\nOnly {len(current_positions)} positions. Need at least 5 to check profit targets."
        
        # Sort by performance (best first)
        sorted_positions = current_positions.sort_values('unrealized_return_pct', ascending=False)
        top_5 = sorted_positions.head(5)
        
        # Calculate average return of top 5
        avg_return = top_5['unrealized_return_pct'].mean()
        
        message = f"🎯 **PROFIT TARGET CHECK** ({latest_date})\n\n"
        message += f"*Top 5 Average Return:* {avg_return:.1f}%\n"
        message += f"*Target:* 100.0%\n\n"
        
        if avg_return >= 100.0:
            message += "🚨 **TAKE PROFIT SIGNAL!** 🚨\n\n"
            message += "*Exit these positions:*\n"
            
            for _, pos in top_5.iterrows():
                symbol = pos['symbol']
                return_pct = pos['unrealized_return_pct']
                value = pos['market_value']
                
                message += f"• *{symbol}*: {return_pct:.1f}% (${value:,.0f})\n"
        else:
            remaining = 100.0 - avg_return
            message += f"✋ Hold positions. Need {remaining:.1f}% more on average.\n\n"
            
            message += "*Current Top 5:*\n"
            for _, pos in top_5.iterrows():
                symbol = pos['symbol']
                return_pct = pos['unrealized_return_pct']
                
                message += f"• *{symbol}*: {return_pct:.1f}%\n"
        
        return message
        
    except Exception as e:
        return f"Error checking profit targets: {str(e)}"

def get_account_summary():
    """Get account summary from Alpaca"""
    
    try:
        ssm = boto3.client('ssm')
        
        # Get credentials
        api_key = ssm.get_parameter(Name='/screener/alpaca/api_key', WithDecryption=True)['Parameter']['Value']
        secret_key = ssm.get_parameter(Name='/screener/alpaca/secret_key', WithDecryption=True)['Parameter']['Value']
        base_url = ssm.get_parameter(Name='/screener/alpaca/base_url')['Parameter']['Value']
        
        headers = {
            'APCA-API-KEY-ID': api_key,
            'APCA-API-SECRET-KEY': secret_key
        }
        
        import requests
        response = requests.get(f"{base_url}/v2/account", headers=headers, timeout=10)
        
        if response.status_code == 200:
            account = response.json()
            
            equity = float(account['equity'])
            buying_power = float(account['buying_power'])
            
            message = f"💰 **ACCOUNT SUMMARY**\n\n"
            message += f"*Total Equity:* ${equity:,.2f}\n"
            message += f"*Buying Power:* ${buying_power:,.2f}\n"
            message += f"*Account Status:* {account['status']}\n"
            
            return message
        else:
            return f"Error getting account info: {response.status_code}"
            
    except Exception as e:
        return f"Error getting account summary: {str(e)}"

def send_telegram_message(bot_token, chat_id, message):
    """Send message to Telegram"""
    
    import requests
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    
    try:
        response = requests.post(url, data=payload, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ Message sent successfully to chat {chat_id}")
        else:
            print(f"❌ Error sending message: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Exception sending Telegram message: {str(e)}")