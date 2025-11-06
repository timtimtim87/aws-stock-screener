import json
import boto3
import pandas as pd
import os
import requests
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):
    """
    Russell 1000 Telegram Bot:
    Responds to commands with latest data from S3
    - /dashboard - Complete daily overview (NEW!)
    - /screen - Top 10 drawdown candidates
    - /portfolio - Current positions  
    - /monitor - Check profit targets
    - /account - Account summary
    - /trigger - Manually trigger data collection
    - /stats - System statistics
    """
    
    print("🤖 Russell 1000 Telegram bot webhook received")
    
    try:
        # Parse Telegram webhook
        body = json.loads(event.get('body', '{}'))
        
        if 'message' not in body:
            print("No message in webhook body")
            return {'statusCode': 200}
        
        message = body['message']
        chat_id = message['chat']['id']
        text = message.get('text', '').strip()
        user_name = message.get('from', {}).get('first_name', 'User')
        
        print(f"Message from {user_name} (chat {chat_id}): {text}")
        
        # Initialize clients
        ssm = boto3.client('ssm')
        s3 = boto3.client('s3')
        lambda_client = boto3.client('lambda')
        
        # Get Telegram bot token
        bot_token = ssm.get_parameter(
            Name='/screener/telegram/bot_token',
            WithDecryption=True
        )['Parameter']['Value']
        
        # Verify authorized user (optional security)
        try:
            authorized_chat_id = ssm.get_parameter(
                Name='/screener/telegram/chat_id'
            )['Parameter']['Value']
            
            if str(chat_id) != str(authorized_chat_id):
                response = f"🚫 Unauthorized access. Contact admin."
                send_telegram_message(bot_token, chat_id, response)
                return {'statusCode': 200}
        except:
            # If no authorized chat ID set, allow any user
            pass
        
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        
        # Route commands
        if text.startswith('/'):
            command = text.split()[0].lower()
            
            if command == '/start' or command == '/help':
                response = get_help_message(user_name)
            elif command == '/dashboard' or command == '/daily':
                response = get_daily_dashboard(s3, bucket_name)
            elif command == '/screen':
                response = get_screening_results(s3, bucket_name)
            elif command == '/portfolio':
                response = get_portfolio_summary(s3, bucket_name)
            elif command == '/monitor':
                response = check_profit_targets(s3, bucket_name)
            elif command == '/account':
                response = get_account_summary()
            elif command == '/trigger':
                response = trigger_data_collection(lambda_client)
            elif command == '/stats':
                response = get_system_stats(s3, bucket_name)
            elif command == '/download':
                response = get_download_links(s3, bucket_name)
            else:
                response = f"❓ Unknown command '{command}'. Type /help for available commands."
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
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_daily_dashboard(s3_client, bucket_name):
    """
    NEW: Comprehensive daily dashboard with all key information
    """
    
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M UTC')
        
        # Start building the dashboard
        dashboard = f"📊 **DAILY DASHBOARD** ({current_time})\n"
        dashboard += "=" * 40 + "\n\n"
        
        # 1. ACCOUNT SUMMARY
        account_info = get_account_summary_data()
        if account_info:
            dashboard += f"💰 **ACCOUNT STATUS**\n"
            dashboard += f"*Equity:* ${account_info['equity']:,.2f}\n"
            dashboard += f"*Cash:* ${account_info['cash']:,.2f}\n"
            dashboard += f"*Buying Power:* ${account_info['buying_power']:,.2f}\n"
            dashboard += f"*Status:* {account_info['status']}\n\n"
        else:
            dashboard += f"💰 **ACCOUNT STATUS**\n❌ Unable to fetch account data\n\n"
        
        # 2. PORTFOLIO OVERVIEW
        portfolio_summary = get_portfolio_summary_data(s3_client, bucket_name)
        if portfolio_summary and portfolio_summary['positions']:
            dashboard += f"💼 **PORTFOLIO OVERVIEW**\n"
            dashboard += f"*Positions:* {portfolio_summary['position_count']}\n"
            dashboard += f"*Total Value:* ${portfolio_summary['total_value']:,.2f}\n"
            dashboard += f"*Unrealized P&L:* ${portfolio_summary['total_unrealized']:+,.2f}\n"
            dashboard += f"*Avg Return:* {portfolio_summary['avg_return']:+.1f}%\n\n"
            
            # 3. TOP 5 POSITIONS PERFORMANCE
            top_5 = portfolio_summary['positions'].head(5)
            dashboard += f"🏆 **TOP 5 POSITIONS**\n"
            for _, pos in top_5.iterrows():
                symbol = pos['symbol']
                return_pct = pos['unrealized_return_pct']
                value = pos['market_value']
                emoji = "🟢" if return_pct > 0 else "🔴"
                dashboard += f"{emoji} *{symbol}*: {return_pct:+.1f}% (${value:,.0f})\n"
            dashboard += "\n"
            
            # 4. PROFIT TARGET CHECK
            if len(portfolio_summary['positions']) >= 5:
                top_5_avg = top_5['unrealized_return_pct'].mean()
                dashboard += f"🎯 **PROFIT TARGET STATUS**\n"
                dashboard += f"*Top 5 Avg Return:* {top_5_avg:.1f}%\n"
                dashboard += f"*Target:* 100.0%\n"
                
                if top_5_avg >= 100.0:
                    dashboard += f"🚨 **TAKE PROFIT SIGNAL!** 🚨\n"
                    dashboard += f"*Profit to realize:* ${top_5['unrealized_pl'].sum():+,.0f}\n\n"
                else:
                    remaining = 100.0 - top_5_avg
                    dashboard += f"⏳ *Need {remaining:.1f}% more*\n\n"
            else:
                dashboard += f"🎯 **PROFIT TARGET STATUS**\n"
                dashboard += f"*Need 5+ positions for target analysis*\n\n"
            
            # 5. ALL POSITIONS DETAIL
            dashboard += f"📋 **ALL POSITIONS**\n"
            sorted_positions = portfolio_summary['positions'].sort_values('unrealized_return_pct', ascending=False)
            for _, pos in sorted_positions.iterrows():
                symbol = pos['symbol']
                return_pct = pos['unrealized_return_pct']
                current = pos['current_price']
                entry = pos['avg_entry_price']
                pl = pos['unrealized_pl']
                emoji = "🟢" if return_pct > 0 else "🔴"
                dashboard += f"{emoji} *{symbol}*: {return_pct:+.1f}% (${entry:.2f}→${current:.2f}) ${pl:+.0f}\n"
            dashboard += "\n"
            
        else:
            dashboard += f"💼 **PORTFOLIO OVERVIEW**\n"
            dashboard += f"*No current positions*\n\n"
        
        # 6. TOP 10 SCREENING CANDIDATES
        screening_data = get_screening_results_data(s3_client, bucket_name)
        if screening_data and not screening_data.empty:
            latest_date = screening_data['date'].max()
            latest_candidates = screening_data[screening_data['date'] == latest_date].head(10)
            
            dashboard += f"📉 **TOP 10 BUY CANDIDATES** ({latest_date})\n"
            dashboard += f"*Worst Russell 1000 drawdowns:*\n"
            
            for _, row in latest_candidates.iterrows():
                rank = int(row.get('rank', 0)) if row.get('rank', 0) else "•"
                symbol = row['symbol']
                drawdown = row['drawdown_pct']
                current = row.get('current_price', 0)
                peak = row.get('peak_price', 0)
                days = int(row.get('days_since_peak', 0))
                
                dashboard += f"*{rank}. {symbol}*: {drawdown:.1f}%"
                if current > 0 and peak > 0:
                    dashboard += f" (${peak:.2f}→${current:.2f}, {days}d)\n"
                else:
                    dashboard += f" ({days} days from peak)\n"
                    
        else:
            dashboard += f"📉 **TOP 10 BUY CANDIDATES**\n"
            dashboard += f"*No screening data available*\n"
        
        dashboard += f"\n💡 *Use individual commands (/portfolio, /screen, etc.) for more details*"
        
        return dashboard
        
    except Exception as e:
        return f"❌ Error generating dashboard: {str(e)}"

def get_account_summary_data():
    """Get account data and return as dict"""
    try:
        ssm = boto3.client('ssm')
        
        api_key = ssm.get_parameter(Name='/screener/alpaca/api_key', WithDecryption=True)['Parameter']['Value']
        secret_key = ssm.get_parameter(Name='/screener/alpaca/secret_key', WithDecryption=True)['Parameter']['Value']
        base_url = ssm.get_parameter(Name='/screener/alpaca/base_url')['Parameter']['Value']
        
        headers = {
            'APCA-API-KEY-ID': api_key,
            'APCA-API-SECRET-KEY': secret_key
        }
        
        response = requests.get(f"{base_url}/v2/account", headers=headers, timeout=10)
        
        if response.status_code == 200:
            account = response.json()
            return {
                'equity': float(account['equity']),
                'cash': float(account['cash']),
                'buying_power': float(account['buying_power']),
                'status': account['status']
            }
        return None
    except:
        return None

def get_portfolio_summary_data(s3_client, bucket_name):
    """Get portfolio data and return processed summary"""
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key='portfolio_snapshots.csv')
        portfolio_df = pd.read_csv(obj['Body'])
        
        if portfolio_df.empty:
            return None
        
        latest_date = portfolio_df['date'].max()
        current_positions = portfolio_df[portfolio_df['date'] == latest_date]
        
        if current_positions.empty:
            return None
        
        total_value = current_positions['market_value'].sum()
        total_unrealized = current_positions['unrealized_pl'].sum()
        position_count = len(current_positions)
        avg_return = current_positions['unrealized_return_pct'].mean()
        
        return {
            'positions': current_positions,
            'total_value': total_value,
            'total_unrealized': total_unrealized,
            'position_count': position_count,
            'avg_return': avg_return
        }
    except:
        return None

def get_screening_results_data(s3_client, bucket_name):
    """Get screening data and return DataFrame"""
    try:
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key='daily_top_candidates.csv')
            return pd.read_csv(obj['Body'])
        except:
            obj = s3_client.get_object(Bucket=bucket_name, Key='russell_1000_drawdown_results.csv')
            full_df = pd.read_csv(obj['Body'])
            latest_date = full_df['date'].max()
            candidates_df = full_df[full_df['date'] == latest_date].head(10).copy()
            candidates_df['rank'] = range(1, len(candidates_df) + 1)
            return candidates_df
    except:
        return pd.DataFrame()

def get_help_message(user_name):
    """Return help message with available commands"""
    
    return f"""🤖 **Hi {user_name}! Russell 1000 Screener Bot**

📊 **Available Commands:**

*🎯 Quick Access:*
/dashboard - Complete daily overview (account + portfolio + top stocks)
/daily - Same as /dashboard

*📈 Market Analysis:*
/screen - Top 10 worst drawdown stocks (buy candidates)
/stats - System performance & data statistics

*💼 Portfolio Management:*
/portfolio - Your current positions
/monitor - Check profit-taking opportunities (100% target)
/account - Alpaca account summary

*⚙️ System Control:*
/trigger - Manually run data collection
/download - Get CSV download links

*ℹ️ Info:*
/help - Show this menu

**📋 Strategy:** Contrarian value investing - buy Russell 1000 stocks with worst 180-day drawdowns, sell when top 5 average ≥100% gains.

**⏰ Data:** Updated daily at 6 AM ET using Polygon.io feed.
**💰 Trading:** Alpaca Markets integration for portfolio tracking.

*Happy investing! 📈✨*"""

def get_screening_results(s3_client, bucket_name):
    """Get latest top 10 screening results from S3"""
    
    try:
        # Try to read from your data collection output
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key='daily_top_candidates.csv')
            candidates_df = pd.read_csv(obj['Body'])
        except:
            # Fallback to other possible file names
            try:
                obj = s3_client.get_object(Bucket=bucket_name, Key='russell_1000_drawdown_results.csv')
                full_df = pd.read_csv(obj['Body'])
                # Get latest date and top 10
                latest_date = full_df['date'].max()
                candidates_df = full_df[full_df['date'] == latest_date].head(10).copy()
                candidates_df['rank'] = range(1, len(candidates_df) + 1)
            except:
                return "📊 No screening data available yet. Try /trigger to collect data or check back after 6 AM ET."
        
        if candidates_df.empty:
            return "📊 No screening data available yet. Try /trigger to collect data."
        
        # Get latest date
        latest_date = candidates_df['date'].max()
        latest_candidates = candidates_df[candidates_df['date'] == latest_date]
        
        if latest_candidates.empty:
            return "📊 No candidates found for latest date."
        
        # Format message
        message = f"📉 **TOP 10 WORST DRAWDOWNS** ({latest_date})\n\n"
        message += "_Contrarian value opportunities from Russell 1000:_\n\n"
        
        for _, row in latest_candidates.head(10).iterrows():
            rank = int(row.get('rank', 0))
            symbol = row['symbol']
            drawdown = row['drawdown_pct']
            current = row.get('current_price', 0)
            peak = row.get('peak_price', 0)
            days = int(row.get('days_since_peak', 0))
            
            message += f"*{rank}. {symbol}*: {drawdown:.1f}%\n"
            if current > 0 and peak > 0:
                message += f"   ${peak:.2f} → ${current:.2f} ({days} days ago)\n\n"
            else:
                message += f"   {drawdown:.1f}% from peak ({days} days ago)\n\n"
        
        message += "_💡 These are the most beaten-down Russell 1000 stocks - potential contrarian plays._"
        
        return message
        
    except Exception as e:
        return f"❌ Error getting screening results: {str(e)}"

def get_portfolio_summary(s3_client, bucket_name):
    """Get current portfolio summary from S3"""
    
    try:
        # Read latest portfolio snapshot
        obj = s3_client.get_object(Bucket=bucket_name, Key='portfolio_snapshots.csv')
        portfolio_df = pd.read_csv(obj['Body'])
        
        if portfolio_df.empty:
            return "💼 **PORTFOLIO SUMMARY**\n\nNo current positions."
        
        # Get latest date
        latest_date = portfolio_df['date'].max()
        current_positions = portfolio_df[portfolio_df['date'] == latest_date]
        
        if current_positions.empty:
            return "💼 **PORTFOLIO SUMMARY**\n\nNo current positions."
        
        # Calculate totals
        total_value = current_positions['market_value'].sum()
        total_unrealized = current_positions['unrealized_pl'].sum()
        position_count = len(current_positions)
        avg_return = current_positions['unrealized_return_pct'].mean()
        
        # Format message
        message = f"💼 **PORTFOLIO SUMMARY** ({latest_date})\n\n"
        message += f"*Positions:* {position_count}\n"
        message += f"*Total Value:* ${total_value:,.2f}\n"
        message += f"*Unrealized P&L:* ${total_unrealized:+,.2f}\n"
        message += f"*Average Return:* {avg_return:+.1f}%\n\n"
        
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
        return f"❌ Error getting portfolio summary: {str(e)}"

def check_profit_targets(s3_client, bucket_name):
    """Check if profit-taking conditions are met"""
    
    try:
        # Read latest portfolio snapshot
        obj = s3_client.get_object(Bucket=bucket_name, Key='portfolio_snapshots.csv')
        portfolio_df = pd.read_csv(obj['Body'])
        
        if portfolio_df.empty:
            return "🎯 **PROFIT TARGET CHECK**\n\nNo portfolio data available."
        
        # Get latest positions
        latest_date = portfolio_df['date'].max()
        current_positions = portfolio_df[portfolio_df['date'] == latest_date]
        
        if len(current_positions) == 0:
            return f"🎯 **PROFIT TARGET CHECK**\n\nNo current positions to monitor."
        elif len(current_positions) < 5:
            avg_return = current_positions['unrealized_return_pct'].mean()
            message = f"🎯 **PROFIT TARGET CHECK** ({latest_date})\n\n"
            message += f"*Current Positions:* {len(current_positions)} (need 5+ for top-5 analysis)\n"
            message += f"*Average Return:* {avg_return:.1f}%\n\n"
            message += "📊 *All Positions:*\n"
            sorted_positions = current_positions.sort_values('unrealized_return_pct', ascending=False)
            for _, pos in sorted_positions.iterrows():
                symbol = pos['symbol']
                return_pct = pos['unrealized_return_pct']
                emoji = "🟢" if return_pct > 0 else "🔴"
                message += f"{emoji} *{symbol}*: {return_pct:+.1f}%\n"
            return message
        
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
            message += "*🎊 Exit these winners:*\n"
            
            for _, pos in top_5.iterrows():
                symbol = pos['symbol']
                return_pct = pos['unrealized_return_pct']
                value = pos['market_value']
                
                message += f"• *{symbol}*: {return_pct:.1f}% (${value:,.0f})\n"
                
            message += f"\n💰 *Total profit to realize:* ${top_5['unrealized_pl'].sum():+,.0f}"
        else:
            remaining = 100.0 - avg_return
            message += f"⏳ Hold positions. Need {remaining:.1f}% more on average.\n\n"
            
            message += "📊 *Current Top 5:*\n"
            for _, pos in top_5.iterrows():
                symbol = pos['symbol']
                return_pct = pos['unrealized_return_pct']
                emoji = "🟢" if return_pct > 0 else "🔴"
                
                message += f"{emoji} *{symbol}*: {return_pct:+.1f}%\n"
        
        return message
        
    except Exception as e:
        return f"❌ Error checking profit targets: {str(e)}"

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
        
        response = requests.get(f"{base_url}/v2/account", headers=headers, timeout=10)
        
        if response.status_code == 200:
            account = response.json()
            
            equity = float(account['equity'])
            buying_power = float(account['buying_power'])
            cash = float(account['cash'])
            
            message = f"💰 **ALPACA ACCOUNT** ({account['status']})\n\n"
            message += f"*Total Equity:* ${equity:,.2f}\n"
            message += f"*Cash:* ${cash:,.2f}\n"
            message += f"*Buying Power:* ${buying_power:,.2f}\n"
            
            # Add day trading info if relevant
            if account.get('pattern_day_trader'):
                message += f"*Day Trading:* Pattern Day Trader\n"
            if account.get('daytrade_count', 0) > 0:
                message += f"*Day Trades Used:* {account['daytrade_count']}/3\n"
            
            return message
        else:
            return f"❌ Alpaca API error: {response.status_code}"
            
    except Exception as e:
        return f"❌ Error getting account summary: {str(e)}"

def trigger_data_collection(lambda_client):
    """Manually trigger data collection function"""
    
    try:
        # Find your data collection function name
        function_name = None
        
        # Try common naming patterns
        possible_names = [
            'stock-screener-test-DailyDataCollectorFunction-*',
            'stock-screener-DataCollectorFunction-*',
            '*DataCollectorFunction*'
        ]
        
        # Get function list
        functions = lambda_client.list_functions()['Functions']
        for func in functions:
            name = func['FunctionName']
            if 'DataCollector' in name or 'daily_collector' in name.lower():
                function_name = name
                break
        
        if not function_name:
            return "❌ Could not find data collection function. Check function naming."
        
        print(f"Triggering function: {function_name}")
        
        # Invoke function asynchronously
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Asynchronous
            Payload=json.dumps({})
        )
        
        if response['StatusCode'] == 202:
            return f"✅ **DATA COLLECTION TRIGGERED**\n\nFunction `{function_name}` started.\n\nCheck back in 2-3 minutes for updated data. 📊"
        else:
            return f"❌ Error triggering collection: Status {response['StatusCode']}"
            
    except Exception as e:
        return f"❌ Error triggering data collection: {str(e)}"

def get_system_stats(s3_client, bucket_name):
    """Get system statistics and data health"""
    
    try:
        stats = []
        
        # Check available files
        files_to_check = [
            'russell_1000_drawdown_results.csv',
            'daily_top_candidates.csv', 
            'portfolio_snapshots.csv'
        ]
        
        message = f"📊 **SYSTEM STATISTICS** ({datetime.now().strftime('%Y-%m-%d %H:%M')} UTC)\n\n"
        
        for file in files_to_check:
            try:
                obj = s3_client.get_object(Bucket=bucket_name, Key=file)
                df = pd.read_csv(obj['Body'])
                
                if not df.empty:
                    latest_date = df['date'].max() if 'date' in df.columns else 'Unknown'
                    rows = len(df)
                    message += f"✅ *{file.replace('.csv', '').replace('_', ' ').title()}*\n"
                    message += f"   📅 Latest: {latest_date}\n"
                    message += f"   📝 Records: {rows:,}\n\n"
                else:
                    message += f"⚠️ *{file}*: Empty file\n\n"
                    
            except s3_client.exceptions.NoSuchKey:
                message += f"❌ *{file}*: Not found\n\n"
            except Exception as e:
                message += f"❌ *{file}*: Error ({str(e)[:30]}...)\n\n"
        
        # System health
        message += "*🔧 System Health:*\n"
        message += f"📦 S3 Bucket: `{bucket_name}`\n"
        message += f"⚡ Last Check: {datetime.now().strftime('%H:%M:%S')} UTC\n"
        
        return message
        
    except Exception as e:
        return f"❌ Error getting system stats: {str(e)}"

def get_download_links(s3_client, bucket_name):
    """Generate presigned URLs for CSV downloads"""
    
    try:
        csv_files = [
            'russell_1000_drawdown_results.csv',
            'daily_top_candidates.csv',
            'portfolio_snapshots.csv'
        ]
        
        message = "📥 **CSV DOWNLOAD LINKS** (Valid for 1 hour)\n\n"
        
        for file in csv_files:
            try:
                # Check if file exists first
                s3_client.head_object(Bucket=bucket_name, Key=file)
                
                url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': file},
                    ExpiresIn=3600
                )
                
                file_name = file.replace('.csv', '').replace('_', ' ').title()
                message += f"📄 [{file_name}]({url})\n"
                
            except s3_client.exceptions.NoSuchKey:
                file_name = file.replace('.csv', '').replace('_', ' ').title()
                message += f"❌ {file_name}: Not available\n"
            except Exception as e:
                message += f"❌ {file}: Error generating link\n"
        
        message += "\n💡 Right-click links → 'Save Link As' to download CSV files."
        
        return message
        
    except Exception as e:
        return f"❌ Error generating download links: {str(e)}"

def send_telegram_message(bot_token, chat_id, message):
    """Send message to Telegram"""
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # Split long messages
    max_length = 4000  # Telegram limit is 4096, leave some buffer
    
    if len(message) <= max_length:
        messages = [message]
    else:
        # Split by paragraphs first
        parts = message.split('\n\n')
        messages = []
        current = ""
        
        for part in parts:
            if len(current) + len(part) + 2 <= max_length:
                current += part + "\n\n"
            else:
                if current:
                    messages.append(current.strip())
                current = part + "\n\n"
        
        if current:
            messages.append(current.strip())
    
    for msg in messages:
        payload = {
            'chat_id': chat_id,
            'text': msg,
            'parse_mode': 'Markdown',
            'disable_web_page_preview': True
        }
        
        try:
            response = requests.post(url, data=payload, timeout=10)
            
            if response.status_code == 200:
                print(f"✅ Message sent successfully to chat {chat_id}")
            else:
                print(f"❌ Error sending message: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"❌ Exception sending Telegram message: {str(e)}")