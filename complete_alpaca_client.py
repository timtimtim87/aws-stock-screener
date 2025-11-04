import boto3
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlpacaClient:
    def __init__(self):
        """Initialize Alpaca API client with credentials from Parameter Store"""
        
        self.ssm = boto3.client('ssm')
        self.api = None
        
        # Get API credentials from Parameter Store
        try:
            self.api_key = self.ssm.get_parameter(
                Name='/screener/alpaca/api_key',
                WithDecryption=True
            )['Parameter']['Value']
            
            self.secret_key = self.ssm.get_parameter(
                Name='/screener/alpaca/secret_key',
                WithDecryption=True
            )['Parameter']['Value']
            
            self.base_url = self.ssm.get_parameter(
                Name='/screener/alpaca/base_url'
            )['Parameter']['Value']
            
        except Exception as e:
            logger.error(f"Error getting Alpaca credentials: {e}")
            raise
        
        # Initialize API client
        try:
            self.api = tradeapi.REST(
                key_id=self.api_key,
                secret_key=self.secret_key,
                base_url=self.base_url,
                api_version='v2'
            )
            logger.info("Alpaca client initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Alpaca API client: {e}")
            raise
    
    def test_connection(self):
        """Test the API connection and return account info"""
        try:
            account = self.api.get_account()
            logger.info(f"✅ Connected to Alpaca account: {account.status}")
            logger.info(f"✅ Account equity: ${float(account.equity):,.2f}")
            logger.info(f"✅ Buying power: ${float(account.buying_power):,.2f}")
            return {
                'success': True,
                'account_status': account.status,
                'equity': float(account.equity),
                'buying_power': float(account.buying_power)
            }
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_historical_bars(self, symbol, start_date, end_date, timeframe='1Day'):
        """
        Get historical price bars for a symbol
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL')
            start_date (datetime): Start date for historical data
            end_date (datetime): End date for historical data
            timeframe (str): Bar timeframe ('1Day', '1Hour', etc.)
        
        Returns:
            list: List of bar objects with OHLCV data
        """
        try:
            logger.info(f"Fetching bars for {symbol} from {start_date.date()} to {end_date.date()}")
            
            bars_response = self.api.get_bars(
                symbol,
                timeframe,
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                adjustment='raw'
            )
            
            # Convert to list for easier processing
            bars = []
            for bar in bars_response:
                bars.append({
                    'timestamp': bar.timestamp,
                    'open': float(bar.open),
                    'high': float(bar.high),
                    'low': float(bar.low),
                    'close': float(bar.close),
                    'volume': int(bar.volume)
                })
            
            logger.info(f"Retrieved {len(bars)} bars for {symbol}")
            return bars
            
        except Exception as e:
            logger.error(f"Error getting bars for {symbol}: {str(e)}")
            return []
    
    def get_latest_price(self, symbol):
        """
        Get the latest price for a symbol
        
        Args:
            symbol (str): Stock symbol
            
        Returns:
            float: Latest close price, or None if error
        """
        try:
            # Try to get latest trade first
            try:
                latest_trade = self.api.get_latest_trade(symbol)
                price = float(latest_trade.price)
                logger.debug(f"Latest trade price for {symbol}: ${price:.2f}")
                return price
            except:
                # Fallback to latest bar close price
                bars = self.api.get_bars(symbol, '1Day', limit=1)
                latest_bar = list(bars)[-1]
                price = float(latest_bar.close)
                logger.debug(f"Latest bar close price for {symbol}: ${price:.2f}")
                return price
                
        except Exception as e:
            logger.error(f"Error getting latest price for {symbol}: {str(e)}")
            return None
    
    def get_multiple_latest_prices(self, symbols):
        """
        Get latest prices for multiple symbols efficiently
        
        Args:
            symbols (list): List of stock symbols
            
        Returns:
            dict: Dictionary with symbol as key and price as value
        """
        prices = {}
        
        # Process in batches to avoid API limits
        batch_size = 100
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"Processing price batch {i//batch_size + 1}: {len(batch)} symbols")
            
            for symbol in batch:
                try:
                    price = self.get_latest_price(symbol)
                    if price is not None:
                        prices[symbol] = price
                except Exception as e:
                    logger.warning(f"Failed to get price for {symbol}: {e}")
                    continue
        
        logger.info(f"Retrieved prices for {len(prices)} out of {len(symbols)} symbols")
        return prices
    
    def get_positions(self):
        """
        Get all current positions
        
        Returns:
            list: List of position dictionaries
        """
        try:
            positions = self.api.list_positions()
            position_data = []
            
            for position in positions:
                position_data.append({
                    'symbol': position.symbol,
                    'qty': float(position.qty),
                    'side': position.side,
                    'avg_entry_price': float(position.avg_entry_price),
                    'market_value': float(position.market_value),
                    'unrealized_pl': float(position.unrealized_pl),
                    'unrealized_plpc': float(position.unrealized_plpc),
                    'current_price': float(position.current_price) if position.current_price else None
                })
            
            logger.info(f"Retrieved {len(position_data)} positions")
            return position_data
            
        except Exception as e:
            logger.error(f"Error getting positions: {str(e)}")
            return []
    
    def get_account(self):
        """
        Get account information
        
        Returns:
            dict: Account information dictionary
        """
        try:
            account = self.api.get_account()
            
            account_data = {
                'account_number': account.account_number,
                'status': account.status,
                'currency': account.currency,
                'buying_power': float(account.buying_power),
                'cash': float(account.cash),
                'portfolio_value': float(account.portfolio_value),
                'equity': float(account.equity),
                'last_equity': float(account.last_equity),
                'multiplier': float(account.multiplier),
                'initial_margin': float(account.initial_margin),
                'maintenance_margin': float(account.maintenance_margin),
                'sma': float(account.sma),
                'daytrade_count': int(account.daytrade_count),
                'daytrading_buying_power': float(account.daytrading_buying_power),
                'regt_buying_power': float(account.regt_buying_power),
                'pattern_day_trader': account.pattern_day_trader,
                'trading_blocked': account.trading_blocked,
                'transfers_blocked': account.transfers_blocked,
                'account_blocked': account.account_blocked,
                'created_at': account.created_at,
                'trade_suspended_by_user': account.trade_suspended_by_user,
                'crypto_status': account.crypto_status if hasattr(account, 'crypto_status') else None
            }
            
            logger.info(f"Account data retrieved for account {account_data['account_number']}")
            return account_data
            
        except Exception as e:
            logger.error(f"Error getting account: {str(e)}")
            raise
    
    def calculate_drawdown_data(self, symbol, lookback_days=180):
        """
        Calculate drawdown data for a symbol over the specified period
        
        Args:
            symbol (str): Stock symbol
            lookback_days (int): Number of days to look back for peak calculation
            
        Returns:
            dict: Drawdown analysis data
        """
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=lookback_days + 30)  # Add buffer for weekends/holidays
            
            # Get historical data
            bars = self.get_historical_bars(symbol, start_date, end_date)
            
            if len(bars) < 30:  # Need minimum data
                logger.warning(f"Insufficient data for {symbol}: {len(bars)} bars")
                return None
            
            # Calculate drawdown metrics
            current_price = bars[-1]['close']
            highs = [bar['high'] for bar in bars]
            peak_price = max(highs)
            
            # Find peak date and days since peak
            peak_index = highs.index(peak_price)
            days_since_peak = len(bars) - 1 - peak_index
            
            # Calculate drawdown percentage
            drawdown_pct = ((current_price - peak_price) / peak_price) * 100
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'peak_price': peak_price,
                'drawdown_pct': drawdown_pct,
                'days_since_peak': days_since_peak,
                'volume': bars[-1]['volume'],
                'bars_analyzed': len(bars)
            }
            
        except Exception as e:
            logger.error(f"Error calculating drawdown for {symbol}: {str(e)}")
            return None
    
    def get_market_calendar(self, start_date=None, end_date=None):
        """
        Get market calendar information
        
        Args:
            start_date (datetime, optional): Start date
            end_date (datetime, optional): End date
            
        Returns:
            list: Market calendar data
        """
        try:
            if start_date is None:
                start_date = datetime.now()
            if end_date is None:
                end_date = start_date + timedelta(days=7)
                
            calendar = self.api.get_calendar(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d')
            )
            
            return [
                {
                    'date': day.date,
                    'open': day.open,
                    'close': day.close
                }
                for day in calendar
            ]
            
        except Exception as e:
            logger.error(f"Error getting market calendar: {str(e)}")
            return []
    
    def is_market_open(self):
        """
        Check if the market is currently open
        
        Returns:
            bool: True if market is open, False otherwise
        """
        try:
            clock = self.api.get_clock()
            return clock.is_open
        except Exception as e:
            logger.error(f"Error checking market status: {str(e)}")
            return False
