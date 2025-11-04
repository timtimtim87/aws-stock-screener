# AWS Serverless Stock Screener

🤖 A serverless stock screening system that identifies beaten-down Russell 1000 stocks using a contrarian value strategy.

## 🎯 Overview

This system automatically:
- Collects daily Russell 1000 stock data and calculates drawdowns
- Stores data in CSV files on S3 for ultra-low cost (~$0.30/month)
- Provides Telegram bot interface for instant data access
- Enables local analysis by downloading CSV files
- Implements contrarian value strategy (buy beaten-down stocks, sell winners)

## 🏗️ Architecture

- **AWS Lambda** - Serverless data collection and bot functions
- **Amazon S3** - Ultra-low cost CSV data storage
- **Telegram Bot** - Real-time notifications and commands
- **Alpaca Markets** - Stock market data and paper trading
- **Local Analysis** - Jupyter notebooks for data visualization

## 💰 Cost Structure

- **S3 Storage:** ~$0.30/month for CSV files
- **Lambda Execution:** ~$2-5/month for daily runs
- **Total:** Under $10/month for full operation

## 📊 Investment Strategy

**Contrarian Value Approach:**
- **Buy Signal:** Stocks with worst 180-day drawdowns
- **Sell Signal:** When top 5 positions average ≥100% gains
- **Universe:** Russell 1000 stocks

## 🚀 Quick Start

### Prerequisites
- AWS Account
- Alpaca Markets Account (Paper Trading)
- Telegram Account
- Python 3.11+
- AWS CLI v2
- AWS SAM CLI

### Setup Steps

1. **Clone and setup:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/aws-stock-screener.git
   cd aws-stock-screener
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure AWS:**
   ```bash
   aws configure
   ```

4. **Deploy:**
   ```bash
   sam build --use-container
   sam deploy --guided
   ```

## 📱 Telegram Bot Commands

- `/screen` - View top 10 worst drawdown stocks
- `/portfolio` - Current portfolio summary
- `/monitor` - Check profit-taking opportunities
- `/account` - Account balance and status
- `/download` - Get CSV download links
- `/history` - Historical performance summary

## 📁 Project Structure

```
aws-stock-screener/
├── src/
│   ├── shared/           # Common utilities
│   ├── data_collector/   # Daily data gathering Lambda
│   └── telegram_bot/     # Bot interface Lambda
├── infrastructure/       # Deployment scripts
├── analysis/            # Local data analysis tools
├── tests/              # Integration tests
├── docs/               # Documentation
├── template.yaml       # SAM deployment config
└── README.md
```

## 🔐 Security

- All API keys stored in AWS Parameter Store
- No hardcoded credentials
- IAM roles with least-privilege access
- Secure webhook endpoints

## 📈 Data Outputs

1. **`daily_screening_results.csv`** - Complete Russell 1000 daily data
2. **`portfolio_snapshots.csv`** - Daily portfolio positions
3. **`top_candidates.csv`** - Top 10 drawdown candidates per day

## 🧪 Testing

Start with paper trading:
```bash
# Run integration tests
python tests/test_integration.py

# Test Telegram bot locally
python tests/test_telegram_bot.py
```

## 📚 Documentation

- [Implementation Guide](docs/implementation-guide.md)
- [API Documentation](docs/api-docs.md)
- [Troubleshooting](docs/troubleshooting.md)

## ⚠️ Disclaimer

This system is for educational purposes and to support investment decisions. Always:
- Start with paper trading
- Begin with small positions when going live
- Monitor performance closely
- Never invest more than you can afford to lose
- Consider consulting a financial advisor

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 📞 Support

- Create an issue for bugs or feature requests
- Check the [troubleshooting guide](docs/troubleshooting.md)
- Review CloudWatch logs for debugging

---

⭐ **Star this repo if it helps you!**
