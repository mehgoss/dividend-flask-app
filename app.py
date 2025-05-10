from flask import Flask, render_template, send_file, jsonify
import os
from scraper import scrape_and_process_dividends
import pandas as pd
import logging

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# CSV file path
CSV_PATH = os.path.join('static', 'data', 'dividends_with_prices_current_month.csv')

@app.route('/')
def index():
    # Check if CSV exists and is recent (e.g., within 1 hour)
    if os.path.exists(CSV_PATH):
        file_age = time.time() - os.path.getmtime(CSV_PATH)
        if file_age < 3600:  # 1 hour
            return render_template('index.html')
    
    # Trigger scraper if no recent CSV
    try:
        scrape_and_process_dividends()
        logger.info("Dividend data scraped and saved to CSV")
    except Exception as e:
        logger.error(f"Error scraping dividends: {e}")
        return render_template('index.html', error="Failed to fetch dividend data")
    
    return render_template('index.html')

@app.route('/refresh')
def refresh_data():
    try:
        scrape_and_process_dividends()
        return jsonify({"status": "success", "message": "Data refreshed successfully"})
    except Exception as e:
        logger.error(f"Error refreshing dividends: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/download')
def download_csv():
    if os.path.exists(CSV_PATH):
        return send_file(CSV_PATH, as_attachment=True)
    else:
        return "CSV file not found", 404

@app.route('/data')
def get_data():
    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH)
        return jsonify(df.to_dict(orient='records'))
    return jsonify([]), 404

if __name__ == '__main__':
    app.run(debug=True)
