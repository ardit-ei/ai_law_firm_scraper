from flask import Flask, render_template, request, redirect, url_for
import subprocess

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Get the form inputs
        urls = request.form['urls'].strip().split('\n')  # Multiple URLs split by new lines
        output_file = request.form['output_file'].strip()
        
        # Run the scraper for each URL
        for url in urls:
            if url:  # Ensure it's not empty
                run_scraper(url.strip(), output_file)

        return redirect(url_for('success'))

    return render_template('index.html')

@app.route('/success')
def success():
    return "Scraping started! Check your output files for results."

def run_scraper(url, output_file):
    """Runs the scraper script for the given URL and output file."""
    try:
        # Run the scraper using subprocess
        subprocess.Popen(['python3', 'scraper.py', url, output_file])
    except Exception as e:
        print(f"Failed to run scraper for {url}: {e}")

if __name__ == '__main__':
    app.run(debug=True)
