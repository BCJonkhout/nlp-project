import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque  # Using deque for an efficient queue
import time
import re
from io import BytesIO
from typing import List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- All helper functions are unchanged ---
try:
    from PyPDF2 import PdfReader

    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
    print("Warning: PyPDF2 library not found. PDF extraction will be skipped. Install with 'pip install PyPDF2'")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
REQUEST_TIMEOUT = 20


def is_valid_url(url):
    # ... (unchanged)
    parsed = urlparse(url)
    return bool(parsed.scheme) and bool(parsed.netloc)


def normalize_url(url, base_url):
    # ... (unchanged)
    joined_url = urljoin(base_url, url)
    parsed_url = urlparse(joined_url)
    return parsed_url._replace(fragment="").geturl()


def extract_text_from_html(html_content: str) -> str:
    # ... (unchanged)
    soup = BeautifulSoup(html_content, 'html.parser')
    for script_or_style in soup(["script", "style", "header", "footer", "nav", "aside"]):
        script_or_style.decompose()
    text = soup.get_text(separator=' ', strip=True)
    text = re.sub(r'\s+', ' ', text)
    return text


def extract_text_from_pdf(pdf_content: bytes) -> str:
    # ... (unchanged)
    if not PYPDF2_AVAILABLE: return " (PDF content skipped: PyPDF2 not available) "
    try:
        reader = PdfReader(BytesIO(pdf_content))
        text = "".join(page.extract_text() or "" for page in reader.pages)
        return re.sub(r'\s+', ' ', text)
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return " (Error reading PDF content) "


def requests_session_with_retries(retries=5, backoff_factor=1,
                                  status_forcelist=(429, 500, 502, 503, 504)) -> requests.Session:
    # ... (unchanged)
    session = requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist, respect_retry_after_header=True)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def process_url(session: requests.Session, url: str, base_domain: str):
    # Worker function is now simpler: it doesn't need to know about depth.
    # It just processes one URL and returns its findings.
    try:
        headers = {"User-Agent": USER_AGENT}
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        final_url_domain = urlparse(response.url).netloc
        if final_url_domain != base_domain:
            return {"error": f"Redirected off domain: {url} -> {response.url}"}
        final_url = normalize_url(response.url, response.url)
        content_type = response.headers.get('content-type', '').lower()
        page_text, page_title, new_links = "", "No Title Found", []
        if 'html' in content_type:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            page_title = soup.title.string.strip() if soup.title and soup.title.string else page_title
            page_text = extract_text_from_html(html_content)
            for link_tag in soup.find_all('a', href=True):
                href = link_tag['href']
                absolute_url = normalize_url(href, final_url)
                if urlparse(absolute_url).netloc == base_domain and is_valid_url(absolute_url):
                    new_links.append(absolute_url)
        elif 'pdf' in content_type:
            page_text = extract_text_from_pdf(response.content)
        else:
            return {"error": f"Skipped non-HTML/PDF content type: {content_type} at {url}"}
        return {"url": final_url, "title": page_title, "text": page_text.strip(), "new_links": new_links, "error": None}
    except requests.exceptions.RequestException as e:
        return {"error": f"Request failed for {url} after retries: {e}"}
    except Exception as e:
        return {"error": f"Unexpected error for {url}: {e}"}


# --- NEW: Continuous Flow Orchestrator ---
def crawl_site_continuous(start_urls: Union[str, List[str]], max_pages: int, max_depth: int, workers: int,
                          throttle_delay: float) -> dict:
    if isinstance(start_urls, str): start_urls = [start_urls]

    base_domain = urlparse(start_urls[0]).netloc

    # Use a deque for an efficient thread-safe queue
    # The queue holds tuples of (url, current_depth)
    urls_to_crawl = deque([(url, 0) for url in start_urls])

    visited_urls = set()
    all_pages_data = []
    errors = []

    print(f"Starting continuous crawl with {workers} workers and a {throttle_delay}s throttle delay.")

    with ThreadPoolExecutor(max_workers=workers) as executor, requests_session_with_retries() as session:
        active_futures = set()

        while (urls_to_crawl or active_futures) and len(visited_urls) < max_pages:
            # Submit new jobs as long as there are free workers and URLs to process
            while len(active_futures) < workers and urls_to_crawl:
                url, depth = urls_to_crawl.popleft()
                if url in visited_urls or depth > max_depth:
                    continue

                visited_urls.add(url)
                future = executor.submit(process_url, session, url, base_domain)
                future.url_context = (url, depth)  # Attach context to the future
                active_futures.add(future)
                time.sleep(throttle_delay)  # Throttle new job submissions

            # Process completed jobs
            for future in as_completed(active_futures):
                active_futures.remove(future)  # Remove from in-flight set

                original_url, current_depth = future.url_context

                try:
                    result = future.result()
                    if result.get("error"):
                        errors.append(result["error"])
                        continue  # Don't print an error message here, it's too noisy

                    if result.get("text"):
                        all_pages_data.append(result)
                        print(
                            f"({len(all_pages_data)}/{len(visited_urls)}) Depth {current_depth} | Success: {result['url']}")

                    # Add newly found links to the queue for crawling if depth allows
                    if current_depth < max_depth:
                        for new_link in result.get("new_links", []):
                            if new_link not in visited_urls:
                                urls_to_crawl.append((new_link, current_depth + 1))
                except Exception as exc:
                    error_msg = f"{original_url} generated an exception: {exc}"
                    errors.append(error_msg)

                # Break the inner loop to go back to submitting new jobs
                break

    print("\n--- Crawling Finished ---")
    all_extracted_text = ""
    for page_data in all_pages_data:
        all_extracted_text += f"\n\n--- Content from: {page_data['url']} ---\n{page_data['text']}\n--- End of content from: {page_data['url']} ---\n"
    return {"pages": all_pages_data, "all_text": all_extracted_text.strip(), "crawled_sources": list(visited_urls),
            "errors": errors}


# --- Main execution block ---
if __name__ == "__main__":
    start_time = time.time()

    URL_TEMPLATE = "https://wetten.overheid.nl/zoeken/zoekresultaat/rs/2,3,4/titelf/1/tekstf/1/d/10-06-2025/dx/0/page/{page_number}/count/200/s/2"
    target_urls = [URL_TEMPLATE.format(page_number=page_num) for page_num in range(1, 58)]

    # --- TUNING PARAMETERS ---
    MAX_PAGES_TO_CRAWL = 5000  # Set a reasonable limit to avoid crawling forever
    MAX_CRAWL_DEPTH = 1
    CONCURRENT_WORKERS = 5
    THROTTLE_DELAY = 0.2  # (5 workers * 0.2s = 1s/request) -> max 5 req/sec

    crawl_results = crawl_site_continuous(
        target_urls,
        max_pages=MAX_PAGES_TO_CRAWL,
        max_depth=MAX_CRAWL_DEPTH,
        workers=CONCURRENT_WORKERS,
        throttle_delay=THROTTLE_DELAY
    )

    if target_urls:
        output_filename = f"{urlparse(target_urls[0]).netloc}_crawled_data_continuous.txt"
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(f"Data crawled from a list of {len(target_urls)} URLs.\n")
            f.write(f"URL Template: {URL_TEMPLATE}\n")
            f.write(
                f"Settings: Workers={CONCURRENT_WORKERS}, Throttle={THROTTLE_DELAY}s, Depth={MAX_CRAWL_DEPTH}, Max Pages={MAX_PAGES_TO_CRAWL}\n\n")
            f.write("=" * 50 + " CRAWLED SOURCES " + "=" * 50 + "\n")
            crawl_results["crawled_sources"].sort()
            for source in crawl_results["crawled_sources"]: f.write(f"- {source}\n")
            f.write("\n\n" + "=" * 50 + " AGGREGATED TEXT " + "=" * 50 + "\n")
            f.write(crawl_results["all_text"])
            if crawl_results["errors"]:
                f.write("\n\n" + "=" * 50 + " ERRORS " + "=" * 50 + "\n")
                for err in crawl_results["errors"]: f.write(f"- {err}\n")

    end_time = time.time()
    print(f"\nAll extracted text saved to: {output_filename}")
    print(f"Number of unique sources processed: {len(crawl_results['crawled_sources'])}")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")