"""
API client for accessing organization and person data from the external API
"""
import requests
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import API_BASE_URL, API_KEY, MAX_WORKERS

logger = logging.getLogger(__name__)

class ApiClient:
    """Client for accessing the external API."""
    
    def __init__(self, base_url=API_BASE_URL, api_key=API_KEY, max_retries=3, retry_delay=1):
        self.base_url = base_url
        self.headers = {
            "api_key": api_key,
            "content-type": "application/json"
        }
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        
        # Verify API connection and authentication
        self._verify_auth()
    
    def _verify_auth(self):
        """Verify API authentication is working."""
        try:
            response = self.session.get(f"{self.base_url}/", headers=self.headers)
            response.raise_for_status()
            
            if response.json().get('Authenticated') is not True:
                logger.error(f"API authentication failed: {e}")
                raise ValueError("API authentication failed")
                
            logger.info("API authentication successful")
            
        except Exception as e:
            logger.error(f"API authentication failed: {e}")
            raise ValueError("API authentication failed")
    
    def _make_request(self, method, endpoint, data=None, params=None):
        """Make an API request with automatic retries."""
        url = f"{self.base_url}/{endpoint}"
        retries = 0
        
        while retries <= self.max_retries:
            try:
                if method.lower() == 'get':
                    response = self.session.get(url, headers=self.headers, params=params)
                elif method.lower() == 'post':
                    response = self.session.post(url, headers=self.headers, json=data)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries > self.max_retries:
                    logger.error(f"Failed to make API request after {self.max_retries} retries: {e}")
                    raise
                
                logger.warning(f"API request failed (attempt {retries}/{self.max_retries}): {e}")
                time.sleep(self.retry_delay * retries)
    
    def get_organization_data(self, domain):
        """Get organization data from the API by domain."""
        if not domain:
            logger.warning("No domain provided for organization lookup")
            return None
            
        try:
            return self._make_request('post', 'org', data={"domain": domain})
        except Exception as e:
            logger.error(f"Failed to fetch organization data for domain {domain}: {e}")
            return None
    
    def get_person_data(self, linkedin_url):
        """Get person data from the API by LinkedIn URL."""
        if not linkedin_url:
            logger.warning("No LinkedIn URL provided for person lookup")
            return None
            
        try:
            return self._make_request('post', 'person', data={"linkedin_url": linkedin_url})
        except Exception as e:
            logger.error(f"Failed to fetch person data for LinkedIn URL {linkedin_url}: {e}")
            return None

    def batch_get_data(self, items, data_type):
        """Fetch data for multiple items in parallel (organization or person data)."""
        if not items:
            return {}

        results = {}
        
        # Determine the appropriate function based on the data_type
        if data_type == 'organization':
            fetch_function = self.get_organization_data
        elif data_type == 'person':
            fetch_function = self.get_person_data
        else:
            raise ValueError("Invalid data_type. Use 'organization' or 'person'.")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all requests
            future_to_item = {
                executor.submit(fetch_function, item): item 
                for item in items if item
            }

            # Process as they complete
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    data = future.result()
                    if data:
                        results[item] = data
                except Exception as e:
                    logger.error(f"Error processing {data_type} {item}: {e}")

        return results


