"""
Tableau Server Operations
Provides operations for interacting with Tableau Server via REST API
Handles authentication, data source and workbook management, and refresh operations
"""

import requests
import xml.etree.ElementTree as ET
import html
import logging
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import quote
import time

logger = logging.getLogger(__name__)


class TableauServerError(Exception):
    """Custom exception for Tableau Server API errors"""
    pass


class TableauOperations:
    """
    Tableau Server operations class
    Handles authentication, listing, and refresh operations via REST API
    """
    
    def __init__(self, server_url: str, username: str, password: str, site_id: str = ""):
        """
        Initialize Tableau operations
        
        Args:
            server_url: Tableau Server URL (e.g., 'https://tableau.company.com')
            username: Username for authentication
            password: Password for authentication
            site_id: Site ID (empty string for default site)
        """
        self.server_url = server_url.rstrip('/')
        self.username = username
        self.password = password
        self.site_id = site_id
        self.auth_token = None
        self.site_uuid = None
        self.api_version = "3.19"  # Tableau REST API version
        
        # Base API URL
        self.api_base = f"{self.server_url}/api/{self.api_version}"
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        })
        
    def sign_in(self) -> Tuple[str, str]:
        """
        Authenticates and retrieves an auth token and site ID
        
        Returns:
            Tuple of (auth_token, site_uuid)
            
        Raises:
            TableauServerError: If authentication fails
        """
        try:
            # Prepare sign-in XML payload
            signin_xml = f"""
            <tsRequest>
                <credentials name='{self.encode_for_display(self.username)}' 
                           password='{self.encode_for_display(self.password)}'>
                    <site contentUrl='{self.encode_for_display(self.site_id)}'/>
                </credentials>
            </tsRequest>
            """
            
            # Make sign-in request
            url = f"{self.api_base}/auth/signin"
            response = self.session.post(url, data=signin_xml)
            
            # Check response status
            self.check_status(response, "Sign in failed")
            
            # Parse response XML
            root = ET.fromstring(response.content)
            
            # Extract credentials
            credentials = root.find('.//tsResponse/credentials')
            if credentials is None:
                raise TableauServerError("No credentials found in sign-in response")
                
            self.auth_token = credentials.get('token')
            site_element = credentials.find('site')
            if site_element is not None:
                self.site_uuid = site_element.get('id')
            
            if not self.auth_token:
                raise TableauServerError("No auth token received")
                
            # Update session headers with auth token
            self.session.headers.update({
                'X-Tableau-Auth': self.auth_token
            })
            
            logger.info(f"Successfully signed in to Tableau Server. Site UUID: {self.site_uuid}")
            return self.auth_token, self.site_uuid
            
        except Exception as e:
            logger.error(f"Sign in failed: {e}")
            raise TableauServerError(f"Authentication failed: {e}")
            
    def sign_out(self) -> bool:
        """
        Invalidates the authentication token and logs out
        
        Returns:
            True if sign out successful, False otherwise
        """
        try:
            if not self.auth_token:
                logger.warning("No auth token available for sign out")
                return True
                
            # Make sign-out request
            url = f"{self.api_base}/auth/signout"
            response = self.session.post(url, data="")
            
            # Check response status
            self.check_status(response, "Sign out failed")
            
            # Clear authentication data
            self.auth_token = None
            self.site_uuid = None
            
            # Remove auth header
            if 'X-Tableau-Auth' in self.session.headers:
                del self.session.headers['X-Tableau-Auth']
                
            logger.info("Successfully signed out from Tableau Server")
            return True
            
        except Exception as e:
            logger.error(f"Sign out failed: {e}")
            return False
            
    def check_status(self, response: requests.Response, operation: str = "API call"):
        """
        Checks server response for errors and raises exceptions if needed
        
        Args:
            response: HTTP response object
            operation: Description of the operation for error messages
            
        Raises:
            TableauServerError: If response indicates an error
        """
        if response.status_code >= 400:
            try:
                # Try to parse error from XML response
                root = ET.fromstring(response.content)
                error_element = root.find('.//error')
                
                if error_element is not None:
                    error_code = error_element.get('code', 'Unknown')
                    error_detail = error_element.find('detail')
                    error_message = error_detail.text if error_detail is not None else "Unknown error"
                    
                    raise TableauServerError(
                        f"{operation} failed - Code: {error_code}, Message: {error_message}"
                    )
                else:
                    raise TableauServerError(f"{operation} failed - HTTP {response.status_code}")
                    
            except ET.ParseError:
                # If we can't parse XML, use HTTP status
                raise TableauServerError(
                    f"{operation} failed - HTTP {response.status_code}: {response.text}"
                )
                
    def get_sites(self) -> List[Dict[str, Any]]:
        """
        Retrieves a list of available sites on the Tableau server
        
        Returns:
            List of site dictionaries with id, name, and contentUrl
            
        Raises:
            TableauServerError: If not authenticated or request fails
        """
        if not self.auth_token:
            raise TableauServerError("Not authenticated. Call sign_in() first.")
            
        try:
            url = f"{self.api_base}/sites"
            response = self.session.get(url)
            
            self.check_status(response, "Get sites")
            
            # Parse response XML
            root = ET.fromstring(response.content)
            sites = []
            
            for site_element in root.findall('.//site'):
                site_info = {
                    'id': site_element.get('id'),
                    'name': site_element.get('name'),
                    'contentUrl': site_element.get('contentUrl', ''),
                    'adminMode': site_element.get('adminMode'),
                    'state': site_element.get('state')
                }
                sites.append(site_info)
                
            logger.info(f"Retrieved {len(sites)} sites from Tableau Server")
            return sites
            
        except Exception as e:
            logger.error(f"Get sites failed: {e}")
            raise TableauServerError(f"Failed to retrieve sites: {e}")
            
    def get_data_sources(self, name_filter: str = None) -> List[Dict[str, Any]]:
        """
        Retrieves data sources by name for a given site
        
        Args:
            name_filter: Optional filter to search for specific data source names
            
        Returns:
            List of data source dictionaries
            
        Raises:
            TableauServerError: If not authenticated or request fails
        """
        if not self.auth_token or not self.site_uuid:
            raise TableauServerError("Not authenticated. Call sign_in() first.")
            
        try:
            url = f"{self.api_base}/sites/{self.site_uuid}/datasources"
            response = self.session.get(url)
            
            self.check_status(response, "Get data sources")
            
            # Parse response XML
            root = ET.fromstring(response.content)
            data_sources = []
            
            for ds_element in root.findall('.//datasource'):
                ds_info = {
                    'id': ds_element.get('id'),
                    'name': ds_element.get('name'),
                    'contentUrl': ds_element.get('contentUrl'),
                    'type': ds_element.get('type'),
                    'createdAt': ds_element.get('createdAt'),
                    'updatedAt': ds_element.get('updatedAt')
                }
                
                # Add project information if available
                project_element = ds_element.find('project')
                if project_element is not None:
                    ds_info['project'] = {
                        'id': project_element.get('id'),
                        'name': project_element.get('name')
                    }
                
                # Add owner information if available
                owner_element = ds_element.find('owner')
                if owner_element is not None:
                    ds_info['owner'] = {
                        'id': owner_element.get('id'),
                        'name': owner_element.get('name')
                    }
                
                # Apply name filter if specified
                if name_filter is None or name_filter.lower() in ds_info['name'].lower():
                    data_sources.append(ds_info)
                    
            logger.info(f"Retrieved {len(data_sources)} data sources from Tableau Server")
            return data_sources
            
        except Exception as e:
            logger.error(f"Get data sources failed: {e}")
            raise TableauServerError(f"Failed to retrieve data sources: {e}")
            
    def get_workbooks(self, name_filter: str = None) -> List[Dict[str, Any]]:
        """
        Retrieves workbooks by name for a given site
        
        Args:
            name_filter: Optional filter to search for specific workbook names
            
        Returns:
            List of workbook dictionaries
            
        Raises:
            TableauServerError: If not authenticated or request fails
        """
        if not self.auth_token or not self.site_uuid:
            raise TableauServerError("Not authenticated. Call sign_in() first.")
            
        try:
            url = f"{self.api_base}/sites/{self.site_uuid}/workbooks"
            response = self.session.get(url)
            
            self.check_status(response, "Get workbooks")
            
            # Parse response XML
            root = ET.fromstring(response.content)
            workbooks = []
            
            for wb_element in root.findall('.//workbook'):
                wb_info = {
                    'id': wb_element.get('id'),
                    'name': wb_element.get('name'),
                    'contentUrl': wb_element.get('contentUrl'),
                    'showTabs': wb_element.get('showTabs'),
                    'size': wb_element.get('size'),
                    'createdAt': wb_element.get('createdAt'),
                    'updatedAt': wb_element.get('updatedAt')
                }
                
                # Add project information if available
                project_element = wb_element.find('project')
                if project_element is not None:
                    wb_info['project'] = {
                        'id': project_element.get('id'),
                        'name': project_element.get('name')
                    }
                
                # Add owner information if available
                owner_element = wb_element.find('owner')
                if owner_element is not None:
                    wb_info['owner'] = {
                        'id': owner_element.get('id'),
                        'name': owner_element.get('name')
                    }
                
                # Apply name filter if specified
                if name_filter is None or name_filter.lower() in wb_info['name'].lower():
                    workbooks.append(wb_info)
                    
            logger.info(f"Retrieved {len(workbooks)} workbooks from Tableau Server")
            return workbooks
            
        except Exception as e:
            logger.error(f"Get workbooks failed: {e}")
            raise TableauServerError(f"Failed to retrieve workbooks: {e}")
            
    def update_data_source(self, data_source_id: str) -> Dict[str, Any]:
        """
        Triggers a refresh operation for a specified data source
        
        Args:
            data_source_id: ID of the data source to refresh
            
        Returns:
            Dictionary with refresh job information
            
        Raises:
            TableauServerError: If not authenticated or refresh fails
        """
        if not self.auth_token or not self.site_uuid:
            raise TableauServerError("Not authenticated. Call sign_in() first.")
            
        try:
            # Prepare refresh XML payload
            refresh_xml = "<tsRequest />"
            
            url = f"{self.api_base}/sites/{self.site_uuid}/datasources/{data_source_id}/refresh"
            response = self.session.post(url, data=refresh_xml)
            
            self.check_status(response, "Data source refresh")
            
            # Parse response XML to get job information
            root = ET.fromstring(response.content)
            job_element = root.find('.//job')
            
            if job_element is not None:
                job_info = {
                    'id': job_element.get('id'),
                    'mode': job_element.get('mode'),
                    'type': job_element.get('type'),
                    'createdAt': job_element.get('createdAt')
                }
                
                logger.info(f"Data source refresh initiated. Job ID: {job_info['id']}")
                return job_info
            else:
                logger.info(f"Data source {data_source_id} refresh completed")
                return {'status': 'completed', 'data_source_id': data_source_id}
                
        except Exception as e:
            logger.error(f"Data source refresh failed: {e}")
            raise TableauServerError(f"Failed to refresh data source: {e}")
            
    def update_workbook(self, workbook_id: str) -> Dict[str, Any]:
        """
        Triggers a refresh operation for a specified workbook
        
        Args:
            workbook_id: ID of the workbook to refresh
            
        Returns:
            Dictionary with refresh job information
            
        Raises:
            TableauServerError: If not authenticated or refresh fails
        """
        if not self.auth_token or not self.site_uuid:
            raise TableauServerError("Not authenticated. Call sign_in() first.")
            
        try:
            # Prepare refresh XML payload
            refresh_xml = "<tsRequest />"
            
            url = f"{self.api_base}/sites/{self.site_uuid}/workbooks/{workbook_id}/refresh"
            response = self.session.post(url, data=refresh_xml)
            
            self.check_status(response, "Workbook refresh")
            
            # Parse response XML to get job information
            root = ET.fromstring(response.content)
            job_element = root.find('.//job')
            
            if job_element is not None:
                job_info = {
                    'id': job_element.get('id'),
                    'mode': job_element.get('mode'),
                    'type': job_element.get('type'),
                    'createdAt': job_element.get('createdAt')
                }
                
                logger.info(f"Workbook refresh initiated. Job ID: {job_info['id']}")
                return job_info
            else:
                logger.info(f"Workbook {workbook_id} refresh completed")
                return {'status': 'completed', 'workbook_id': workbook_id}
                
        except Exception as e:
            logger.error(f"Workbook refresh failed: {e}")
            raise TableauServerError(f"Failed to refresh workbook: {e}")
            
    def encode_for_display(self, text: str) -> str:
        """
        Encodes text for safe display and XML processing
        
        Args:
            text: Text to encode
            
        Returns:
            HTML-encoded text safe for XML
        """
        if text is None:
            return ""
        return html.escape(str(text), quote=True)
        
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get the status of a background job
        
        Args:
            job_id: ID of the job to check
            
        Returns:
            Dictionary with job status information
        """
        if not self.auth_token or not self.site_uuid:
            raise TableauServerError("Not authenticated. Call sign_in() first.")
            
        try:
            url = f"{self.api_base}/sites/{self.site_uuid}/jobs/{job_id}"
            response = self.session.get(url)
            
            self.check_status(response, "Get job status")
            
            # Parse response XML
            root = ET.fromstring(response.content)
            job_element = root.find('.//job')
            
            if job_element is not None:
                job_status = {
                    'id': job_element.get('id'),
                    'mode': job_element.get('mode'),
                    'type': job_element.get('type'),
                    'progress': job_element.get('progress'),
                    'createdAt': job_element.get('createdAt'),
                    'startedAt': job_element.get('startedAt'),
                    'completedAt': job_element.get('completedAt'),
                    'finishCode': job_element.get('finishCode')
                }
                return job_status
            else:
                raise TableauServerError(f"Job {job_id} not found")
                
        except Exception as e:
            logger.error(f"Get job status failed: {e}")
            raise TableauServerError(f"Failed to get job status: {e}")
            
    def wait_for_job_completion(self, job_id: str, timeout: int = 300, poll_interval: int = 10) -> Dict[str, Any]:
        """
        Wait for a background job to complete
        
        Args:
            job_id: ID of the job to wait for
            timeout: Maximum time to wait in seconds (default: 300)
            poll_interval: Time between status checks in seconds (default: 10)  
            
        Returns:
            Final job status dictionary
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            job_status = self.get_job_status(job_id)
            
            finish_code = job_status.get('finishCode')
            if finish_code is not None:
                # Job completed
                logger.info(f"Job {job_id} completed with finish code: {finish_code}")
                return job_status
                
            # Job still running, wait before next check
            time.sleep(poll_interval)
            
        # Timeout reached
        raise TableauServerError(f"Job {job_id} did not complete within {timeout} seconds")
        
    def tableau_refresh(self, workbook_names: List[str] = None, data_source_names: List[str] = None,
                       wait_for_completion: bool = False, timeout: int = 300) -> Dict[str, Any]:
        """
        Orchestrates the refresh of specified workbooks and data sources
        Handles authentication, lookup, refresh, and sign out
        
        Args:
            workbook_names: List of workbook names to refresh
            data_source_names: List of data source names to refresh
            wait_for_completion: Whether to wait for refresh jobs to complete
            timeout: Timeout for waiting for job completion
            
        Returns:
            Dictionary with refresh results
        """
        results = {
            'success': False,
            'workbooks_refreshed': [],
            'data_sources_refreshed': [],
            'jobs': [],
            'errors': []
        }
        
        try:
            # Step 1: Sign in
            logger.info("Starting Tableau refresh orchestration")
            self.sign_in()
            
            # Step 2: Refresh data sources
            if data_source_names:
                logger.info(f"Refreshing {len(data_source_names)} data sources")
                data_sources = self.get_data_sources()
                
                for ds_name in data_source_names:
                    try:
                        # Find data source by name
                        matching_ds = [ds for ds in data_sources if ds['name'] == ds_name]
                        
                        if not matching_ds:
                            error_msg = f"Data source '{ds_name}' not found"
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                            continue
                            
                        ds_id = matching_ds[0]['id']
                        job_info = self.update_data_source(ds_id)
                        
                        results['data_sources_refreshed'].append({
                            'name': ds_name,
                            'id': ds_id,
                            'job_info': job_info
                        })
                        
                        if 'id' in job_info:
                            results['jobs'].append(job_info)
                            
                    except Exception as e:
                        error_msg = f"Failed to refresh data source '{ds_name}': {e}"
                        logger.error(error_msg)
                        results['errors'].append(error_msg)
                        
            # Step 3: Refresh workbooks
            if workbook_names:
                logger.info(f"Refreshing {len(workbook_names)} workbooks")
                workbooks = self.get_workbooks()
                
                for wb_name in workbook_names:
                    try:
                        # Find workbook by name
                        matching_wb = [wb for wb in workbooks if wb['name'] == wb_name]
                        
                        if not matching_wb:
                            error_msg = f"Workbook '{wb_name}' not found"
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                            continue
                            
                        wb_id = matching_wb[0]['id']
                        job_info = self.update_workbook(wb_id)
                        
                        results['workbooks_refreshed'].append({
                            'name': wb_name,
                            'id': wb_id,
                            'job_info': job_info
                        })
                        
                        if 'id' in job_info:
                            results['jobs'].append(job_info)
                            
                    except Exception as e:
                        error_msg = f"Failed to refresh workbook '{wb_name}': {e}"
                        logger.error(error_msg)
                        results['errors'].append(error_msg)
                        
            # Step 4: Wait for job completion if requested
            if wait_for_completion and results['jobs']:
                logger.info(f"Waiting for {len(results['jobs'])} jobs to complete")
                
                for job in results['jobs']:
                    try:
                        final_status = self.wait_for_job_completion(job['id'], timeout)
                        job['final_status'] = final_status
                    except Exception as e:
                        error_msg = f"Error waiting for job {job['id']}: {e}"
                        logger.error(error_msg)
                        results['errors'].append(error_msg)
                        
            # Set success flag
            results['success'] = len(results['errors']) == 0
            
            logger.info(f"Tableau refresh orchestration completed. Success: {results['success']}")
            
        except Exception as e:
            error_msg = f"Tableau refresh orchestration failed: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            
        finally:
            # Step 5: Sign out
            try:
                self.sign_out()
            except Exception as e:
                logger.warning(f"Sign out failed: {e}")
                
        return results
        
    def __enter__(self):
        """Context manager entry"""
        self.sign_in()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.sign_out()