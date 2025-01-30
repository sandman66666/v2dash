"""
Gauge for counting total number of users and new signups in Descope.
"""
import os
import aiohttp
import logging
import ssl
import certifi
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from . import GaugeResult

logger = logging.getLogger(__name__)

class DescopeUsersGauge:
    def __init__(self):
        self.api_url = os.getenv("DESCOPE_API_URL", "https://api.descope.com/v1/mgmt/user/search")
        self.bearer_token = os.getenv("DESCOPE_BEARER_TOKEN")

    async def get_gauge_data(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get gauge data in a format suitable for the dashboard.
        Returns both total users and new signups within the specified period.
        """
        try:
            headers = {
                "Authorization": f"Bearer {self.bearer_token}",
                "Content-Type": "application/json"
            }

            # Create SSL context with certifi certificates
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(self.api_url, headers=headers, json={}) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch Descope users: {response.status}")
                        return {
                            "total_users": 0,
                            "new_signups": 0
                        }

                    data = await response.json()
                    users = data.get("users", [])
                    
                    # Count total users
                    total_users = len(users)
                    
                    # Count new signups within the period if dates are provided
                    new_signups = 0
                    if start_date and end_date:
                        for user in users:
                            created_at = datetime.fromtimestamp(user.get("createdTime", 0) / 1000)
                            if start_date <= created_at <= end_date:
                                new_signups += 1
                        
                        logger.info(f"Found {new_signups} new users between {start_date} and {end_date}")
                    
                    logger.info(f"Found {total_users} total users in Descope")
                    return {
                        "total_users": total_users,
                        "new_signups": new_signups
                    }

        except Exception as e:
            logger.error(f"Error fetching Descope users: {str(e)}")
            return {
                "total_users": 0,
                "new_signups": 0
            }