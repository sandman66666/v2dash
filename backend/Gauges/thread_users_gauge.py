"""
Gauge for counting users who create threads.
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from opensearchpy import AsyncOpenSearch
from . import GaugeResult

logger = logging.getLogger(__name__)

class ThreadUsersGauge:
    def __init__(self, client: AsyncOpenSearch):
        self.client = client
        self.index = "events-v2"

    async def get_gauge_data(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get gauge data in a format suitable for the dashboard."""
        try:
            query = {
                "aggs": {
                    "unique_users": {
                        "cardinality": {
                            "field": "trace_id.keyword",
                            "precision_threshold": 40000
                        }
                    }
                },
                "query": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "event_name.keyword": [
                                        "createThread_start",
                                        "create_thread",
                                        "createThread"
                                    ]
                                }
                            }
                        ]
                    }
                }
            }

            # Add date range if provided
            if start_date and end_date:
                query["query"]["bool"]["must"].append({
                    "range": {
                        "timestamp": {
                            "gte": int(start_date.timestamp() * 1000),
                            "lt": int(end_date.timestamp() * 1000)
                        }
                    }
                })

            response = await self.client.search(
                index=self.index,
                body=query,
                size=0
            )

            user_count = response['aggregations']['unique_users']['value']
            
            date_range = ""
            if start_date and end_date:
                date_range = f" ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})"
            
            logger.info(f"Found {user_count} thread users{date_range}")
            
            return GaugeResult(
                value=user_count,
                label="Thread Users",
                description="Users who have created at least one thread"
            ).to_dict()

        except Exception as e:
            logger.error(f"Error counting thread users: {str(e)}")
            return GaugeResult(0, "Thread Users", f"Error: {str(e)}").to_dict()