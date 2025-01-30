"""
Gauge for counting active chat users (21+ handleMessageInThread_start events).
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from opensearchpy import AsyncOpenSearch
from . import GaugeResult

logger = logging.getLogger(__name__)

class ActiveChatUsersGauge:
    def __init__(self, client: AsyncOpenSearch):
        self.client = client
        self.index = "events-v2"

    async def get_gauge_data(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get gauge data in a format suitable for the dashboard."""
        try:
            query = {
                "aggs": {
                    "users_by_messages": {
                        "terms": {
                            "field": "trace_id.keyword",
                            "size": 10000  # Large enough to get all users
                        },
                        "aggs": {
                            "message_count": {
                                "value_count": {
                                    "field": "event_name.keyword"
                                }
                            },
                            "active_users_bucket_selector": {
                                "bucket_selector": {
                                    "buckets_path": {
                                        "count": "message_count"
                                    },
                                    "script": "params.count >= 21"
                                }
                            }
                        }
                    }
                },
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"event_name.keyword": "handleMessageInThread_start"}}
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

            buckets = response.get('aggregations', {}).get('users_by_messages', {}).get('buckets', [])
            user_count = len(buckets)
            
            date_range = ""
            if start_date and end_date:
                date_range = f" ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})"
            
            logger.info(f"Found {user_count} active chat users{date_range}")
            
            return GaugeResult(
                value=user_count,
                label="Active Chat Users",
                description="Users who have started 21+ message threads"
            ).to_dict()

        except Exception as e:
            logger.error(f"Error counting active chat users: {str(e)}")
            return GaugeResult(0, "Active Chat Users", f"Error: {str(e)}").to_dict()