import asyncio
import logging
from typing import Dict, List, Optional
from opensearchpy import AsyncOpenSearch
from opensearchpy.exceptions import ConnectionError, TransportError
from dashboardbackend.utils.query_builder import OpenSearchQueryBuilder

logger = logging.getLogger(__name__)

class EventRepository:
    def __init__(self, client: AsyncOpenSearch):
        self.client = client
        self.query_builder = OpenSearchQueryBuilder()
        self.index = "events-v2"  # Default index name
        self.max_retries = 3
        self.base_delay = 1  # Base delay in seconds

    async def _execute_with_retry(self, operation):
        """Execute an OpenSearch operation with exponential backoff retry."""
        for attempt in range(self.max_retries):
            try:
                return await operation()
            except (ConnectionError, TransportError) as e:
                if attempt == self.max_retries - 1:
                    raise e
                delay = self.base_delay * (2**attempt)  # Exponential backoff
                await asyncio.sleep(delay)

    async def get_producers_count(self) -> int:
        """Get count of users who have at least one sketch"""
        logger.info("Starting get_producers_count")
        
        query = {
            "aggs": {
                "unique_producers": {
                    "cardinality": {
                        "field": "event_data.body.butcherId.keyword"
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [
                        {"term": {"event_name.keyword": "uploadSketch_end"}}
                    ]
                }
            }
        }

        async def execute():
            try:
                logger.info(f"Executing producers search query on index: {self.index}")
                response = await self.client.search(
                    index=self.index,
                    body=query,
                    size=0,  # We only need the aggregation
                    request_timeout=30
                )
                logger.info("Search response received")
                producer_count = response.get('aggregations', {}).get('unique_producers', {}).get('value', 0)
                logger.info(f"Found {producer_count} producers")
                return producer_count
            except Exception as e:
                logger.error(f"Error executing producers search: {str(e)}", exc_info=True)
                raise

        logger.info("Calling _execute_with_retry for producers count")
        return await self._execute_with_retry(execute)

    async def get_event_counts(
        self,
        start_time: str,
        end_time: str,
        event_name: Optional[str] = None,
        event_type: Optional[str] = None,
        interval: Optional[str] = "day",
    ) -> List[Dict]:
        """Get event counts with time-based aggregation."""
        must_conditions = [
            self.query_builder.build_date_range_query(start_time, end_time)
        ]

        if event_name:
            must_conditions.append({"term": {"event_name": event_name}})
        if event_type:
            must_conditions.append({"term": {"type": event_type}})

        # Use the query builder's aggregation functionality
        aggs = self.query_builder.build_aggregation_query("timestamp", interval)

        query = self.query_builder.build_composite_query(
            must_conditions=must_conditions, aggregations=aggs
        )

        async def execute():
            response = await self.client.search(
                index=self.index, body=query, size=0  # We only need aggregations
            )
            return self._process_time_series_response(response)

        return await self._execute_with_retry(execute)

    async def get_user_events(
        self,
        user_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        event_name: Optional[str] = None,
        page_token: Optional[str] = None,
        page_size: int = 100,
    ) -> Dict:
        """Get paginated events for a specific user."""
        must_conditions = [{"term": {"trace_id": user_id}}]

        if start_time and end_time:
            must_conditions.append(
                self.query_builder.build_date_range_query(start_time, end_time)
            )
        if event_name:
            must_conditions.append({"term": {"event_name": event_name}})

        pagination = self.query_builder.build_paginated_query(
            search_after=page_token, size=page_size
        )

        query = self.query_builder.build_composite_query(
            must_conditions=must_conditions,
            source_fields=["event_name", "timestamp", "type", "event_data"],
            pagination=pagination,
        )

        async def execute():
            response = await self.client.search(index=self.index, body=query)
            return self._process_user_events_response(response)

        return await self._execute_with_retry(execute)

    async def get_error_summary(
        self, start_time: str, end_time: str, interval: Optional[str] = None
    ) -> Dict:
        """Get error events summary and trends."""
        must_conditions = [
            self.query_builder.build_date_range_query(start_time, end_time),
            {"term": {"type": "error"}},
        ]

        aggs = {
            "aggs": {
                "errors_by_name": {
                    "terms": {"field": "error_name", "size": 100},
                    "aggs": {"latest_occurrence": {"max": {"field": "timestamp"}}},
                }
            }
        }

        if interval:
            aggs["aggs"]["error_trends"] = {
                "date_histogram": {
                    "field": "timestamp",
                    "fixed_interval": f"1{interval}",
                }
            }

        query = self.query_builder.build_composite_query(
            must_conditions=must_conditions, aggregations=aggs
        )

        async def execute():
            response = await self.client.search(index=self.index, body=query, size=0)
            return self._process_error_summary_response(response, interval)

        return await self._execute_with_retry(execute)

    async def get_path_analytics(
        self, start_time: str, end_time: str, limit: int = 10
    ) -> Dict:
        """Get analytics for popular request paths."""
        must_conditions = [
            self.query_builder.build_date_range_query(start_time, end_time)
        ]

        aggs = {
            "aggs": {
                "popular_paths": {
                    "terms": {"field": "path", "size": limit},
                    "aggs": {
                        "average_status": {"avg": {"field": "status_code"}},
                        "error_count": {
                            "filter": {"range": {"status_code": {"gte": 400}}}
                        },
                    },
                }
            }
        }

        query = self.query_builder.build_composite_query(
            must_conditions=must_conditions, aggregations=aggs
        )

        async def execute():
            response = await self.client.search(index=self.index, body=query, size=0)
            return self._process_path_analytics_response(response)

        return await self._execute_with_retry(execute)

    def _process_time_series_response(self, response: Dict) -> List[Dict]:
        """Process the response from get_event_counts query into time series format."""
        if "time_buckets" not in response.get("aggregations", {}):
            return []

        return [
            {"timestamp": bucket["key_as_string"], "count": bucket["doc_count"]}
            for bucket in response["aggregations"]["time_buckets"]["buckets"]
        ]

    def _process_user_events_response(self, response: Dict) -> Dict:
        """Process the response from get_user_events query."""
        hits = response["hits"]["hits"]
        events = [
            {
                "event_name": hit["_source"]["event_name"],
                "timestamp": hit["_source"]["timestamp"],
                "type": hit["_source"]["type"],
                "event_data": hit["_source"]["event_data"],
            }
            for hit in hits
        ]

        result = {"events": events}

        # Add next page token if there are more results
        if hits:
            last_hit = hits[-1]
            result["next_page_token"] = f"{last_hit['sort'][0]},{last_hit['sort'][1]}"

        return result

    def _process_error_summary_response(
        self, response: Dict, interval: Optional[str]
    ) -> Dict:
        """Process the response from get_error_summary query."""
        result = {
            "total_errors": response["hits"]["total"]["value"],
            "errors_by_name": [
                {
                    "error_name": bucket["key"],
                    "count": bucket["doc_count"],
                    "latest_occurrence": bucket["latest_occurrence"]["value_as_string"],
                }
                for bucket in response["aggregations"]["errors_by_name"]["buckets"]
            ],
        }

        if interval and "error_trends" in response["aggregations"]:
            result["error_trends"] = [
                {"timestamp": bucket["key_as_string"], "count": bucket["doc_count"]}
                for bucket in response["aggregations"]["error_trends"]["buckets"]
            ]

        return result

    def _process_path_analytics_response(self, response: Dict) -> Dict:
        """Process the response from get_path_analytics query."""
        paths = []
        for bucket in response["aggregations"]["popular_paths"]["buckets"]:
            total_requests = bucket["doc_count"]
            error_count = bucket["error_count"]["doc_count"]

            paths.append({
                "path": bucket["key"],
                "total_requests": total_requests,
                "average_status": bucket["average_status"]["value"],
                "error_rate": (error_count / total_requests if total_requests > 0 else 0),
            })

        return {"paths": paths}