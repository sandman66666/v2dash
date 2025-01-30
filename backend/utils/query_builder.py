from typing import Dict, Optional
from datetime import datetime
from dateutil.parser import parse


class OpenSearchQueryBuilder:
    def build_date_range_query(self, start_time: str, end_time: str) -> Dict:
        """
        Build a date range query for OpenSearch.

        Args:
            start_time: ISO format timestamp
            end_time: ISO format timestamp

        Returns:
            Dict containing the date range query
        """
        return {"range": {"timestamp": {"gte": start_time, "lte": end_time}}}

    def build_aggregation_query(
        self, agg_field: str, interval: Optional[str] = None
    ) -> Dict:
        """
        Build an aggregation query for OpenSearch.

        Args:
            agg_field: Field to aggregate on
            interval: Time interval for date histogram aggregation ('hour', 'day', 'week', 'month')

        Returns:
            Dict containing the aggregation query
        """
        # Define interval mappings and types
        fixed_intervals = {"hour": "1h", "day": "1d"}
        calendar_intervals = {"week": "week", "month": "month"}

        aggs = {}

        if interval:
            date_histogram = {
                "field": "timestamp",
                "min_doc_count": 0,
                "format": "yyyy-MM-dd'T'HH:mm:ssZ",
            }

            if interval in fixed_intervals:
                date_histogram["fixed_interval"] = fixed_intervals[interval]
            elif interval in calendar_intervals:
                date_histogram["calendar_interval"] = calendar_intervals[interval]
            else:
                # Default to 1d fixed interval
                date_histogram["fixed_interval"] = "1d"

            aggs["time_buckets"] = {"date_histogram": date_histogram}

        aggs[f"{agg_field}_buckets"] = {"terms": {"field": agg_field, "size": 10000}}

        return {"aggs": aggs}

    def build_paginated_query(
        self, search_after: Optional[str] = None, size: int = 100
    ) -> Dict:
        """
        Build a paginated query using search_after for deep pagination.

        Args:
            search_after: Token for pagination
            size: Number of results per page

        Returns:
            Dict containing the pagination parameters
        """
        query = {
            "sort": [{"timestamp": "desc"}, {"_id": "desc"}],
            "size": min(size, 1000),  # Enforce maximum page size
        }

        if search_after:
            query["search_after"] = search_after.split(",")

        return query

    def build_composite_query(
        self,
        must_conditions: list,
        source_fields: Optional[list] = None,
        aggregations: Optional[Dict] = None,
        pagination: Optional[Dict] = None,
    ) -> Dict:
        """
        Build a complete composite query combining multiple conditions.

        Args:
            must_conditions: List of must conditions for bool query
            source_fields: List of fields to include in _source
            aggregations: Aggregation queries
            pagination: Pagination parameters

        Returns:
            Dict containing the complete query
        """
        query = {"query": {"bool": {"must": must_conditions}}}

        if source_fields:
            query["_source"] = source_fields

        if aggregations:
            query.update(aggregations)

        if pagination:
            query.update(pagination)

        return query