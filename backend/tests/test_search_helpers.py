"""
Unit tests for search helper functions and utilities.

Tests for:
- build_non_semantic_filters function
- SearchMode enum
- Filter building logic
"""

import pytest
from datetime import date

from app.core.enums import DBCOLUMNS, OPERATORS, SearchMode
from app.api.articles import build_non_semantic_filters, determine_search_mode


class TestSearchModeEnum:
    """Tests for SearchMode enum."""

    def test_search_mode_values(self):
        """Test that SearchMode has expected values."""
        assert SearchMode.SEMANTIC.value == "semantic"
        assert SearchMode.KEYWORD.value == "keyword"
        assert SearchMode.BROWSE.value == "browse"

    def test_search_mode_is_string_enum(self):
        """Test that SearchMode is a string enum."""
        assert isinstance(SearchMode.SEMANTIC, str)
        assert SearchMode.SEMANTIC == "semantic"


class TestDetermineSearchMode:
    """Comprehensive tests for determine_search_mode function."""

    def test_semantic_mode_with_query(self):
        """Semantic: query provided."""
        mode = determine_search_mode(query="climate change")
        assert mode == SearchMode.SEMANTIC

    def test_semantic_mode_with_any_query(self):
        """Semantic: any non-empty query triggers semantic search."""
        mode = determine_search_mode(query="test")
        assert mode == SearchMode.SEMANTIC

    def test_browse_mode_with_empty_string_query(self):
        """Browse: empty string query."""
        mode = determine_search_mode(query="")
        assert mode == SearchMode.BROWSE

    def test_browse_mode_no_query(self):
        """Browse: no query provided."""
        mode = determine_search_mode(query=None)
        assert mode == SearchMode.BROWSE


class TestBuildNonSemanticFilters:
    """Tests for build_non_semantic_filters function."""

    def test_empty_filters(self):
        """Test with no parameters returns empty dict."""
        filters = build_non_semantic_filters()
        assert filters == {}

    def test_date_start_filter(self):
        """Test date_start filter."""
        filters = build_non_semantic_filters(date_start=date(2024, 1, 1))

        assert DBCOLUMNS.date in filters
        assert len(filters[DBCOLUMNS.date]) == 1
        assert filters[DBCOLUMNS.date][0] == (OPERATORS.ge, date(2024, 1, 1))

    def test_date_end_filter(self):
        """Test date_end filter."""
        filters = build_non_semantic_filters(date_end=date(2024, 12, 31))

        assert DBCOLUMNS.date in filters
        assert len(filters[DBCOLUMNS.date]) == 1
        assert filters[DBCOLUMNS.date][0] == (OPERATORS.le, date(2024, 12, 31))

    def test_date_range_filter(self):
        """Test both date_start and date_end."""
        filters = build_non_semantic_filters(
            date_start=date(2024, 1, 1),
            date_end=date(2024, 12, 31),
        )

        assert DBCOLUMNS.date in filters
        assert len(filters[DBCOLUMNS.date]) == 2
        assert (OPERATORS.ge, date(2024, 1, 1)) in filters[DBCOLUMNS.date]
        assert (OPERATORS.le, date(2024, 12, 31)) in filters[DBCOLUMNS.date]

    def test_tag_filter(self):
        """Test tag filter."""
        filters = build_non_semantic_filters(tag="Politics")

        assert DBCOLUMNS.tag in filters
        assert filters[DBCOLUMNS.tag] == [(OPERATORS.like, "Politics")]

    def test_tag_filter_with_whitespace(self):
        """Test tag filter strips whitespace."""
        filters = build_non_semantic_filters(tag="  Economics  ")

        assert DBCOLUMNS.tag in filters
        assert filters[DBCOLUMNS.tag] == [(OPERATORS.like, "Economics")]

    def test_archives_filter_single(self):
        """Test single archive filter."""
        filters = build_non_semantic_filters(archives=["lemonde"])

        assert DBCOLUMNS.archive in filters
        assert filters[DBCOLUMNS.archive] == [(OPERATORS.in_, ["lemonde"])]

    def test_archives_filter_multiple(self):
        """Test multiple archives filter."""
        archives_list = ["lemonde", "lesechos", "leparisien"]
        filters = build_non_semantic_filters(archives=archives_list)

        assert DBCOLUMNS.archive in filters
        assert filters[DBCOLUMNS.archive] == [(OPERATORS.in_, archives_list)]

    def test_has_image_filter(self):
        """Test has_image filter."""
        filters = build_non_semantic_filters(has_image=True)

        assert DBCOLUMNS.image in filters
        assert filters[DBCOLUMNS.image] == [(OPERATORS.notnull, None)]

    def test_has_image_false_no_filter(self):
        """Test has_image=False doesn't add filter."""
        filters = build_non_semantic_filters(has_image=False)

        # has_image=False should not add a filter (falsy check)
        assert DBCOLUMNS.image not in filters

    def test_combined_filters(self):
        """Test multiple filters combined."""
        filters = build_non_semantic_filters(
            archives=["lemonde"],
            tag="Politics",
            date_start=date(2024, 1, 1),
            date_end=date(2024, 6, 30),
            has_image=True,
        )

        assert DBCOLUMNS.archive in filters
        assert DBCOLUMNS.tag in filters
        assert DBCOLUMNS.date in filters
        assert DBCOLUMNS.image in filters

        # Verify all filter values
        assert filters[DBCOLUMNS.archive] == [(OPERATORS.in_, ["lemonde"])]
        assert filters[DBCOLUMNS.tag] == [(OPERATORS.like, "Politics")]
        assert len(filters[DBCOLUMNS.date]) == 2
        assert filters[DBCOLUMNS.image] == [(OPERATORS.notnull, None)]

    def test_no_embedding_or_text_search_filter(self):
        """Test that embedding and text_searchable filters are never added."""
        filters = build_non_semantic_filters(
            archives=["lemonde"],
            tag="Test",
            date_start=date(2024, 1, 1),
            date_end=date(2024, 12, 31),
            has_image=True,
        )

        assert DBCOLUMNS.embedding not in filters
        assert DBCOLUMNS.text_searchable not in filters


class TestFilterOperators:
    """Tests for filter operators enum."""

    def test_all_operators_exist(self):
        """Test that all expected operators exist."""
        expected = ["eq", "gt", "lt", "ge", "le", "in", "like", "isnull", "notnull", "ts", "semantic"]

        for op in expected:
            assert hasattr(OPERATORS, op.replace("in", "in_"))

    def test_operator_values(self):
        """Test operator enum values."""
        assert OPERATORS.ge == "ge"
        assert OPERATORS.le == "le"
        assert OPERATORS.in_ == "in"
        assert OPERATORS.like == "like"
        assert OPERATORS.notnull == "notnull"
        assert OPERATORS.ts == "text_search"
        assert OPERATORS.semantic == "semantic"


class TestDBColumns:
    """Tests for DBCOLUMNS enum."""

    def test_all_columns_exist(self):
        """Test that all expected columns exist."""
        expected = [
            "rowid", "date", "archive", "image", "title",
            "content", "tag", "link", "hash", "text_searchable", "embedding"
        ]

        for col in expected:
            assert hasattr(DBCOLUMNS, col)

    def test_column_values(self):
        """Test column enum values."""
        assert DBCOLUMNS.date == "date"
        assert DBCOLUMNS.archive == "archive"
        assert DBCOLUMNS.tag == "tag"
        assert DBCOLUMNS.image == "image"
        assert DBCOLUMNS.text_searchable == "text_searchable"
        assert DBCOLUMNS.embedding == "embedding"


class TestFilterEdgeCases:
    """Edge case tests for filter building."""

    def test_none_values_ignored(self):
        """Test that None values don't create filters."""
        filters = build_non_semantic_filters(
            archives=None,
            tag=None,
            date_start=None,
            date_end=None,
            has_image=None,
            translated_query=None,
        )

        assert filters == {}

    def test_empty_archives_list(self):
        """Test empty archives list doesn't create filter."""
        filters = build_non_semantic_filters(archives=[])

        # Empty list is falsy
        assert DBCOLUMNS.archive not in filters

    def test_empty_tag_string(self):
        """Test empty tag string doesn't create filter."""
        filters = build_non_semantic_filters(tag="")

        # Empty string is falsy
        assert DBCOLUMNS.tag not in filters

    def test_empty_translated_query(self):
        """Test empty translated_query doesn't create filter."""
        filters = build_non_semantic_filters(translated_query="")

        # Empty string is falsy
        assert DBCOLUMNS.text_searchable not in filters
