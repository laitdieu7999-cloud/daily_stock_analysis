from api.v1.endpoints.history import get_history_detail


class _FakeService:
    def __init__(self, _db_manager):
        pass

    def resolve_and_get_detail(self, _record_id):
        return {
            "context_snapshot": {
                "enhanced_context": {
                    "realtime": {
                        "price": 123.4,
                        "change_pct": 0.0,
                        "change_60d": 9.9,
                    }
                },
                "realtime_quote_raw": {
                    "price": 123.4,
                    "change_pct": 8.8,
                    "pct_chg": 7.7,
                },
            },
            "raw_result": {"report_language": "zh"},
            "report_language": "zh",
            "code": "600519",
            "name": "贵州茅台",
            "report_markdown": "# report",
            "report_html": "<p>report</p>",
            "query_id": "query-1",
            "created_at": None,
            "report_type": "simple",
            "news_content": "",
            "model_used": "",
            "stock_name": "贵州茅台",
            "stock_code": "600519",
            "sentiment_score": 80,
            "trend_prediction": "看多",
            "operation_advice": "持有",
            "analysis_summary": "summary",
        }


def test_history_detail_preserves_zero_change_pct(monkeypatch):
    monkeypatch.setattr("api.v1.endpoints.history.HistoryService", _FakeService)

    class _FakeDbManager:
        def get_latest_fundamental_snapshot(self, query_id, code):
            return None

    report = get_history_detail("1", db_manager=_FakeDbManager())

    assert report.meta.current_price == 123.4
    assert report.meta.change_pct == 0.0
