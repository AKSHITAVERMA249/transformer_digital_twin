from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app


def test_dashboard_routes_serve_html_and_poll_live_endpoints(tmp_path: Path) -> None:
    app = create_app(db_path=tmp_path / "dash_test.db")
    with TestClient(app) as client:
        for route in ("/", "/dashboard"):
            res = client.get(route)
            assert res.status_code == 200
            assert "text/html" in res.headers["content-type"]
            html = res.text
            assert "TX-001" in html
            assert "Demo-Only Notice" in html
            assert "/assets/${ASSET_ID}/health" in html
            assert "/assets/${ASSET_ID}/history" in html
            assert "/assets/${ASSET_ID}/alerts" in html
            assert "API unreachable" in html
