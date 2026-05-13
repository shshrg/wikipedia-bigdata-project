from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional
from uuid import UUID
from fastapi import FastAPI, HTTPException, Query
from cassandra.cluster import Cluster
import logging
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

state = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    cluster = Cluster(["wiki-cassandra"])
    session = cluster.connect("wikipedia_analytics")

    state["session"] = session
    state["STMT_DOMAINS"] = session.prepare("""
        SELECT domain, new_page_count, unique_authors, average_title_length, trend
        FROM wikipedia_analytics.language_activity
        WHERE window_start = ?
    """)
    state["STMT_PAGES_BY_USER"] = session.prepare("""
        SELECT page_id, page_title, domain, dt, user_is_bot
        FROM wikipedia_analytics.page_events
        WHERE user_name = ?
        LIMIT ?
    """)
    state["STMT_PAGE_BY_ID"] = session.prepare("""
        SELECT page_id, page_title, domain, user_name, user_is_bot, dt
        FROM wikipedia_analytics.pages_by_id
        WHERE page_id = ?
    """)
    state["STMT_PAGES_BY_DOMAIN"] = session.prepare("""
        SELECT page_id, page_title, user_name, user_is_bot, dt
        FROM wikipedia_analytics.pages_by_domain
        WHERE domain = ?
        AND dt >= ?
        AND dt <= ?
        LIMIT ?
    """)

    yield

    cluster.shutdown()
    logger.info("Cassandra connection closed.")


app = FastAPI(title="Wikipedia Analytics API", lifespan=lifespan)

def current_hour():
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0)

# C1. Domain List
@app.get("/api/domains")
def list_domains():
    session = state["session"]
    hour = current_hour() - timedelta(hours=1)
    rows = session.execute(state["STMT_DOMAINS"], (hour,))
    results = [dict(row._asdict()) for row in rows]
    if not results:
        hour = current_hour()
        rows = session.execute(state["STMT_DOMAINS"], (hour,))
        results = [dict(row._asdict()) for row in rows]
    return {"window_start": hour.isoformat(), "domains": results}


# C2. Pages by User
@app.get("/api/users/{user_id}/pages")
def pages_by_user(
    user_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
):
    rows = state["session"].execute(state["STMT_PAGES_BY_USER"], (user_id, limit))
    results = [dict(row._asdict()) for row in rows]
    if not results:
        raise HTTPException(status_code=404, detail=f"No pages found for user '{user_id}'")
    return {"user_name": user_id, "count": len(results), "pages": results}


# C3. Page Details
@app.get("/api/pages/{page_id}")
def page_details(page_id: UUID):
    rows = state["session"].execute(state["STMT_PAGE_BY_ID"], (page_id,))
    row = rows.one()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Page '{page_id}' not found")
    return dict(row._asdict())


# C4. Pages by Domain
@app.get("/api/domains/{domain}/pages")
def pages_by_domain(
    domain: str,
    from_ts: Optional[datetime] = Query(default=None, alias="from"),
    to_ts: Optional[datetime] = Query(default=None, alias="to"),
    limit: int = Query(default=100, ge=1, le=1000),
):
    now = datetime.now(timezone.utc)
    if to_ts is None:
        to_ts = now
    if from_ts is None:
        from_ts = now - timedelta(hours=24)
    if from_ts >= to_ts:
        raise HTTPException(status_code=400, detail="'from' must be earlier than 'to'")
    rows = state["session"].execute(state["STMT_PAGES_BY_DOMAIN"], (domain, from_ts, to_ts, limit))
    results = [dict(row._asdict()) for row in rows]
    if not results:
        raise HTTPException(status_code=404, detail=f"No pages found for domain '{domain}' in the given time range")
    return {"domain": domain, "from": from_ts.isoformat(), "to": to_ts.isoformat(), "count": len(results), "pages": results}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)