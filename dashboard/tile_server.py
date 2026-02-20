"""
FastAPI Tile Server for H3 MVT tiles.

Uses the H3EngineMVT directly for query execution and spatial operations.
This ensures the visualization exactly matches the h3_engine logic.

Endpoints:
- POST /query - Register queries and get a session ID
- GET /tiles/{layer}/{z}/{x}/{y}.mvt - Get MVT tiles for a layer

Run with:
    uvicorn dashboard.tile_server:app --host 0.0.0.0 --port 8001 --reload
"""

import re
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import sys
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from engine import H3EngineMVT

# Configuration
DB_PATH = project_root / "data" / "swissNAMES3D_combined_h3.duckdb"
SESSION_TTL_SECONDS = 600  # 10 minutes
TILE_CACHE_SIZE = 1000  # Max tiles to cache

# Session storage
sessions: dict[str, dict] = {}

# Tile cache: (session_id, layer, z, x, y) -> mvt_bytes
_tile_cache: dict[tuple, bytes] = {}
_tile_cache_order: list[tuple] = []  # LRU order


# ============ Tile Cache Helpers ============

def get_cached_tile(key: tuple) -> Optional[bytes]:
    """Get tile from cache."""
    if key in _tile_cache:
        # Move to end (most recently used)
        if key in _tile_cache_order:
            _tile_cache_order.remove(key)
        _tile_cache_order.append(key)
        return _tile_cache[key]
    return None


def set_cached_tile(key: tuple, data: bytes):
    """Store tile in cache with LRU eviction."""
    # Evict oldest if at capacity
    while len(_tile_cache) >= TILE_CACHE_SIZE and _tile_cache_order:
        oldest = _tile_cache_order.pop(0)
        _tile_cache.pop(oldest, None)

    _tile_cache[key] = data
    _tile_cache_order.append(key)


def clear_session_cache(session_id: str):
    """Clear all cached tiles for a session."""
    global _tile_cache_order
    keys_to_remove = [k for k in _tile_cache if k[0] == session_id]
    for key in keys_to_remove:
        _tile_cache.pop(key, None)
    _tile_cache_order = [k for k in _tile_cache_order if k[0] != session_id]


def cleanup_old_sessions():
    """Remove sessions older than TTL."""
    now = time.time()
    expired = [
        sid for sid, data in sessions.items()
        if now - data.get("created_at", 0) > SESSION_TTL_SECONDS
    ]
    for sid in expired:
        try:
            get_engine().rendering_unregister_session(sid)
        except Exception:
            pass
        clear_session_cache(sid)
        del sessions[sid]


# ============ Cell Pre-computation ============

def precompute_cells_for_session(session_id: str, session_data: dict):
    """Pre-compute all H3 cells for a session's queries. Called once at registration.

    Registers cells as DuckDB tables to avoid DataFrame overhead on every tile request.
    """
    engine = get_engine()

    query_a = session_data.get("query_a")
    query_b = session_data.get("query_b")
    operation = session_data.get("operation", "none")

    start_time = time.time()

    counts = engine.rendering_register_session(session_id, query_a, query_b)

    session_data["cell_counts"] = counts
    print(f"[precompute] Session {session_id}: Registered A={counts['a']}, B={counts['b']} cells")

    # Pre-compute result cells for intersection/union
    if operation in ["intersection", "union"] and query_a:
        try:
            if operation == "intersection" and query_b:
                result_relation = engine.intersection(query_a, query_b)
                count = engine.rendering_register_result(session_id, result_relation)
                print(f"[precompute] Session {session_id}: Result (intersection) has {count} cells")
            elif operation == "union":
                result_relation = engine.union(query_a)
                count = engine.rendering_register_result(session_id, result_relation)
                print(f"[precompute] Session {session_id}: Result (union) has {count} cells")
        except Exception as e:
            print(f"[precompute] Session {session_id}: Error computing result - {e}")

    elapsed = time.time() - start_time
    print(f"[precompute] Session {session_id}: Pre-computation done in {elapsed:.2f}s")


def validate_sql_where(where_clause: str) -> bool:
    """Basic SQL injection prevention."""
    if not where_clause or not where_clause.strip():
        return False

    dangerous_patterns = [
        r";\s*(?:DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|TRUNCATE)",
        r"--",
        r"/\*",
        r"UNION\s+SELECT",
        r"INTO\s+(?:OUTFILE|DUMPFILE)",
        r"LOAD_FILE",
        r"BENCHMARK",
        r"SLEEP\s*\(",
    ]

    clause_upper = where_clause.upper()
    for pattern in dangerous_patterns:
        if re.search(pattern, clause_upper, re.IGNORECASE):
            return False

    return True


# FastAPI app
app = FastAPI(
    title="H3 Tile Server",
    description="MVT tile server for H3 spatial data",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Engine (lazy initialization)
_engine: Optional[H3EngineMVT] = None


def get_engine() -> H3EngineMVT:
    """Get or create the H3Engine instance."""
    global _engine
    if _engine is None:
        _engine = H3EngineMVT(DB_PATH)
    return _engine


class QueryRequest(BaseModel):
    """Request body for registering a query."""
    query_a: Optional[str] = None
    query_b: Optional[str] = None
    operation: str = "none"


class QueryResponse(BaseModel):
    """Response for query registration."""
    session_id: str
    message: str


@app.post("/query", response_model=QueryResponse)
async def register_query(request: QueryRequest):
    """Register queries and return a session ID. Pre-computes cells synchronously."""
    cleanup_old_sessions()

    if request.query_a and not validate_sql_where(request.query_a):
        raise HTTPException(status_code=400, detail="Invalid Query A")
    if request.query_b and not validate_sql_where(request.query_b):
        raise HTTPException(status_code=400, detail="Invalid Query B")

    session_id = str(uuid.uuid4())[:8]
    session_data = {
        "query_a": request.query_a,
        "query_b": request.query_b,
        "operation": request.operation,
        "created_at": time.time(),
        "cell_counts": None,  # Filled by precompute_cells_for_session
    }

    sessions[session_id] = session_data

    try:
        precompute_cells_for_session(session_id, session_data)
    except Exception as e:
        print(f"[precompute] ERROR for session {session_id}: {e}")
        import traceback
        traceback.print_exc()

    return QueryResponse(
        session_id=session_id,
        message="Query registered, cells precomputed"
    )


def generate_tile(
    session_id: str,
    session_data: dict,
    layer: str,
    z: int,
    x: int,
    y: int,
) -> bytes:
    """Generate MVT tile for a layer using session-registered cells (fast path).

    Args:
        session_id: Session identifier
        session_data: Session data dict
        layer: Layer name (a, b, result, geom_a, geom_b)
        z, x, y: Tile coordinates
    """
    engine = get_engine()

    if layer in ["a", "b", "result"]:
        # Use session-registered tables (no DataFrame overhead)
        return engine.rendering_tile_from_session(session_id, layer, z, x, y) or b""

    elif layer == "geom_a":
        where = session_data.get("query_a")
        if where:
            return engine.rendering_tile_geometries(where, z, x, y, layer_name="geom_a") or b""

    elif layer == "geom_b":
        where = session_data.get("query_b")
        if where:
            return engine.rendering_tile_geometries(where, z, x, y, layer_name="geom_b") or b""

    return b""


@app.get("/tiles/{layer}/{z}/{x}/{y}.mvt")
async def get_tile(
    layer: str,
    z: int,
    x: int,
    y: int,
    session: str = Query(..., description="Session ID from /query")
):
    """Get MVT tile for a layer with LRU caching."""
    if session not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    if layer not in ["a", "b", "result", "geom_a", "geom_b"]:
        raise HTTPException(status_code=400, detail=f"Unknown layer: {layer}")

    session_data = sessions[session]

    # Check LRU cache
    cache_key = (session, layer, z, x, y)
    cached = get_cached_tile(cache_key)
    if cached is not None:
        return Response(
            content=cached,
            media_type="application/vnd.mapbox-vector-tile",
            headers={
                "Cache-Control": "public, max-age=3600",
                "X-Tile-Cache": "hit",
                "Access-Control-Allow-Origin": "*"
            }
        )

    # Generate tile
    mvt_data = generate_tile(session, session_data, layer, z, x, y)

    # Cache it
    set_cached_tile(cache_key, mvt_data)

    return Response(
        content=mvt_data,
        media_type="application/vnd.mapbox-vector-tile",
        headers={
            "Cache-Control": "public, max-age=3600",
            "X-Tile-Cache": "miss",
            "Access-Control-Allow-Origin": "*"
        }
    )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "sessions": len(sessions),
        "cached_tiles": len(_tile_cache),
    }


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown."""
    global _engine
    if _engine:
        _engine.close()
        _engine = None
