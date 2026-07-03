"""
OrbisScope Risk Index (BGRI-style, Tier A + C).

Two pillars, written to two tables:

  Pillar 1 — Market Attention  -> risk_scores
    Daily attention*negativity per scope (GLOBAL + top countries), standardized
    against an EWMA history into a z-score. attention_z is the real "GTI":
    0 = normal, +1 = one standard deviation above normal market attention.

  Pillar 2 — Market Movement ("priced-in")  -> risk_movement
    Each risk maps to a signed basket of expected 1-month asset shocks (a simple
    Market-Driven Scenario). We compare it to actual trailing 1-month returns:
      similarity = cosine(actual, expected)   (+1 priced-in, 0 none, -1 betting against)
      magnitude  = normalized size of the move
      movement_index = similarity * magnitude

Runs on CPU (24/7 Actions tier). Falls back to sentiment_score for rows written
before the split-signal migration, so it works on existing data immediately.
"""

import math
import numpy as np
import pandas as pd
import yfinance as yf

from common import get_supabase, now_iso, fetch_all, RISKS

TOP_COUNTRIES = 12           # per-country scopes to score
HISTORY_DAYS = 120           # days of risk_scores to (re)write
EWMA_HALFLIFE = 21           # weight recent history more heavily

# Market-Driven Scenario baskets: expected sign of 1-month asset shock per risk.
BASKETS = {
    "MILITARY_CONFLICT": {"CL=F": 1, "GC=F": 1, "GLD": 1, "SI=F": 0.5,
                          "^GSPC": -1, "^IXIC": -1, "^DJI": -1},
    "DIPLOMATIC_TENSION": {"GC=F": 1, "GLD": 1, "^GSPC": -0.5, "^IXIC": -0.5},
    "ECONOMIC_CRISIS": {"^GSPC": -1, "^IXIC": -1, "^DJI": -1, "GC=F": 1, "GLD": 1},
}
BASKET_ASSETS = sorted({a for b in BASKETS.values() for a in b})


# --------------------------------------------------------------------------
# Pillar 1 — attention z-score
# --------------------------------------------------------------------------

def load_events(supabase):
    rows = fetch_all(
        supabase, "processed_events",
        "sentiment_score,relevance,sentiment_signed,source_weight,event_type,"
        "location_name,processed_at,risk_id,risk_relevance",
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if "risk_id" not in df:
        df["risk_id"] = None
    df["processed_at"] = pd.to_datetime(df["processed_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["processed_at"])
    df["day"] = df["processed_at"].dt.normalize()

    # Fallbacks for rows written before the migration.
    df["relevance"] = df["relevance"].fillna(0.5) if "relevance" in df else 0.5
    df["source_weight"] = df.get("source_weight", 1.0)
    df["source_weight"] = df["source_weight"].fillna(1.0)
    # If sentiment_signed missing, derive from tension: tension 1 -> -1, 0 -> +1.
    if "sentiment_signed" in df:
        df["sentiment_signed"] = df["sentiment_signed"].fillna(1 - 2 * df["sentiment_score"].fillna(0.5))
    else:
        df["sentiment_signed"] = 1 - 2 * df["sentiment_score"].fillna(0.5)

    df["attention"] = df["relevance"] * df["source_weight"]
    # Negativity in [0,1]: 1 when fully escalatory, 0 when fully calming.
    df["negativity"] = (0.5 - 0.5 * df["sentiment_signed"]).clip(0, 1)
    df["risk_contrib"] = df["attention"] * df["negativity"]
    return df


def daily_scope(df):
    g = df.groupby("day").agg(
        risk_level=("risk_contrib", "sum"),
        att=("attention", "sum"),
        event_count=("risk_contrib", "count"),
    )
    # Attention-weighted net sentiment.
    ns = (df.assign(w=df["attention"], ws=df["attention"] * df["sentiment_signed"])
            .groupby("day").agg(ws=("ws", "sum"), w=("w", "sum")))
    g["net_sentiment"] = (ns["ws"] / ns["w"].replace(0, np.nan)).fillna(0)
    return g.sort_index()


def zscore(series):
    mean = series.ewm(halflife=EWMA_HALFLIFE).mean()
    std = series.ewm(halflife=EWMA_HALFLIFE).std().replace(0, np.nan)
    return ((series - mean) / std).fillna(0)


def build_risk_scores(df):
    scopes = {"GLOBAL": df}
    top = (df[df["location_name"].notna() & (df["location_name"] != "Global")]
           ["location_name"].value_counts().head(TOP_COUNTRIES).index.tolist())
    for c in top:
        scopes[c] = df[df["location_name"] == c]

    # Per named-risk scopes (Tier B) — e.g. US_CHINA, MIDDLE_EAST.
    if "risk_id" in df:
        for r in RISKS:
            sub = df[df["risk_id"] == r["slug"]]
            if not sub.empty:
                scopes[r["slug"]] = sub

    cutoff = df["day"].max() - pd.Timedelta(days=HISTORY_DAYS)
    out = []
    for scope, sub in scopes.items():
        if sub.empty:
            continue
        daily = daily_scope(sub)
        daily["attention_z"] = zscore(daily["risk_level"])
        recent = daily[daily.index >= cutoff]
        for day, row in recent.iterrows():
            out.append({
                "scope": scope,
                "day": day.date().isoformat(),
                "risk_level": round(float(row["risk_level"]), 4),
                "net_sentiment": round(float(row["net_sentiment"]), 4),
                "attention_z": round(float(row["attention_z"]), 4),
                "event_count": int(row["event_count"]),
                "updated_at": now_iso(),
            })
    return out


# --------------------------------------------------------------------------
# Pillar 2 — priced-in / market movement
# --------------------------------------------------------------------------

def market_returns():
    """Return {symbol: (ret_1m, vol_1m)} for basket assets."""
    out = {}
    for sym in BASKET_ASSETS:
        try:
            h = yf.download(sym, period="3mo", interval="1d",
                            progress=False, auto_adjust=True)
            if h is None or h.empty:
                continue
            c = h["Close"]
            c = c.iloc[:, 0] if isinstance(c, pd.DataFrame) else c
            c = c.astype(float).dropna()
            if len(c) < 25:
                continue
            ret_1m = float(c.iloc[-1] / c.iloc[-22] - 1)
            vol_1m = float(c.pct_change().std() * math.sqrt(21))
            out[sym] = (ret_1m, vol_1m if vol_1m > 0 else 0.01)
        except Exception as e:  # noqa: BLE001
            print(f"  ⚠️ returns {sym}: {e}")
    return out


def movement_for_basket(weights, rets):
    """similarity, magnitude, movement_index for a signed weight vector."""
    syms = [s for s in weights if s in rets]
    if len(syms) < 2:
        return None
    a = np.array([rets[s][0] for s in syms])              # actual 1m returns
    e = np.array([weights[s] for s in syms], dtype=float)  # expected shock signs
    na, ne = np.linalg.norm(a), np.linalg.norm(e)
    if na == 0 or ne == 0:
        return None
    similarity = float(np.dot(a, e) / (na * ne))
    typical = np.mean([rets[s][1] for s in syms])          # avg monthly vol
    magnitude = float(min(1.0, np.mean(np.abs(a)) / typical)) if typical else 0.0
    return similarity, magnitude, round(similarity * magnitude, 4)


def blended_weights(counts):
    """Blend basket vectors weighted by event-type counts."""
    total = sum(counts.get(k, 0) for k in BASKETS)
    if total == 0:
        return None
    blend = {}
    for etype, n in counts.items():
        if etype not in BASKETS or n == 0:
            continue
        for sym, w in BASKETS[etype].items():
            blend[sym] = blend.get(sym, 0.0) + w * (n / total)
    return blend or None


def build_risk_movement(df, rets):
    out = []

    def add(scope, weights):
        if not weights:
            return
        res = movement_for_basket(weights, rets)
        if not res:
            return
        sim, mag, idx = res
        out.append({"scope": scope, "similarity": round(sim, 4),
                    "magnitude": round(mag, 4), "movement_index": idx,
                    "updated_at": now_iso()})

    # Per event-type scenario.
    for etype, weights in BASKETS.items():
        add(etype, weights)

    # Per named-risk scenario (Tier B) — each risk's own MDS basket.
    for r in RISKS:
        add(r["slug"], r["basket"])

    # Recent window drives GLOBAL + per-country blends.
    recent = df[df["day"] >= (df["day"].max() - pd.Timedelta(days=30))]
    add("GLOBAL", blended_weights(recent["event_type"].value_counts().to_dict()))

    top = (recent[recent["location_name"].notna() & (recent["location_name"] != "Global")]
           ["location_name"].value_counts().head(TOP_COUNTRIES).index.tolist())
    for c in top:
        counts = recent[recent["location_name"] == c]["event_type"].value_counts().to_dict()
        add(c, blended_weights(counts))
    return out


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------

def run():
    supabase = get_supabase()
    print(f"[{now_iso()}] 🧮 Risk index starting...")

    df = load_events(supabase)
    if df.empty:
        print("  ❌ no events."); return

    scores = build_risk_scores(df)
    for i in range(0, len(scores), 200):
        supabase.table("risk_scores").upsert(scores[i:i + 200], on_conflict="scope,day").execute()
    latest_global = next((s for s in reversed(scores) if s["scope"] == "GLOBAL"), None)
    print(f"  ✅ risk_scores: {len(scores)} rows. "
          f"Global attention_z = {latest_global['attention_z'] if latest_global else 'n/a'}σ")

    rets = market_returns()
    movement = build_risk_movement(df, rets)
    for row in movement:
        supabase.table("risk_movement").upsert(row, on_conflict="scope").execute()
    g = next((m for m in movement if m["scope"] == "GLOBAL"), None)
    print(f"  ✅ risk_movement: {len(movement)} scopes. "
          f"Global priced-in = {g['movement_index'] if g else 'n/a'}")
    print("✅ Risk index complete.")


if __name__ == "__main__":
    run()
