"""
OrbisScope Predictive Engine — GTI + LightGBM -> market_signals.

Runs on CPU (LightGBM needs no GPU), so it lives in the 24/7 GitHub Actions
tier. Pipeline:

  1. Aggregate processed_events into a daily Geopolitical Tension Index (GTI):
     mean tension, event volume, and a conflict share.
  2. Pull daily price history per asset (yfinance) and engineer technical
     features (returns, RSI, momentum, volatility, price-vs-SMA).
  3. Join GTI onto every asset and train ONE pooled LightGBM classifier to
     predict next-day direction. Pooling gives the model enough rows to learn
     from despite a short history.
  4. For the latest bar of each asset, emit a full signal:
       - action_signal / confidence / uncertainty / trend
       - trade setup: entry / stop-loss / target / R:R / ATR-like volatility
       - reasoning: the model's REAL top-contributing features (LightGBM
         pred_contrib — genuine attribution, not fabricated narrative)
       - triggering_event / timeline: the most relevant tagged news driving
         that asset right now, pulled from processed_events.risk_id
       - reliability: an IN-SAMPLE backtest (win rate, directional accuracy,
         Sharpe, max drawdown) of following this model's signal historically.
         Disclosed as in-sample, not a walk-forward guarantee.

Everything degrades gracefully: if migration 004 hasn't been run yet, the
upsert falls back to the original core columns instead of failing.
"""

import math
import numpy as np
import pandas as pd
import yfinance as yf
import lightgbm as lgb

from common import get_supabase, now_iso, fetch_all, RISKS

# symbol -> display name. Symbols match the existing market_signals rows so the
# upsert refreshes them in place.
ASSETS = {
    "^GSPC": "S&P 500",
    "^IXIC": "NASDAQ",
    "^DJI": "Dow Jones",
    "^N225": "Nikkei 225",
    "^HSI": "Hang Seng",
    "^NSEI": "Nifty 50",
    "GLD": "XAU/USD",
    "GC=F": "Gold Futures",
    "SI=F": "Silver",
    "CL=F": "Crude Oil",
}

ASSET_TAGS = {
    "^GSPC": {"category": "equities", "region": "us"},
    "^IXIC": {"category": "equities", "region": "us"},
    "^DJI": {"category": "equities", "region": "us"},
    "^N225": {"category": "equities", "region": "japan"},
    "^HSI": {"category": "equities", "region": "china"},
    "^NSEI": {"category": "equities", "region": "india"},
    "GLD": {"category": "metals", "region": "global"},
    "GC=F": {"category": "metals", "region": "global"},
    "SI=F": {"category": "metals", "region": "global"},
    "CL=F": {"category": "energy", "region": "global"},
}

RISK_NAME_BY_SLUG = {r["slug"]: r["name"] for r in RISKS}


# --------------------------------------------------------------------------
# 1. Geopolitical Tension Index
# --------------------------------------------------------------------------

def build_gti(supabase):
    # Paginate past the 1000-row cap so the GTI reflects the full event history.
    rows = fetch_all(supabase, "processed_events",
                     "sentiment_score,event_type,processed_at")
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()
    df["processed_at"] = pd.to_datetime(df["processed_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["processed_at"])
    df["date"] = df["processed_at"].dt.tz_convert("UTC").dt.normalize()
    df["is_conflict"] = df["event_type"].isin(
        ["MILITARY_CONFLICT", "DIPLOMATIC_TENSION", "ECONOMIC_CRISIS"]).astype(int)

    gti = df.groupby("date").agg(
        gti=("sentiment_score", "mean"),
        gti_vol=("sentiment_score", "count"),
        conflict_share=("is_conflict", "mean"),
    )
    gti["gti_3d"] = gti["gti"].rolling(3, min_periods=1).mean()
    gti["gti_delta"] = gti["gti"].diff().fillna(0)
    gti.index = gti.index.tz_localize(None)  # align with yfinance naive dates
    return gti


# --------------------------------------------------------------------------
# 2. Technical features
# --------------------------------------------------------------------------

def rsi(series, window=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window).mean()
    loss = (-delta.clip(upper=0)).rolling(window).mean()
    rs = gain / loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def asset_frame(symbol, gti):
    try:
        hist = yf.download(symbol, period="1y", interval="1d",
                           progress=False, auto_adjust=True)
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠️ download failed {symbol}: {e}")
        return None
    if hist is None or hist.empty:
        return None
    close = hist["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    df = pd.DataFrame({"close": close.astype(float)}).dropna()
    if len(df) < 40:
        return None

    df["ret"] = df["close"].pct_change()
    df["mom5"] = df["close"].pct_change(5)
    df["mom20"] = df["close"].pct_change(20)
    df["vol10"] = df["ret"].rolling(10).std()
    df["sma20"] = df["close"].rolling(20).mean()
    df["px_vs_sma"] = df["close"] / df["sma20"] - 1
    df["rsi"] = rsi(df["close"])

    df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
    if not gti.empty:
        df = df.join(gti, how="left")
    for col in ["gti", "gti_vol", "conflict_share", "gti_3d", "gti_delta"]:
        if col not in df:
            df[col] = 0.0
        df[col] = df[col].ffill().fillna(0.0)

    # Forward return (next day's move) drives both the classifier target and
    # the reliability backtest below.
    df["fwd_ret"] = df["ret"].shift(-1)
    df["target"] = (df["fwd_ret"] > 0).astype(int)
    return df


FEATURES = ["ret", "mom5", "mom20", "vol10", "px_vs_sma", "rsi",
            "gti", "gti_vol", "conflict_share", "gti_3d", "gti_delta"]

FEATURE_LABELS = {
    "gti": "Geopolitical Tension",
    "gti_delta": "Tension Trend (1d)",
    "gti_3d": "Tension Trend (3d)",
    "gti_vol": "News Volume",
    "conflict_share": "Conflict Share",
    "rsi": "Momentum (RSI)",
    "mom5": "5-Day Momentum",
    "mom20": "20-Day Momentum",
    "vol10": "Volatility",
    "px_vs_sma": "Trend vs Average",
    "ret": "Latest Move",
}


def describe_feature(name, value):
    if name == "gti":
        return f"Geopolitical tension reading of {value:+.2f} vs its historical baseline"
    if name == "gti_delta":
        return f"Tension {'climbing' if value > 0 else 'easing'} {abs(value):.2f} day-over-day"
    if name == "gti_3d":
        return f"3-day average tension reading of {value:+.2f}"
    if name == "gti_vol":
        return f"{value:.0f} tracked geopolitical events feeding the signal"
    if name == "conflict_share":
        return f"{value * 100:.0f}% of recent events are conflict-related"
    if name == "rsi":
        band = "oversold" if value < 35 else "overbought" if value > 70 else "neutral"
        return f"RSI at {value:.0f} ({band})"
    if name == "mom5":
        return f"{value * 100:+.1f}% price move over the last 5 sessions"
    if name == "mom20":
        return f"{value * 100:+.1f}% price move over the last 20 sessions"
    if name == "vol10":
        return f"10-day volatility running at {value * 100:.1f}% daily"
    if name == "px_vs_sma":
        return f"Price {abs(value) * 100:.1f}% {'above' if value > 0 else 'below'} its 20-day average"
    if name == "ret":
        return f"Last session moved {value * 100:+.1f}%"
    return f"{name}: {value:.3f}"


# --------------------------------------------------------------------------
# 3. Model attribution ("reasoning") — genuine LightGBM feature contributions
# --------------------------------------------------------------------------

def compute_reasoning(model, row_df):
    """Top-4 real drivers of this specific prediction, via LightGBM pred_contrib
    (SHAP-style additive attribution) — not a fabricated narrative."""
    contrib = model.predict(row_df, pred_contrib=True)
    contrib = contrib[0][:-1]  # drop the bias/base-value term
    total = sum(abs(c) for c in contrib) or 1.0
    ranked = sorted(zip(FEATURES, contrib), key=lambda x: abs(x[1]), reverse=True)[:4]
    steps = []
    for i, (name, c) in enumerate(ranked, 1):
        val = float(row_df.iloc[0][name])
        steps.append({
            "step": i,
            "label": FEATURE_LABELS.get(name, name),
            "detail": describe_feature(name, val),
            "contribution": round(abs(c) / total * 100, 1),
            "direction": "bullish" if c > 0 else "bearish",
        })
    return steps


# --------------------------------------------------------------------------
# 4. Reliability — IN-SAMPLE backtest of following this model's signal
# --------------------------------------------------------------------------

def backtest_asset(model, frame):
    sub = frame.dropna(subset=FEATURES + ["fwd_ret"])
    if len(sub) < 30:
        return None
    X = sub[FEATURES]
    probs = model.predict_proba(X)[:, 1]
    signal = np.sign(probs - 0.5)
    fwd = sub["fwd_ret"].to_numpy()

    mask = signal != 0
    if mask.sum() < 10:
        return None
    strat = (signal * fwd)[mask]

    win_rate = float((strat > 0).mean() * 100)
    signal_accuracy = float((np.sign(fwd[mask]) == signal[mask]).mean() * 100)
    sharpe = float(strat.mean() / strat.std() * math.sqrt(252)) if strat.std() > 0 else 0.0

    cum = np.cumprod(1 + strat)
    running_max = np.maximum.accumulate(cum)
    drawdown = cum / running_max - 1
    max_dd = float(abs(drawdown.min()) * 100)

    return {
        "win_rate": round(win_rate, 1),
        "signal_accuracy": round(signal_accuracy, 1),
        "sharpe_ratio": round(sharpe, 2),
        "max_drawdown": round(max_dd, 1),
    }


# --------------------------------------------------------------------------
# 5. Triggering event + timeline — real tagged news driving this asset
# --------------------------------------------------------------------------

def risk_candidates_for_symbol(symbol):
    return [r["slug"] for r in RISKS if symbol in r.get("basket", {})]


def latest_attention_by_scope(supabase, scopes):
    if not scopes:
        return {}
    rows = (supabase.table("risk_scores").select("scope,attention_z,day")
            .in_("scope", scopes).order("day", desc=True).execute().data) or []
    out = {}
    for r in rows:
        out.setdefault(r["scope"], r["attention_z"])
    return out


def fetch_events_for_risk(supabase, slug, limit=6):
    """Newest first, but prefer rows that actually carry a description —
    older tagged events from before the description fix would otherwise
    surface as blank "Event" placeholders."""
    q = supabase.table("processed_events").select("event_description,sentiment_score,processed_at")
    if slug:
        q = q.eq("risk_id", slug)
    rows = q.order("processed_at", desc=True).limit(max(limit * 4, 20)).execute().data or []
    with_desc = [r for r in rows if r.get("event_description")]
    return (with_desc or rows)[:limit]


def news_context(supabase, symbol, attn_map):
    candidates = risk_candidates_for_symbol(symbol)
    driving_slug = max(candidates, key=lambda s: attn_map.get(s, -999)) if candidates else None

    events = fetch_events_for_risk(supabase, driving_slug) if driving_slug else []
    if not events:
        driving_slug = None
        events = fetch_events_for_risk(supabase, None, limit=5)

    triggering = None
    if events:
        top = events[0]
        triggering = {
            "description": top.get("event_description") or "Market-wide conditions",
            "category": RISK_NAME_BY_SLUG.get(driving_slug, "Global Market"),
            "severity": round(float(top.get("sentiment_score") or 0) * 100, 1),
            "occurred_at": top.get("processed_at"),
        }
    timeline = [
        {
            "description": e.get("event_description") or "Event",
            "severity": round(float(e.get("sentiment_score") or 0) * 100, 1),
            "occurred_at": e.get("processed_at"),
        }
        for e in events[:5]
    ]
    return triggering, timeline


# --------------------------------------------------------------------------
# Signal shaping
# --------------------------------------------------------------------------

def signal_from_prob(prob_up, rsi_val):
    if prob_up >= 0.66:
        action = "STRONG BUY"
    elif prob_up >= 0.55:
        action = "BUY"
    elif prob_up <= 0.34:
        action = "STRONG SELL"
    elif prob_up <= 0.45:
        action = "SELL"
    else:
        action = "HOLD"

    trend = "BULLISH" if prob_up >= 0.5 else "BEARISH"
    if rsi_val < 35:
        trend += " (OVERSOLD)"
    elif rsi_val > 70:
        trend += " (OVERBOUGHT)"
    return action, trend


def describe_factors(row, prob_up):
    pos, neg = [], []
    if row["rsi"] < 35:
        pos.append("Asset is oversold (RSI < 35) — mean-reversion upside")
    elif row["rsi"] > 70:
        neg.append("Asset is overbought (RSI > 70) — pullback risk")
    if row["px_vs_sma"] > 0.01:
        pos.append("Trading above its 20-day trend")
    elif row["px_vs_sma"] < -0.01:
        neg.append("Trading below its 20-day trend")
    if row["mom20"] > 0:
        pos.append("Positive 20-day price momentum")
    else:
        neg.append("Negative 20-day price momentum")
    if row["gti_delta"] > 0.02:
        neg.append("Geopolitical tension rising — macro risk-off pressure")
    elif row["gti_delta"] < -0.02:
        pos.append("Geopolitical tension easing — supportive backdrop")
    if prob_up >= 0.5 and not pos:
        pos.append("Model leans constructive on balance of factors")
    if prob_up < 0.5 and not neg:
        neg.append("Model leans cautious on balance of factors")
    return pos[:4], neg[:4]


def trade_setup(entry, prob_up, vol10):
    """Deterministic trade levels from current price + realized volatility —
    a simple heuristic (stop = 1.5x daily vol, target = 2x the stop distance),
    not a fitted model. Disclosed as such in the UI."""
    risk_reward = 2.0
    direction = 1 if prob_up >= 0.5 else -1
    risk_distance = entry * max(vol10, 0.002) * 1.5
    stop_loss = entry - direction * risk_distance
    target = entry + direction * risk_reward * risk_distance
    atr_pct = round(float(vol10) * 100, 2)
    return {
        "entry": round(entry, 2),
        "stop_loss": round(stop_loss, 2),
        "target": round(target, 2),
        "risk_reward": risk_reward,
        "atr_pct": atr_pct,
    }


CORE_KEYS = ["symbol", "name", "price", "change_pct", "confidence", "uncertainty",
             "action_signal", "trend", "positive_factors", "negative_factors", "updated_at"]

_HAS_DETAIL_COLS = True


def upsert_signal(supabase, row, existing_symbols):
    global _HAS_DETAIL_COLS
    payload = row if _HAS_DETAIL_COLS else {k: row[k] for k in CORE_KEYS}
    try:
        if row["symbol"] in existing_symbols:
            supabase.table("market_signals").update(payload).eq("symbol", row["symbol"]).execute()
        else:
            supabase.table("market_signals").insert(payload).execute()
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if _HAS_DETAIL_COLS and ("PGRST204" in msg or "schema cache" in msg or "does not exist" in msg):
            print("  ⚠️ detail columns missing (run migration 004) — falling back to core fields")
            _HAS_DETAIL_COLS = False
            core = {k: row[k] for k in CORE_KEYS}
            if row["symbol"] in existing_symbols:
                supabase.table("market_signals").update(core).eq("symbol", row["symbol"]).execute()
            else:
                supabase.table("market_signals").insert(core).execute()
        else:
            raise


def run():
    supabase = get_supabase()
    print(f"[{now_iso()}] 📈 Predictor starting...")

    gti = build_gti(supabase)
    print(f"  GTI series: {len(gti)} days.")

    frames = {}
    for symbol in ASSETS:
        f = asset_frame(symbol, gti)
        if f is not None:
            f["symbol"] = symbol
            frames[symbol] = f
    if not frames:
        print("  ❌ No market data available. Aborting.")
        return

    # Pooled training set (all assets, drop rows with NaN target/features).
    train = pd.concat(frames.values())
    train = train.dropna(subset=FEATURES + ["target"])
    X, y = train[FEATURES], train["target"]

    model = lgb.LGBMClassifier(n_estimators=200, max_depth=5,
                               learning_rate=0.05, verbosity=-1)
    model.fit(X, y)
    print(f"  Model trained on {len(X)} samples across {len(frames)} assets.")

    # Which named risks are relevant to any tracked asset, and their current
    # attention — used to pick each asset's "driving" risk/news.
    all_candidate_slugs = sorted({s for sym in ASSETS for s in risk_candidates_for_symbol(sym)})
    attn_map = latest_attention_by_scope(supabase, all_candidate_slugs)

    existing = supabase.table("market_signals").select("id,symbol").execute()
    existing_symbols = {r["symbol"] for r in (existing.data or [])}

    for symbol, f in frames.items():
        sub = f.dropna(subset=FEATURES)
        latest = sub.iloc[-1]
        X_pred = sub.iloc[[-1]][FEATURES].astype(float)
        prob_up = float(model.predict_proba(X_pred)[0][1])
        action, trend = signal_from_prob(prob_up, latest["rsi"])
        confidence = round(max(prob_up, 1 - prob_up) * 100, 1)
        uncertainty = round((1 - abs(prob_up - 0.5) * 2) * 100, 1)
        pos, neg = describe_factors(latest, prob_up)

        setup = trade_setup(float(latest["close"]), prob_up, float(latest["vol10"]))
        reasoning = compute_reasoning(model, X_pred)
        reliability = backtest_asset(model, f) or {}
        triggering, timeline = news_context(supabase, symbol, attn_map)

        tags_meta = ASSET_TAGS.get(symbol, {"category": "asset", "region": "global"})
        vol_label = "HIGH" if setup["atr_pct"] > 2.5 else "MEDIUM" if setup["atr_pct"] > 1.2 else "LOW"
        tags = [f"{vol_label} VOLATILITY", "short-term", tags_meta["category"], tags_meta["region"]]

        bull_strength = round(prob_up * 100, 1)
        bear_strength = round((1 - prob_up) * 100, 1)

        row = {
            "symbol": symbol,
            "name": ASSETS[symbol],
            "price": round(float(latest["close"]), 2),
            "change_pct": round(float(latest["ret"] * 100), 2),
            "confidence": confidence,
            "uncertainty": uncertainty,
            "action_signal": action,
            "trend": trend,
            "positive_factors": pos,
            "negative_factors": neg,
            "updated_at": now_iso(),
            "bull_strength": bull_strength,
            "bear_strength": bear_strength,
            "reasoning": reasoning,
            "triggering_event": triggering,
            "timeline": timeline,
            "tags": tags,
            **setup,
            **{k: reliability.get(k) for k in ("signal_accuracy", "win_rate", "sharpe_ratio", "max_drawdown")},
        }
        try:
            upsert_signal(supabase, row, existing_symbols)
            print(f"  [+] {symbol:6} {action:11} conf={confidence}%  {trend}")
        except Exception as e:  # noqa: BLE001
            print(f"  ❌ upsert failed for {symbol}: {e}")

    print("✅ Predictor complete. market_signals refreshed.")


if __name__ == "__main__":
    run()
