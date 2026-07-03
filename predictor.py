"""
OrbisScope Predictive Engine (Fix 1) — GTI + LightGBM -> market_signals.

Runs on CPU (LightGBM needs no GPU), so it lives in the 24/7 GitHub Actions
tier. Pipeline:

  1. Aggregate processed_events into a daily Geopolitical Tension Index (GTI):
     mean tension, event volume, and a conflict share (MILITARY/DIPLOMATIC).
  2. Pull daily price history per asset (yfinance) and engineer technical
     features (returns, RSI, momentum, volatility, price-vs-SMA).
  3. Join GTI onto every asset and train ONE pooled LightGBM classifier to
     predict next-day direction. Pooling gives the model enough rows to learn
     from despite a short history.
  4. For the latest bar of each asset, emit an actionable signal
     (action_signal / confidence / uncertainty / trend + factor bullets) and
     upsert into market_signals so the AI Signals panel shows live output.

This replaces the ~3-month-stale demo rows currently in market_signals.
"""

import numpy as np
import pandas as pd
import yfinance as yf
import lightgbm as lgb

from common import get_supabase, now_iso, fetch_all

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
    # yfinance may return a single-level or MultiIndex frame; grab Close robustly.
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

    df["target"] = (df["ret"].shift(-1) > 0).astype(int)
    return df


FEATURES = ["ret", "mom5", "mom20", "vol10", "px_vs_sma", "rsi",
            "gti", "gti_vol", "conflict_share", "gti_3d", "gti_delta"]


# --------------------------------------------------------------------------
# 3 + 4. Train pooled model and emit signals
# --------------------------------------------------------------------------

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
    if row["vol10"] > row.get("vol10_med", row["vol10"]):
        neg.append("Elevated short-term volatility")
    if prob_up >= 0.5 and not pos:
        pos.append("Model leans constructive on balance of factors")
    if prob_up < 0.5 and not neg:
        neg.append("Model leans cautious on balance of factors")
    return pos[:4], neg[:4]


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

    # Upsert-by-symbol (avoids needing a DB unique constraint).
    existing = supabase.table("market_signals").select("id,symbol").execute()
    id_by_symbol = {r["symbol"]: r["id"] for r in (existing.data or [])}

    for symbol, f in frames.items():
        sub = f.dropna(subset=FEATURES)
        latest = sub.iloc[-1]
        # Predict from a 1-row DataFrame cast to float; a transposed Series
        # collapses to object dtype, which LightGBM rejects.
        X_pred = sub.iloc[[-1]][FEATURES].astype(float)
        prob_up = float(model.predict_proba(X_pred)[0][1])
        action, trend = signal_from_prob(prob_up, latest["rsi"])
        confidence = round(max(prob_up, 1 - prob_up) * 100, 1)
        uncertainty = round((1 - abs(prob_up - 0.5) * 2) * 100, 1)
        pos, neg = describe_factors(latest, prob_up)

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
        }
        try:
            if symbol in id_by_symbol:
                supabase.table("market_signals").update(row).eq("symbol", symbol).execute()
            else:
                supabase.table("market_signals").insert(row).execute()
            print(f"  [+] {symbol:6} {action:11} conf={confidence}%  {trend}")
        except Exception as e:  # noqa: BLE001
            print(f"  ❌ upsert failed for {symbol}: {e}")

    print("✅ Predictor complete. market_signals refreshed.")


if __name__ == "__main__":
    run()
