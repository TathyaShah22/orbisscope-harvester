-- OrbisScope — deep AI Signals detail (trade setup, reasoning, reliability).
-- Run once in the Supabase SQL editor. Safe / idempotent.
alter table market_signals
  add column if not exists entry real,
  add column if not exists stop_loss real,
  add column if not exists target real,
  add column if not exists risk_reward real,
  add column if not exists atr_pct real,
  add column if not exists bull_strength real,
  add column if not exists bear_strength real,
  add column if not exists reasoning jsonb,          -- [{step,label,detail,contribution,direction}]
  add column if not exists triggering_event jsonb,   -- {description,category,severity,occurred_at}
  add column if not exists timeline jsonb,           -- [{description,severity,occurred_at}]
  add column if not exists signal_accuracy real,     -- in-sample backtest, %
  add column if not exists win_rate real,            -- in-sample backtest, %
  add column if not exists sharpe_ratio real,        -- in-sample backtest, annualized
  add column if not exists max_drawdown real,        -- in-sample backtest, %
  add column if not exists tags jsonb;                -- ["HIGH VOLATILITY","short-term","metals","global"]
