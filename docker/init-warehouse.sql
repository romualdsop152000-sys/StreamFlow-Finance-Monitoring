-- Création de la table lead_lag_features
CREATE TABLE IF NOT EXISTS lead_lag_features (
    id SERIAL PRIMARY KEY,
    ts_minute_utc TIMESTAMP WITH TIME ZONE,
    execution_date DATE,
    
    -- BTC data
    btc_close DOUBLE PRECISION,
    btc_volume DOUBLE PRECISION,
    btc_high DOUBLE PRECISION,
    btc_low DOUBLE PRECISION,
    btc_change_pct DOUBLE PRECISION,
    btc_return_1m DOUBLE PRECISION,
    
    -- NASDAQ data
    ndaq_close DOUBLE PRECISION,
    ndaq_volume DOUBLE PRECISION,
    ndaq_high DOUBLE PRECISION,
    ndaq_low DOUBLE PRECISION,
    ndaq_return_1m DOUBLE PRECISION,
    
    -- Lag features BTC
    btc_close_lag_1 DOUBLE PRECISION,
    btc_close_lag_2 DOUBLE PRECISION,
    btc_close_lag_3 DOUBLE PRECISION,
    btc_close_lag_4 DOUBLE PRECISION,
    btc_close_lag_5 DOUBLE PRECISION,
    btc_volume_lag_1 DOUBLE PRECISION,
    btc_volume_lag_2 DOUBLE PRECISION,
    btc_volume_lag_3 DOUBLE PRECISION,
    btc_volume_lag_4 DOUBLE PRECISION,
    btc_volume_lag_5 DOUBLE PRECISION,
    
    -- Lead features BTC
    btc_close_lead_1 DOUBLE PRECISION,
    btc_close_lead_2 DOUBLE PRECISION,
    btc_close_lead_3 DOUBLE PRECISION,
    btc_close_lead_4 DOUBLE PRECISION,
    btc_close_lead_5 DOUBLE PRECISION,
    
    -- Lag features NASDAQ
    ndaq_close_lag_1 DOUBLE PRECISION,
    ndaq_close_lag_2 DOUBLE PRECISION,
    ndaq_close_lag_3 DOUBLE PRECISION,
    ndaq_close_lag_4 DOUBLE PRECISION,
    ndaq_close_lag_5 DOUBLE PRECISION,
    
    -- Lead features NASDAQ
    ndaq_close_lead_1 DOUBLE PRECISION,
    ndaq_close_lead_2 DOUBLE PRECISION,
    ndaq_close_lead_3 DOUBLE PRECISION,
    ndaq_close_lead_4 DOUBLE PRECISION,
    ndaq_close_lead_5 DOUBLE PRECISION,
    
    -- Metadata
    processed_at_utc TIMESTAMP WITH TIME ZONE,
    loaded_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    
    -- Contrainte d'unicité
    UNIQUE (ts_minute_utc, execution_date)
);

-- Index pour les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_leadlag_ts ON lead_lag_features (ts_minute_utc);
CREATE INDEX IF NOT EXISTS idx_leadlag_date ON lead_lag_features (execution_date);
CREATE INDEX IF NOT EXISTS idx_leadlag_btc_close ON lead_lag_features (btc_close);

-- Vue pour l'analyse de corrélation
CREATE OR REPLACE VIEW v_btc_ndaq_correlation AS
SELECT 
    execution_date,
    COUNT(*) as record_count,
    CORR(btc_return_1m, ndaq_return_1m) as correlation_1m,
    AVG(btc_return_1m) as avg_btc_return,
    AVG(ndaq_return_1m) as avg_ndaq_return,
    STDDEV(btc_return_1m) as stddev_btc_return,
    STDDEV(ndaq_return_1m) as stddev_ndaq_return
FROM lead_lag_features
WHERE btc_return_1m IS NOT NULL 
  AND ndaq_return_1m IS NOT NULL
GROUP BY execution_date
ORDER BY execution_date DESC;
