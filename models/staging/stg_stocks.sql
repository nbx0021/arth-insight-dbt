with raw as (
    select * from {{ source('arth_bq', 'stock_intelligence_v3') }}
),

cleaned as (
    select
        ticker,
        company_name,
        
        -- 🐍 PYTHON LOGIC PORT: refine_sector_name
        -- We replicate your "God Mode" lists here
        CASE 
            -- FMCG
            WHEN ticker IN ('ITC', 'HINDUNILVR', 'NESTLEIND', 'BRITANNIA', 'DABUR', 'GODREJCP', 'MARICO', 'VBL', 'VARUN BEVERAGES', 'COLPAL') THEN 'FMCG'
            
            -- AUTOMOBILE
            WHEN ticker IN ('MARUTI', 'TATAMOTORS', 'M&M', 'ASHOKLEY', 'HEROMOTOCO', 'EICHERMOT', 'BAJAJ-AUTO', 'TVSMOTOR') THEN 'AUTOMOBILE'
            
            -- NBFC (Crucial for Debt Logic)
            WHEN ticker IN ('SHRIRAMFIN', 'BAJFINANCE', 'BAJAJFINSV', 'CHOLAFIN', 'MUTHOOTFIN', 'JIOFIN', 'PFC', 'REC', 'IRFC', 'M&MFIN') THEN 'NBFC'
            
            -- PHARMA
            WHEN ticker IN ('SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB', 'LUPIN', 'APOLLOHOSP', 'TORNTPHARM', 'ALKEM') THEN 'PHARMA'
            
            -- METALS
            WHEN ticker IN ('TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'VEDL', 'COALINDIA', 'NMDC', 'SAIL', 'HINDZINC') THEN 'METALS'
            
            -- CEMENT
            WHEN ticker IN ('ULTRACEMCO', 'AMBUJACEM', 'ACC', 'SHREECEM', 'DALBHARAT', 'RAMCOCEM') THEN 'CEMENT'
            
            -- BANKING (Safety Net)
            WHEN ticker IN ('HDFCBANK', 'ICICIBANK', 'SBIN', 'KOTAKBANK', 'AXISBANK', 'INDUSINDBK', 'BANKBARODA', 'PNB') THEN 'BANKING'
            
            -- OIL & GAS
            WHEN ticker IN ('RELIANCE', 'ONGC', 'OIL', 'IOC', 'BPCL', 'HPCL', 'GAIL', 'IGL', 'MGL') THEN 'OIL & GAS'
            
            -- AVIATION
            WHEN ticker IN ('INDIGO', 'SPICEJET', 'JETAIRWAYS') THEN 'AVIATION'
            
            -- POWER & RENEWABLES (Crucial for Debt Logic)
            WHEN ticker IN ('SUZLON', 'INOXWIND', 'KPIGREEN', 'ADANIGREEN', 'TATAPOWER', 'NTPC', 'POWERGRID', 'SJVN', 'NHPC', 'ADANIPOWER') THEN 'POWER & RENEWABLES'
            
            -- IT SERVICES
            WHEN ticker IN ('INFY', 'TCS', 'WIPRO', 'HCLTECH', 'TECHM', 'LTIM', 'PERSISTENT', 'COFORGE', 'MPHASIS', 'KPITTECH') THEN 'IT SERVICES'
            
            -- CONSUMER TECH
            WHEN ticker IN ('ZOMATO', 'PAYTM', 'NYKAA', 'POLICYBZR', 'DELHIVERY', 'NAUKRI', 'CARTRADE') THEN 'CONSUMER TECH'

            -- Fallback to Yahoo's sector if not in list
            ELSE sector 
        END as refined_sector,
        
        -- Passthrough Columns
        price as current_price,
        `52_high` as high_52_week,
        `52_low` as low_52_week,
        day_high,
        day_low,
        mcap as market_cap,
        enterprise_value,
        pe_ratio,
        industry_pe,
        pb_ratio,
        div_yield,
        
        -- Handle Nulls for Math
        coalesce(roe, 0) as roe,
        debt_to_equity,
        profit_growth,
        cagr_5y,
        
        -- JSON Columns (Keep as string for now)
        technicals,
        news,
        policy,
        shareholding,
        shares_fmt,
        chart_dates,
        chart_prices,
        chart_volumes,
        last_updated

    from raw
)

select * from cleaned