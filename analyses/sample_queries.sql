-- ============================================================
-- Sample Analytics Queries — RavenStack SaaS Dimensional Model
-- Run with: dbt show --inline "query" --profiles-dir .
-- ============================================================

-- Q1: MRR Trend by Plan Tier (Monthly)
-- "What is our revenue trend per plan tier?"
SELECT
    date_trunc('month', s.start_date) AS month,
    p.plan_tier,
    SUM(s.mrr_amount) AS total_mrr,
    COUNT(*) AS subscriptions
FROM {{ ref('fct_subscriptions') }} s
JOIN {{ ref('dim_plan') }} p ON s.plan_key = p.plan_key
GROUP BY 1, 2
ORDER BY 1, p.plan_tier_rank;


-- Q2: Top 5 Most Adopted Features
-- "Which features drive the most engagement?"
SELECT
    feature_name,
    COUNT(DISTINCT account_key) AS unique_accounts,
    SUM(usage_count) AS total_usage,
    ROUND(AVG(usage_duration_mins), 2) AS avg_duration_mins
FROM {{ ref('fct_feature_usage') }}
GROUP BY feature_name
ORDER BY total_usage DESC
LIMIT 5;


-- Q3: Top Churn Reasons
-- "Why are customers leaving?"
SELECT
    churn_reason,
    COUNT(*) AS churn_count,
    ROUND(AVG(refund_amount_usd), 2) AS avg_refund,
    SUM(CASE WHEN had_preceding_downgrade THEN 1 ELSE 0 END) AS preceded_by_downgrade
FROM {{ ref('fct_churn') }}
GROUP BY churn_reason
ORDER BY churn_count DESC;


-- Q4: Support Volume vs Churn Correlation
-- "Do accounts with more support tickets churn more?"
SELECT
    a.account_id,
    a.account_name,
    a.current_plan_tier,
    a.lifetime_tickets,
    a.lifetime_churn_events,
    a.avg_satisfaction_score,
    a.has_churned
FROM {{ ref('dim_account') }} a
WHERE a.lifetime_tickets > 0
ORDER BY a.lifetime_tickets DESC
LIMIT 20;


-- Q5: Trial-to-Paid Conversion Funnel
-- "How well do we convert trials to paying customers?"
SELECT
    plan_tier,
    COUNT(*) AS total_subscriptions,
    SUM(CASE WHEN is_trial THEN 1 ELSE 0 END) AS trials,
    SUM(CASE WHEN NOT is_trial THEN 1 ELSE 0 END) AS paid,
    SUM(CASE WHEN is_upgrade THEN 1 ELSE 0 END) AS upgrades,
    SUM(CASE WHEN has_churned THEN 1 ELSE 0 END) AS churned,
    ROUND(100.0 * SUM(CASE WHEN NOT is_trial THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN is_trial THEN 1 ELSE 0 END), 0), 2) AS trial_to_paid_pct
FROM {{ ref('fct_subscriptions') }} s
JOIN {{ ref('dim_plan') }} p ON s.plan_key = p.plan_key
GROUP BY plan_tier
ORDER BY plan_tier;
