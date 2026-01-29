# 📊 Silver Layer Data Analysis Findings

## Olist E-Commerce Data Warehouse

**Data Period:** September 2016 - October 2018  
**Total Orders:** ~99,441 | **Total Revenue:** ~R$ 15.4M

---

## Executive Summary

> **Key Findings at a Glance:**
>
> - 📦 **8.11%** of deliveries were late, with Northern states showing 2x higher rates
> - ⭐ Late deliveries correlate with **1.73 star lower** review scores
> - 💳 Credit card dominates at **73.92%** of payments with avg **2.8 installments**
> - 🏪 Top 10 sellers account for **12.80%** of total revenue
> - 📈 Orders grew **significant** month-over-month from 2017 to 2018

---

## Table of Contents

1. [Orders & Delivery Performance](#1-orders--delivery-performance)
2. [Revenue & Product Analysis](#2-revenue--product-analysis)
3. [Customer Analysis](#3-customer-analysis)
4. [Payment Behavior](#4-payment-behavior)
5. [Review & Satisfaction](#5-review--satisfaction)
6. [Seller Performance](#6-seller-performance)
7. [Time-Based Trends](#7-time-based-trends)
8. [Freight & Logistics](#8-freight--logistics)
9. [Marketing Funnel](#9-marketing-funnel)
10. [External Factors (Weather & Holidays)](#10-external-factors)
11. [Key Business Recommendations](#11-key-business-recommendations)

---

## 1. Orders & Delivery Performance

### 1.1 Order Status Distribution

| Status      | Count | Percentage |
|-------------|-------|------------|
| delivered   | 96478 | 97.02%     |
| shipped     | 1107  | 1.11%      |
| canceled    | 625   | 0.63%      |
| unavailable | 609   | 0.61%      |
| invoiced    | 314   | 0.32%      |
| processing  | 301   | 0.30%      |
| created     | 5     | 0.01%      |
| approved    | 2     | 0.00%      |

**Insight:** _["97.02% of orders were successfully delivered, with only 0.63% cancellation rate indicating strong fulfillment operations."]_

### 1.2 Late Delivery Analysis

| Metric                      | Value |
|-----------------------------|-------|
| Total Delivered Orders      | 96478 |
| Late Deliveries             | 7826  |
| Late Delivery Rate          | 8.11% |
| Avg Actual Delivery Days    | 12.09 |
| Avg Estimated Delivery Days | 23.37 |

**Insight:** _["Olist conservatively estimates delivery times at 23.37 days but actually delivers in 12.09 days on average, which is positive for customer satisfaction. However, the 8.11% late delivery rate still impacts reviews significantly."]_

### 1.3 Late Delivery by State (Top 10 Worst)

| State | Total Deliveries | Late Deliveries | Late % |
|-------|------------------|-----------------|--------|
| AL    | 397              | 95              | 23.93% |
| MA    | 717              | 141             | 19.67% |
| PI    | 476              | 76              | 15.97% |
| CE    | 1279             | 196             | 15.32% |
| SE    | 335              | 51              | 15.22% |
| BA    | 3256             | 457             | 14.04% |
| RJ    | 12350            | 1664            | 13.47% |
| TO    | 274              | 35              | 12.77% |
| PA    | 946              | 117             | 12.37% |
| ES    | 1995             | 244             | 12.23% |

**Insight:** _["Northeastern states (AL, MA, PI, CE) have the highest late delivery rates, likely due to logistics infrastructure challenges. These states should have longer estimated delivery windows."]_

**Insight:** _["Northeastern states (AL, MA, PI) have 2-3x higher late delivery rates due to logistics infrastructure challenges. These states should have longer estimated delivery windows."]_

**Business Recommendation:**

- [ ] Increase estimated delivery time for Northern states by X days
- [ ] Partner with regional logistics providers in high-late-rate areas
- [ ] Leverage early delivery advantage in marketing communications to highlight fast service

---

## 2. Revenue & Product Analysis

### 2.1 Top 10 Product Categories by Revenue

| Rank | Category (Portuguese)  | Category (English)    | Items Sold | Revenue (R$) |
|------|------------------------|-----------------------|------------|--------------|
| 1    | beleza_saude           | health_beauty         | 9670       | 1,441,248.07 |
| 2    | relogios_presentes     | watches_gifts         | 5991       | 1,305,541.61 |
| 3    | cama_mesa_banho        | bed_bath_table        | 11115      | 1,241,681.72 |
| 4    | esporte_lazer          | sports_leisure        | 8641       | 1,156,656.48 |
| 5    | informatica_acessorios | computers_accessories | 7827       | 1,059,272.40 |
| 6    | moveis_decoracao       | furniture_decor       | 8334       | 902,511.79   |
| 7    | utilidades_domesticas  | housewares            | 6964       | 778,397.77   |
| 8    | cool_stuff             | cool_stuff            | 3796       | 719,329.95   |
| 9    | automotivo             | auto                  | 4235       | 685,384.32   |
| 10   | ferramentas_jardim     | garden_tools          | 4347       | 584,219.21   |

**Insight:** _["Health & Beauty and Watches lead revenue, together accounting for over 17% of total sales."]_

### 2.2 Order Value Distribution

| Value Bucket | Orders | Percentage |
|--------------|--------|------------|
| < R$50       | 16807  | 17.03%     |
| R$50-100     | 30095  | 30.50%     |
| R$100-200    | 31670  | 32.10%     |
| R$200-500    | 15861  | 16.08%     |
| R$500-1000   | 3085   | 3.13%      |
| > R$1000     | 1148   | 1.16%      |

**Insight:** _["The majority of orders (48.18%) fall in the R$100-500 range, indicating a middle-market customer base with most purchases between R$100-200."]_

### 2.3 Monthly Average Order Value Trend

| Month   | Avg Order Value (R$) |
|---------|----------------------|
| 2017-01 | 173.88               |
| 2017-06 | 156.35               |
| 2017-12 | 153.55               |
| 2018-06 | 166.02               |

**Insight:** _["AOV decreased slightly from 2017 to 2018, suggesting stable pricing or product mix."]_

---

## 3. Customer Analysis

### 3.1 Customer Distribution by State

| State | Customers | Percentage |
|-------|-----------|------------|
| SP    | 41746     | 41.98%     |
| RJ    | 12852     | 12.92%     |
| MG    | 11635     | 11.70%     |
| RS    | 5466      | 5.50%      |
| PR    | 5045      | 5.07%      |
| SC    | 3637      | 3.66%      |
| BA    | 3380      | 3.40%      |
| DF    | 2140      | 2.15%      |
| ES    | 2033      | 2.04%      |
| GO    | 2020      | 2.03%      |

**Insight:** _["São Paulo alone accounts for 41.98% of all customers, reflecting Brazil's economic concentration in the Southeast."]_

### 3.2 Repeat Customer Analysis

| Metric                       | Value |
|------------------------------|-------|
| Total Unique Customers       | 96096 |
| Repeat Customers (2+ orders) | 2997  |
| Repeat Rate                  | 3.12% |

**Insight:** _["Only 3.12% repeat rate is concerning for an e-commerce platform. Industry benchmark is 20-30%. This suggests potential issues with customer satisfaction or competitive alternatives."]_

**Business Recommendation:**

- [ ] Implement loyalty program to increase repeat purchases
- [ ] Analyze churned customers for improvement opportunities

---

## 4. Payment Behavior

### 4.1 Payment Method Distribution

| Payment Type | Transactions | Percentage | Avg Value (R$) |
|--------------|--------------|------------|----------------|
| credit_card  | 76795        | 73.92%     | 163.32         |
| boleto       | 19784        | 19.04%     | 145.03         |
| voucher      | 5775         | 5.56%      | 65.70          |
| debit_card   | 1529         | 1.47%      | 142.57         |

**Insight:** _["Credit card dominates at 73.92%, reflecting Brazilian consumer preference for installment payments."]_

### 4.2 Credit Card Installment Behavior

| Installments | Orders | Avg Payment (R$) |
|--------------|--------|------------------|
| 1            | 25455  | 95.87            |
| 2-3          | 22874  | 134.89           |
| 4-6          | 16257  | 185.77           |
| 7-10         | 11666  | 287.13           |
| 10+          | 313    | 470.13           |

**Insight:** _["Average installment count is 2.8. Higher-value orders tend to use more installments, with 10+ installments averaging R$470.13."]_

### 4.3 Boleto vs Credit Card Comparison

| Metric          | Credit Card     | Boleto         |
|-----------------|-----------------|----------------|
| Total Orders    | 76505           | 19784          |
| Avg Order Value | R$163.32        | R$145.03       |
| Total Revenue   | R$12,542,084.19 | R$2,869,361.27 |

**Insight:** _["Credit card orders are far more common and have a higher average value than boleto orders."]_

---

## 5. Review & Satisfaction

### 5.1 Review Score Distribution

| Score | Reviews | Percentage |
|-------|---------|------------|
| 5 ⭐   | 56910   | 57.83%     |
| 4 ⭐   | 19007   | 19.31%     |
| 3 ⭐   | 8097    | 8.23%      |
| 2 ⭐   | 3114    | 3.16%      |
| 1 ⭐   | 11282   | 11.46%     |

**Average Score:** 4.09 / 5.0

**Insight:** _["77.14% of reviews are 4-5 stars, but the 11.46% of 1-star reviews require attention."]_

### 5.2 Late Delivery Impact on Reviews ⚠️ CRITICAL

| Delivery Status | Orders | Avg Review Score |
|-----------------|--------|------------------|
| On-Time         | 87964  | 4.30             |
| Late            | 7631   | 2.57             |
| **Difference**  | -      | **1.73**         |

**Insight:** _["Late deliveries result in 1.73 star lower reviews on average. This is a critical finding - improving delivery performance directly impacts customer satisfaction."]_

**Business Recommendation:**

- [ ] Prioritize delivery optimization - each 1% reduction in late deliveries could improve avg rating by X
- [ ] Proactive communication for delayed orders to manage expectations

### 5.3 Review Score by Product Category (Top 10)

| Category               | Reviews | Avg Score |
|------------------------|---------|-----------|
| books_general_interest | 548     | 4.45      |
| books_technical        | 263     | 4.36      |
| luggage_accessories    | 1087    | 4.31      |
| food_drink             | 274     | 4.31      |
| fashion_shoes          | 257     | 4.25      |
| food                   | 494     | 4.22      |
| stationery             | 2496    | 4.20      |
| pet_shop               | 1935    | 4.18      |
| computers              | 200     | 4.18      |
| perfumery              | 3407    | 4.17      |

**Insight:** _["Books and luggage/accessories have the highest satisfaction, while lower-rated categories are not shown here."]_

---

## 6. Seller Performance

### 6.1 Top 10 Sellers by Revenue

| Rank | Seller ID                        | Orders | Revenue (R$) | Avg Item Price |
|------|----------------------------------|--------|--------------|----------------|
| 1    | 4869f7a5dfa277a7dca6462dcf3b52b2 | 1132   | 249,640.70   | 198.51         |
| 2    | 7c67e1448b00f6e969d365cea6b010ab | 982    | 239,536.44   | 137.77         |
| 3    | 53243585a1d6dc2643021fd1853d8905 | 358    | 235,856.68   | 543.36         |
| 4    | 4a3ca9315b744ce9f8e9374361493884 | 1806   | 235,539.96   | 100.89         |
| 5    | fa1c13f2614d7b5c4749cbc52fecda94 | 585    | 204,084.73   | 331.13         |
| 6    | da8622b14eb17ae2831f4ac5b9dab84a | 1314   | 185,192.32   | 103.31         |
| 7    | 7e93a43ef30c4f03f38b393420bc753a | 336    | 182,754.05   | 518.92         |
| 8    | 1025f0e2d44d7041d6cf58b6550e0bfa | 915    | 172,860.69   | 97.32          |
| 9    | 7a67c85e85bb2ce8582c35f2203ad736 | 1160   | 162,648.38   | 121.05         |
| 10   | 955fee9216a65b617aa5c0531780ce60 | 1287   | 160,602.68   | 90.17          |

### 6.2 Seller Revenue Concentration

| Tier        | Sellers | Revenue (R$) | % of Total |
|-------------|---------|--------------|------------|
| Top 10      | 10      | 2,028,716.63 | 12.80%     |
| Top 11-100  | 90      | 5,001,659.52 | 31.57%     |
| Top 101-500 | 400     | 5,236,740.55 | 33.05%     |
| Rest        | 2595    | 3,576,436.54 | 22.57%     |

**Insight:** _["Top 100 sellers (3% of total) generate 44.37% of revenue, indicating high seller concentration. Platform is dependent on a small number of key sellers."]_

### 6.3 Seller Distribution by State

| State | Sellers | Percentage |
|-------|---------|------------|
| SP    | 1849    | 59.74%     |
| PR    | 349     | 11.28%     |
| MG    | 244     | 7.88%      |
| SC    | 190     | 6.14%      |
| RJ    | 171     | 5.53%      |
| RS    | 129     | 4.17%      |
| GO    | 40      | 1.29%      |
| DF    | 30      | 0.97%      |
| ES    | 23      | 0.74%      |
| BA    | 19      | 0.61%      |

**Insight:** _["59.74% of sellers are in São Paulo, creating potential logistics advantages for SP customers but challenges for distant states."]_

---

## 7. Time-Based Trends

### 7.1 Monthly Order & Revenue Trend

| Month   | Orders | Revenue (R$) | MoM Growth |
|---------|--------|--------------|------------|
| 2017-01 | 789    | 137,188.49   | -          |
| 2017-06 | 3217   | 502,963.04   | +266%      |
| 2017-12 | 5624   | 863,547.23   | +72%       |
| 2018-06 | 6160   | 1,022,677.11 | +18%       |

**Insight:** _["Revenue grew significantly from 2017 to 2018, with notable spikes in November (Black Friday) and December (Christmas)."]_

### 7.2 Day of Week Pattern

| Day       | Orders | % of Weekly |
|-----------|--------|-------------|
| Monday    | 16196  | 16.29%      |
| Tuesday   | 15963  | 16.05%      |
| Wednesday | 15552  | 15.64%      |
| Thursday  | 14761  | 14.84%      |
| Friday    | 14122  | 14.20%      |
| Saturday  | 10887  | 10.95%      |
| Sunday    | 11960  | 12.03%      |

**Insight:** _["Orders peak on Monday-Tuesday and drop significantly on weekends. Marketing campaigns should target early weekdays."]_

### 7.3 Hour of Day Pattern

| Time Block  | Orders | Pattern  |
|-------------|--------|----------|
| 00:00-06:00 | 4740   | Low      |
| 06:00-12:00 | 22240  | Moderate |
| 12:00-18:00 | 38361  | Peak     |
| 18:00-24:00 | 34100  | High     |

**Insight:** _["Peak ordering hours are 12:00-18:00, suggesting customers browse during lunch and after work."]_

---

## 8. Freight & Logistics

### 8.1 Freight Cost Analysis

| Metric                       | Value  |
|------------------------------|--------|
| Avg Freight % of Order Value | 32.09% |
| Median Freight %             | 23.14% |

### 8.2 Freight by Customer State (Top 10 Highest)

| State | Orders | Avg Freight (R$) |
|-------|--------|------------------|
| RR    | 52     | 42.98            |
| PB    | 602    | 42.72            |
| RO    | 278    | 41.07            |
| AC    | 92     | 40.07            |
| PI    | 542    | 39.15            |
| MA    | 824    | 38.26            |
| TO    | 315    | 37.25            |
| SE    | 385    | 36.65            |
| AL    | 444    | 35.84            |
| PA    | 1080   | 35.83            |

**Insight:** _["Northern states pay significantly more in freight (up to R$42.98 in RR), which combined with longer delivery times creates a worse customer experience."]_

### 8.3 Product Weight Impact

| Weight Category    | Orders | Avg Delivery Days | Avg Freight (R$) |
|--------------------|--------|-------------------|------------------|
| Light (<500g)      | 43829  | 11.30             | 15.17            |
| Medium (500g-2kg)  | 41234  | 12.06             | 17.78            |
| Heavy (2-10kg)     | 19933  | 12.71             | 26.02            |
| Very Heavy (>10kg) | 5183   | 14.91             | 54.25            |

**Insight:** _["Heavy products take 3.6 days longer and cost 3.6x more in freight. Consider this in delivery estimates."]_

---

## 9. Marketing Funnel

### 9.1 Funnel Conversion

| Stage                                 | Count | Conversion Rate |
|---------------------------------------|-------|-----------------|
| MQLs (Marketing Qualified Leads)      | 8000  | 100%            |
| Closed Deals                          | 842   | 10.53%          |
| Linked to E-Commerce (Active Sellers) | 842   | 100.00%         |

**Insight:** _["Only 10.53% of MQLs convert to closed deals, but all closed deals are linked to e-commerce. There's significant drop-off in the onboarding process."]_

### 9.2 Lead Origins

| Origin            | Leads | Percentage |
|-------------------|-------|------------|
| organic_search    | 2296  | 28.70%     |
| paid_search       | 1586  | 19.83%     |
| social            | 1350  | 16.88%     |
| unknown           | 1099  | 13.74%     |
| direct_traffic    | 499   | 6.24%      |
| email             | 493   | 6.16%      |
| referral          | 284   | 3.55%      |
| other             | 150   | 1.88%      |
| display           | 118   | 1.48%      |
| other_publicities | 65    | 0.81%      |
| (blank)           | 60    | 0.75%      |

**Insight:** _["Organic and paid search bring the most leads."]_

### 9.3 Linked vs Non-Linked Deals Comparison

| Status               | Deals | Avg Declared Revenue |
|----------------------|-------|----------------------|
| Linked to E-Commerce | 842   | R$73,377.68          |

**Insight:** _["All deals are linked to e-commerce with average declared revenue of R$73,377.68."]_

---

## 10. External Factors

### 10.1 Weather Impact on Orders

| Weather | Orders | % of Total |
|---------|--------|------------|
| drizzle | 42278  | 42.52%     |
| cloudy  | 31238  | 31.41%     |
| rain    | 22842  | 22.97%     |
| clear   | 3083   | 3.10%      |

**Insight:** _["Weather has minimal correlation with order volume, suggesting Brazilian e-commerce is resilient to weather conditions."]_

### 10.2 Holiday vs Non-Holiday Orders

| Type        | Orders | % of Total |
|-------------|--------|------------|
| Holiday     | 2811   | 2.83%      |
| Non-Holiday | 96630  | 97.17%     |

**Insight:** _["Only 2.83% of orders occur on holidays, which is expected given holidays are ~3% of total days."]_

**Insight:** _["Only 2.83% of orders occur on holidays, which is expected given holidays are ~3% of total days."]_

### 10.3 Monthly Revenue in USD

| Month   | Revenue (BRL)  | Revenue (USD) |
|---------|----------------|---------------|
| 2017-01 | R$137,188.49   | $41,518.24    |
| 2018-06 | R$1,022,677.11 | $267,626.95   |

**Insight:** _["Currency fluctuation affects USD-equivalent revenue, with significant growth in BRL terms."]_

---

## 11. Key Business Recommendations

Based on the analysis, here are the top recommendations:

### 🚚 Delivery & Logistics

1. **Increase delivery estimates for Northern states** - Current estimates are too aggressive, causing late deliveries
2. **Partner with regional carriers** - Reduce dependency on national logistics for remote areas
3. **Weight-based delivery estimates** - Heavy products should have longer delivery windows

### ⭐ Customer Satisfaction

1. **Address late delivery impact** - Each 1% improvement in on-time delivery = 1.73-star improvement in reviews
2. **Proactive delay communication** - Notify customers before delivery is officially "late"
3. **Focus on 1-star review categories** - Identify and fix product/seller issues

### 💰 Revenue Growth

1. **Increase repeat customer rate** - Current 3.12% is below industry benchmark of 20-30%
2. **Promote higher-installment options** - Customers using 10+ installments have 390% higher order values
3. **Expand seller base in high-demand states** - Reduce logistics costs and delivery times

### 📊 Marketing

1. **Optimize campaign timing** - Focus on Monday-Tuesday when order volume peaks
2. **Improve MQL-to-Seller conversion** - Currently only 10.53% of MQLs become active sellers

---

## Appendix A: Data Quality Notes

| Issue                                 | Tables Affected    | Impact                            |
|---------------------------------------|--------------------|-----------------------------------|
| ~610 products missing category        | olist_products     | Cannot categorize X% of revenue   |
| Geolocation duplicates (deduplicated) | olist_geolocation  | 1M → 19K rows                     |
| Missing seller_id in closed_deals     | olist_closed_deals | 0% cannot be linked to e-commerce |

---

## Appendix B: SQL Queries Used

All queries are available in: `scripts/analysis/silver_layer_findings.sql`
