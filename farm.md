# Choosing and Working a Farm Area

This guide explains how to use the pipeline data to choose the right zipcode
to farm, interpret the metrics produced by `analyze_farm.py`, and turn that
data into a lead generation system.

---

## What Is Farming?

Farming means choosing a specific geographic area — usually a zipcode or
neighborhood — and becoming the dominant listing agent there through
consistent marketing, local expertise, and community presence. Done well, you
become the name homeowners think of first when they decide to sell.

The goal is not to win every deal in a zip immediately. It is to build name
recognition and trust over 12–24 months so that your conversion rate on
listings in that area is significantly higher than any competitor's.

---

## Choosing the Right Zipcode

Run the analysis script to get a ranked table of every Brookhaven zipcode:

    python analyze_farm.py

The output is sorted by **Commission Opportunity Score** — the single best
number to lead with when comparing zips. The sections below explain each
metric and how to weigh it.

---

## The Metrics

### Turnover Rate (most important)

    Turnover Rate = Homes Sold Per Year ÷ Total Homes in Zip

This is the single most important number. It tells you what percentage of
homes in the area actually change hands each year. A high turnover rate means
more transactions and more opportunities to earn commissions regardless of
your market share.

What to look for:

- 7–10%+ is strong. One in ten homeowners is selling each year.
- 3–5% is average. Workable, but you need other factors in your favor.
- Below 3% is slow. The best marketing in the world cannot manufacture
  transactions that owners do not want to make.

The script annualises the rate automatically, so results from a 24-month
lookback window are directly comparable to a 12-month window.

### Average and Median Sale Price

Higher prices mean higher commissions per transaction. A zip with 8% turnover
at $300,000 generates more total commission volume than a zip with 8% turnover
at $200,000, all else being equal.

Use the median rather than the average when the two diverge significantly — a
few very high-value sales can pull the average up and make a market look more
lucrative than it really is for typical listings.

### Days on Market

Shorter days on market means faster closings and faster income. It also
signals stronger buyer demand, which makes your listings easier to sell and
your sellers happier. This field is populated once MLS data is connected.

### Active Agents and Agent-to-Listing Ratio

    Agent-to-Listing Ratio = Active Agents ÷ Homes Sold in Period

This measures how saturated the market is with competing agents. A ratio above
1.0 means more agents are competing than there are listings — a crowded market.
A ratio below 0.5 means you could realistically capture a significant share
of the available transactions.

A zip dominated by one veteran agent with deep community ties will be much
harder to crack than a zip with the same turnover but fragmented agent activity.
These fields are populated once MLS data is connected.

### Commission Opportunity Score

    Score = (Turnover Rate × Avg Sale Price × Total Homes × 3%) ÷ Active Agents

This estimates your dollar opportunity per competing agent — the total
commission pool in the zip divided by the number of agents competing for it.
Sort by this column to find underserved, high-value markets.

Before MLS data is connected, the score divides by 1 instead of the actual
agent count, which overstates it. Use it as a relative ranking between zips
rather than an absolute dollar figure until agent data is available.

---

## The Ideal Zipcode

Look for a zip that combines:

- High turnover rate (7%+ means plenty of transactions)
- Moderate to high price point (healthy commissions per deal)
- Low agent competition (room to become the dominant name)
- Manageable size (500–2,000 homes so you can reach everyone affordably)

Avoid chasing the highest prices alone. A zip with $2M homes but 2% turnover
gives you very few deals per year. A zip with $450,000 homes and 9% turnover
at low competition is a better business.

---

## Working the Farm

Once you have chosen a zip, consistency over 12–24 months is what builds
market share. The data in this pipeline supports several outreach strategies.

### Direct Mail

The `properties` table contains every owner's name and mailing address.
Segment your mailings by owner type for better response rates.

High equity owners — likely bought 15+ years ago, sitting on large gains:

    psql $DATABASE_URL -c "
    SELECT address, city, zip, owner_name, full_market_value, assessed_value
    FROM properties
    WHERE zip = '11772'
      AND assessed_value > 400000
    ORDER BY assessed_value DESC;
    "

Absentee owners — mailing address zip or state differs from property location:

    psql $DATABASE_URL -c "
    SELECT p.address, p.city, p.zip, p.owner_name,
           p.assessed_value, p.full_market_value,
           r.raw_data->>'mailing_address_city'  AS mail_city,
           r.raw_data->>'mailing_address_state' AS mail_state,
           r.raw_data->>'mailing_address_zip'   AS mail_zip
    FROM properties p
    JOIN raw_records r ON r.raw_data->>'print_key_code' = p.parcel_id
    WHERE p.zip = '11772'
      AND (
        r.raw_data->>'mailing_address_state' != 'NY'
        OR (
          r.raw_data->>'mailing_address_zip' IS NOT NULL
          AND r.raw_data->>'mailing_address_zip' != ''
          AND LEFT(r.raw_data->>'mailing_address_zip', 5) != p.zip
        )
      )
    ORDER BY p.address;
    "

Multi-family and investor owners:

    psql $DATABASE_URL -c "
    SELECT address, city, zip, owner_name, assessed_value, property_class
    FROM properties
    WHERE zip = '11772'
      AND property_class IN ('220', '230', '240', '280')
    ORDER BY assessed_value DESC;
    "

### Market Update Postcards

Pull area statistics monthly and send a postcard to every homeowner in the
zip. Quote the average assessed value, total homes, and how many sold
recently. Homeowners respond to local numbers about their own neighborhood.

    psql $DATABASE_URL -c "
    SELECT
        COUNT(*)                              AS total_properties,
        ROUND(AVG(assessed_value))            AS avg_assessed,
        ROUND(AVG(full_market_value))         AS avg_market_value,
        COUNT(*) FILTER (
            WHERE last_sale_date >= NOW() - INTERVAL '12 months'
        )                                     AS sold_last_year
    FROM properties
    WHERE zip = '11772'
      AND assessed_value > 0;
    "

### Pre-Listing Research

Before any listing appointment, pull full details on the property and its
neighbors for a quick comparable analysis:

    psql $DATABASE_URL -c "
    SELECT address || ', ' || city || ', ' || state || ' ' || zip AS full_address,
           assessed_value, full_market_value, lot_size, property_class
    FROM properties
    WHERE latitude  BETWEEN 40.76 AND 40.78
      AND longitude BETWEEN -73.02 AND -73.00
    ORDER BY full_market_value DESC
    LIMIT 20;
    "

### Multi-Property Owners

Owners with multiple properties are active investors. They trade frequently,
know the market, and often work with one agent for all transactions:

    psql $DATABASE_URL -c "
    SELECT owner_name, COUNT(*) AS properties, SUM(assessed_value) AS total_value
    FROM properties
    WHERE zip = '11772'
      AND owner_name IS NOT NULL
    GROUP BY owner_name
    HAVING COUNT(*) > 2
    ORDER BY COUNT(*) DESC
    LIMIT 30;
    "

---

## When FOIL Data Arrives

Once you receive the assessor detail file (bedrooms, bathrooms, square footage,
year built), your outreach becomes far more specific. You can reference a
homeowner's actual property details in a letter — "your 3-bedroom colonial
built in 1987" — which builds credibility and increases response rates.

Once deed transfer records arrive, you can calculate real appreciation rates
per zip and identify owners who bought at the bottom of the last cycle. Those
owners have the most equity and are the most likely to sell.

See the FOIL section in README.md for how to load these files into the pipeline.

---

## When MLS Data Is Connected

With active listing data flowing in, the agent competition metrics become live.
You can see exactly which agents are active in your target zip, how many
listings they carry, and how long their listings sit on market compared to
yours. This lets you refine your pitch — if the dominant agent in the zip has
an average DOM of 45 days and you average 28, that is a concrete value
proposition you can put in a letter.

---

## Suggested Weekly Workflow

**Daily:** Check the leads table for overnight form submissions. Respond within
15 minutes by phone — speed is the single biggest driver of lead conversion.

**Weekly:** Pull the absentee owner list for your zip. Send 25–50 handwritten
notes or postcards. Post a local market stat to social media.

**Monthly:** Generate a market update for your farm area. Email it to all
captured leads. Review which property pages get the most traffic.

**Quarterly:** Re-run `analyze_farm.py` to see if a neighboring zip has moved
up the rankings. Refresh assessment data with `python pipeline.py --pull --normalize`.
