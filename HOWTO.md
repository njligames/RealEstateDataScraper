## Using This Data to Generate Leads and Close Deals

### Direct Mail to Homeowners

The dataset gives you every property owner's name and mailing address in Brookhaven. You can run targeted direct mail campaigns.

**High equity homeowners.** Filter for properties where the full market value is significantly higher than when they likely purchased. Owners who bought 15 to 20 years ago are sitting on large equity gains and may be ready to sell.

```
psql $DATABASE_URL -c "
SELECT address, city, zip, owner_name, full_market_value, assessed_value
FROM properties
WHERE assessed_value > 400000
ORDER BY assessed_value DESC
LIMIT 100
"
```

**Vacant land owners.** Property class codes in the 300s are vacant land. These owners often want to sell but have no urgency. A letter offering to list or buy can get a response.

```
psql $DATABASE_URL -c "
SELECT address, city, zip, owner_name, full_market_value, property_class
FROM properties
WHERE property_class LIKE '3%'
ORDER BY full_market_value DESC
"
```

**Absentee owners.** When the mailing address city or state is different from the property location, the owner does not live there. Absentee owners are more likely to sell, especially if they inherited the property or moved away.

```
psql $DATABASE_URL -c "
SELECT p.address, p.city, p.zip, p.owner_name, p.assessed_value,
       r.raw_data->>'mailing_address_city' as mail_city,
       r.raw_data->>'mailing_address_state' as mail_state
FROM properties p
JOIN raw_records r ON r.raw_data->>'print_key_code' = p.parcel_id
WHERE r.raw_data->>'mailing_address_state' != 'NY'
   OR r.raw_data->>'mailing_address_city' NOT IN ('Patchogue','Medford','Shirley','Mastic','Centereach','Selden','Coram')
LIMIT 100
"
```

**Multi-family and investment property owners.** Property class 210 is single family, 220 is two family, 230 is three family, 280 is multi-purpose residential. Two and three family owners are often investors open to selling.

```
psql $DATABASE_URL -c "
SELECT address, city, zip, owner_name, assessed_value, property_class
FROM properties
WHERE property_class IN ('220','230','240','280')
ORDER BY assessed_value DESC
"
```

### Website Lead Capture

Build property pages that rank in Google for searches like "123 Main St Patchogue NY" or "Patchogue home values." When homeowners search their own address they find your page and see a call to action.

**What each page should have:**

- The property address as the page title
- Assessed value and estimated market value
- Lot size and property classification
- A prominent button saying "Get Your Free Home Valuation"
- A form asking for name, email, phone, and timeline
- Your photo, name, and license number

**Area pages** for each hamlet should show average assessed values, number of properties, and a call to action like "Thinking of selling in Patchogue? See what your home is worth."

Every form submission creates a lead in the `leads` table. Set up email notifications so you can respond within minutes.

### Farming a Neighborhood

Pick a specific area or ZIP code and become the expert there. Use the data to create monthly market updates.

For example, for Patchogue 11772:

```
psql $DATABASE_URL -c "
SELECT
  COUNT(*) as total_properties,
  ROUND(AVG(assessed_value)) as avg_assessed,
  ROUND(AVG(full_market_value)) as avg_market_value,
  MIN(full_market_value) as min_value,
  MAX(full_market_value) as max_value
FROM properties
WHERE zip = '11772'
  AND assessed_value > 0
"
```

Send a postcard or email newsletter to every homeowner in that ZIP saying something like "The average home in Patchogue is now assessed at $X. Wondering what yours is worth? Call me."

### Expired and FSBO Prospecting

Cross-reference the property data with expired MLS listings from your OneKey MLS access. When a listing expires, look up the owner name and mailing address in your database. Send a personalized letter that references their specific property details.

### Pre-Listing Research

Before a listing appointment, pull everything you have on the property:

- Assessment and market value
- Lot dimensions
- Property classification
- Owner name and mailing address
- Nearby property values for a quick comparable analysis

```
psql $DATABASE_URL -c "
SELECT address, assessed_value, full_market_value, lot_size, property_class
FROM properties
WHERE latitude BETWEEN 40.76 AND 40.78
  AND longitude BETWEEN -73.02 AND -73.00
ORDER BY full_market_value DESC
LIMIT 20
"
```

Walk into the appointment with a printed report showing the property details and surrounding values. This shows professionalism and preparation.

### Investor Outreach

Identify owners who hold multiple properties. They are active investors and may want to buy or sell.

```
psql $DATABASE_URL -c "
SELECT owner_name, COUNT(*) as properties, SUM(assessed_value) as total_value
FROM properties
WHERE owner_name IS NOT NULL
  AND owner_name != ''
GROUP BY owner_name
HAVING COUNT(*) > 3
ORDER BY COUNT(*) DESC
LIMIT 50
"
```

These multi-property owners are high-value contacts. They trade properties frequently, they know the market, and they often work with one agent for all transactions.

### Identifying Development Opportunities

Find large vacant lots in desirable areas:

```
psql $DATABASE_URL -c "
SELECT address, city, zip, owner_name, lot_size, full_market_value
FROM properties
WHERE property_class LIKE '3%'
  AND lot_size > 10000
ORDER BY lot_size DESC
LIMIT 50
"
```

Connect developers with landowners. You earn commission on the land sale and potentially on the new homes built.

### What to Do Once FOIL Data Arrives

When you receive the assessor detail with bedrooms, bathrooms, square footage, and year built, your property pages become much more useful. Homeowners will see their actual home details and think "this site knows my property" which builds trust and increases form submissions.

When you receive the deed transfer records with sale prices and dates, you can show actual sale history on each property page. You can calculate real appreciation rates. You can identify owners who bought recently (not likely to sell) versus those who bought long ago (more likely to sell).

Add both files to the pipeline:

```
# Copy the files into the data directory
cp assessor_detail.csv data/raw/brookhaven_assessor.csv
cp deed_records.csv data/raw/brookhaven_deeds.csv
```

Add them to DATA_SOURCES in pipeline.py as local_csv entries, add the new column names to FIELD_MAPPINGS, and re-run:

```
python pipeline.py --normalize --geocode
```

### Daily Workflow

```
Morning:
  Check leads table for overnight form submissions
  Respond to every lead within 15 minutes by phone

Weekly:
  Pull absentee owner list for one ZIP code
  Send 50 handwritten notes or postcards
  Post area stats to social media

Monthly:
  Generate market update for your farm area
  Email newsletter to all captured leads
  Review which property pages get the most traffic
  File any new FOIL requests for updated data

Quarterly:
  Re-run pipeline to refresh assessment data
  Update property pages with new valuations
  Expand to additional ZIP codes or hamlets
```

### Cost to Implement This

```
Item                              Cost
────────────────────────────────────────
This pipeline and data            Free
FOIL requests                     Free or minimal
Domain name                       $12/year
Hosting (small VPS)               $5-20/month
Direct mail printing/postage      $0.50-1.00 per piece
MLS IDX feed (through your MLS)   Included with MLS dues
Email marketing tool              $0-30/month
Total first year                  Under $500
```

Compare that to buying leads from Zillow at $20 to $50 per lead or paying for a Realtor.com zip code at $300 to $1000 per month. This system generates leads you own permanently, from a database you control, at near zero marginal cost per lead.