## Suffolk County Deed Records

**Partially.** The records are public but there is no bulk download API.

The county clerk has an online search portal at https://clerk.suffolkcountyny.gov/kiosk/ but it is a one-parcel-at-a-time lookup. You cannot download everything at once.

Your options for getting this data:

1. **FOIL Request.** File a Freedom of Information Law request with the Suffolk County Clerk for a data extract of all recorded deed transfers in the Town of Brookhaven. This is free or low cost. They may provide a CSV or database export. Processing takes days to weeks. This is the best free option for bulk sale price and date data.

2. **Scrape the clerk portal.** Technically possible but the site likely has terms of service prohibiting it and may use CAPTCHAs or rate limiting. Not recommended.

3. **NYS ACRIS equivalent.** NYC has ACRIS for deed records online. Suffolk County does not have an equivalent public bulk download. The statewide sales datasets on data.ny.gov that we tested are gone.

## Town of Brookhaven Assessor

**Partially.** The assessor data (year built, bedrooms, bathrooms, square footage, stories) is public record but not available as a bulk API.

The Town of Brookhaven uses the Suffolk County Real Property Tax Service Agency. You can look up individual parcels but there is no public API or bulk download.

Your options:

1. **FOIL Request.** File a FOIL request with the Town of Brookhaven Assessor's Office for the full assessment roll with property detail fields. Assessors maintain detailed property characteristic data including year built, building square footage, number of bedrooms, number of bathrooms, number of stories, garage type, heating type, and condition. This data exists internally. A FOIL request is the legal way to get it in bulk. Contact them at:

```
Town of Brookhaven Assessor's Office
1 Independence Hill
Farmingville, NY 11738
Phone: (631) 451-6302
```

2. **Suffolk County RPTS.** The Real Property Tax Service Agency maintains the tax rolls for all of Suffolk County. They may respond to a FOIL request faster than the individual town.

```
Suffolk County Real Property Tax Service Agency
Phone: (631) 852-2900
```

3. **Annual assessment roll.** New York State law requires every municipality to publish its tentative and final assessment roll each year. The roll must be available for public inspection. Ask the assessor for the current roll in electronic format.

## MLS / IDX Feed

**No, not without a license.** MLS data (active listings, listing photos, listing price, agent info, days on market) is private and controlled by the local MLS.

The MLS covering Brookhaven is:

```
OneKey MLS (formerly MLSLI - MLS of Long Island)
https://www.onekeymls.com
```

To get IDX or RETS/RESO data feed access you must:

1. Be a licensed real estate agent or broker in New York State, or
2. Be a licensed vendor/developer working under a broker's account

You cannot get MLS data without a real estate license or a formal agreement with a licensed broker. There is no workaround for this.

If you have or can get a license, the process is:

```
1. Get your NY real estate salesperson or broker license
2. Join a local board of realtors (Long Island Board of Realtors)
3. Subscribe to OneKey MLS
4. Apply for IDX or RESO Web API data feed access
5. Sign the IDX license agreement
6. Build your site according to their display rules
```

If you do not want to get licensed, your options are:

1. **Partner with a licensed broker** who can sponsor your IDX access. You build the site, they provide the data feed under their license. Revenue split or flat fee arrangement.

2. **Use a third-party IDX provider** like IDX Broker, Showcase IDX, or iHomefinder. These companies have existing MLS agreements and provide widgets or APIs you can embed. They require you to work with a licensed agent. Monthly fees range from $50 to $150.

3. **Skip active listings entirely.** Your MVP focuses on seller leads using public assessment data. You do not need MLS data to run "What is your home worth?" campaigns. Many successful lead generation sites work with public records only.

## Summary

```
Source                          Bulk Access    Free    How
────────────────────────────────────────────────────────────────
Suffolk County deed records     FOIL request   Yes     Ask county clerk for data extract
Brookhaven assessor details     FOIL request   Yes     Ask assessor for full roll with details
MLS active listings             License only   No      Need RE license or broker partner
```

## Recommended Next Step

File two FOIL requests. Here are draft letters you can send:

**FOIL Request 1: Deed Records**

```
To: Suffolk County Clerk
Re: Freedom of Information Law Request

I am requesting a copy of all recorded deed transfers
for properties located in the Town of Brookhaven,
Suffolk County, New York, for the period January 1, 2015
through the present date.

For each transfer I am requesting:
- Tax map parcel ID (section-block-lot)
- Property address
- Sale date
- Sale price
- Grantor name (seller)
- Grantee name (buyer)
- Deed type
- Book and page number

I request this data in electronic format (CSV, Excel,
or database export) if available.

Please advise of any fees prior to processing.

Thank you.

[Your name]
[Your address]
[Your email]
[Your phone]
[Date]
```

**FOIL Request 2: Assessor Detail**

```
To: Town of Brookhaven Assessor's Office
1 Independence Hill
Farmingville, NY 11738

Re: Freedom of Information Law Request

I am requesting a copy of the current final assessment
roll for the Town of Brookhaven with full property
characteristic detail for all parcels.

For each parcel I am requesting:
- Tax map parcel ID
- Property address
- Property class code and description
- Owner name
- Assessed value (land, total, and full market)
- Year built
- Building square footage (gross living area)
- Number of bedrooms
- Number of bathrooms (full and half)
- Number of stories
- Lot size (acres or square feet)
- Garage type and capacity
- Heating type
- Building condition
- Sale date and sale price (if maintained on roll)

I request this data in electronic format (CSV, Excel,
or database export) if available.

Please advise of any fees prior to processing.

Thank you.

[Your name]
[Your address]
[Your email]
[Your phone]
[Date]
```

Under New York FOIL law, agencies must respond within 5 business days acknowledging the request and must provide the records within a reasonable time. Electronic records in existing formats should be provided at minimal cost.

Once you receive the data, drop the files into `data/raw/` and add them as `local_csv` sources in `pipeline.py`. The normalizer will pick up the new fields automatically if you add the column names to `FIELD_MAPPINGS`.