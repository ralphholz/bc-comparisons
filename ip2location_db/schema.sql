CREATE TABLE ip2location_db23(
    ip_from INTEGER NOT NULL,
    ip_to INTEGER NOT NULL,
    country_code TEXT NOT NULL,
    country_name TEXT NOT NULL,
    region_name TEXT NOT NULL,
    city_name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    isp TEXT NOT NULL,
    domain TEXT NOT NULL,
    mcc TEXT(256) NOT NULL,
    mnc TEXT(256) NOT NULL,
    mobile_brand TEXT NOT NULL,
    usage_type TEXT NOT NULL,
    PRIMARY KEY (ip_to, ip_from)
);
