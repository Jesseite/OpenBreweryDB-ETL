CREATE TABLE IF NOT EXISTS open_brewery_db.brewery
(
    id character varying(60) COLLATE pg_catalog."default" NOT NULL,
    name character varying(120) COLLATE pg_catalog."default",
    brewery_type character varying(60) COLLATE pg_catalog."default",
    address_1 character varying(60) COLLATE pg_catalog."default",
    address_2 character varying(60) COLLATE pg_catalog."default",
    address_3 character varying(60) COLLATE pg_catalog."default",
    city character varying(60) COLLATE pg_catalog."default",
    state_province character varying(60) COLLATE pg_catalog."default",
    postal_code "char",
    country character varying(60) COLLATE pg_catalog."default",
    longitude numeric(120,0),
    latitude numeric(120,0),
    phone character varying(60) COLLATE pg_catalog."default",
    website_url character varying(120) COLLATE pg_catalog."default",
    state character varying(60) COLLATE pg_catalog."default",
    street character varying(60) COLLATE pg_catalog."default",
    CONSTRAINT brewery_pkey PRIMARY KEY (id)
)