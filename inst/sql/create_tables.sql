CREATE TABLE "x28"."advertisements" (
  "id" text PRIMARY KEY,
  "duplicategroup" uuid,
  "company_id" integer,
  "origin" text,
  "created" timestamp,
  "updated" timestamp,
  "deleted" timestamp,
  "language" text,
  "qualityscore" integer,
  "workquota_minimum" integer,
  "workquota_maximum" integer,
  "temporary" boolean,
  "homeoffice" boolean,
  "url" text,
  "domain" text,
  "company_url" text,
  "company_domain" text,
  "company_name" text,
  "company_uid" text,
  "company_crn" text,
  "company_recruitment_agency" boolean,
  "company_size_id" integer,
  "company_size_name" text,
  "company_size_min" integer,
  "company_size_max" integer
);

CREATE TABLE "x28"."advertisement_details" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "title" text,
  "raw_text" text
);

CREATE TABLE "x28"."advertisement_positions" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "position" text
);

CREATE TABLE "x28"."advertisement_education_levels" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "education" text
);

CREATE TABLE "x28"."advertisement_country" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "country" text
);

CREATE TABLE "x28"."advertisement_canton" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "canton" text
);

CREATE TABLE "x28"."advertisement_postalcode" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "postalcode" integer
);

CREATE TABLE "x28"."advertisement_metadata" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "metadata_id" text,
  "type" text,
  "source" text,
  "name" text,
  "level" text,
  "phrase" text
);

CREATE TABLE "x28"."company_metadata" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "company_id" integer,
  "metadata_id" integer,
  "type" text,
  "name" text
);

CREATE TABLE "x28"."company_addresses" (
  "advertisement_id" text REFERENCES x28.advertisements(id),
  "company_id" integer,
  "country" text,
  "postalcode" text,
  "city" text,
  "primary" boolean
);

CREATE TABLE "x28"."filtered_advertisements" (
  "id" text REFERENCES x28.advertisements(id),
  "created" timestamp,
  "deleted" timestamp,
  "from_portal" boolean
);

CREATE TABLE "x28"."event_log" (
  "t" timestamp DEFAULT CURRENT_TIMESTAMP,
  "event" text NOT NULL,
  "details" text DEFAULT NULL
);
