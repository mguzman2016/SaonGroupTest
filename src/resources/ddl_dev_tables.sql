
CREATE TABLE dev.tmp_adverts_dt (
    advertid             INTEGER IDENTITY(1,1) NOT NULL,
    dateadvertid         INTEGER,
    publicationdatetime  VARCHAR(4000),
    id                   VARCHAR(4000),
    activedays           VARCHAR(50),
    applyurl             VARCHAR(4000),
    status               VARCHAR(4000),
    benefits             VARCHAR(4000),
    title                VARCHAR(4000)
);

ALTER TABLE dev.tmp_adverts_dt ADD CONSTRAINT tmp_dt_advert_pk PRIMARY KEY ( advertid );

CREATE TABLE dev.tmp_companies_dt (
    companyid    INTEGER IDENTITY(1,1) NOT NULL,
    firmidcrc32  VARCHAR(4000),
    city         VARCHAR(4000),
    company      VARCHAR(4000),
    sector       VARCHAR(4000)
);

ALTER TABLE dev.tmp_companies_dt ADD CONSTRAINT tmp_dt_company_pk PRIMARY KEY ( companyid );

CREATE TABLE dev.tmp_dates_dt (
    dateid    INTEGER NOT NULL,
    day_date  VARCHAR(4000),
    day       INTEGER,
    month     INTEGER,
    year      INTEGER
);

ALTER TABLE dev.tmp_dates_dt ADD CONSTRAINT tmp_dt_date_pk PRIMARY KEY ( dateid );

CREATE TABLE dev.tmp_dates_advert_dt (
    dateadvertid  INTEGER NOT NULL,
    day_date      VARCHAR(4000),
    day           INTEGER,
    month         INTEGER,
    year          INTEGER
);

ALTER TABLE dev.tmp_dates_advert_dt ADD CONSTRAINT tmp_dt_date_advert_pk PRIMARY KEY ( dateadvertid );

CREATE TABLE dev.tmp_fact_tbl (
    applicantid      INTEGER IDENTITY(1,1) NOT NULL,
    companyid        INTEGER,
    advertid         INTEGER,
    dateid           INTEGER,
    applicationdate  VARCHAR(4000),
    age              INTEGER,
    postingid        VARCHAR(4000),
    firstname        VARCHAR(4000),
    lastname         VARCHAR(4000),
    skills           VARCHAR(4000)
);

ALTER TABLE dev.tmp_fact_tbl ADD CONSTRAINT tmp_ft_applicants_pk PRIMARY KEY ( applicantid );



