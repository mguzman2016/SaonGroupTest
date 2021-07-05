
CREATE TABLE dt_advert (
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

ALTER TABLE dt_advert ADD CONSTRAINT dt_advert_pk PRIMARY KEY ( advertid );

CREATE TABLE dt_company (
    companyid    INTEGER IDENTITY(1,1) NOT NULL,
    firmidcrc32  VARCHAR(4000),
    city         VARCHAR(4000),
    company      VARCHAR(4000),
    sector       VARCHAR(4000)
);

ALTER TABLE dt_company ADD CONSTRAINT dt_company_pk PRIMARY KEY ( companyid );

CREATE TABLE dt_date (
    dateid    INTEGER IDENTITY(1,1) NOT NULL,
    day_date  VARCHAR(4000),
    day       INTEGER,
    month     INTEGER,
    year      INTEGER
);

ALTER TABLE dt_date ADD CONSTRAINT dt_date_pk PRIMARY KEY ( dateid );

CREATE TABLE dt_date_advert (
    dateadvertid  INTEGER IDENTITY(1,1) NOT NULL,
    day_date      VARCHAR(4000),
    day           INTEGER,
    month         INTEGER,
    year          INTEGER
);

ALTER TABLE dt_date_advert ADD CONSTRAINT dt_date_advert_pk PRIMARY KEY ( dateadvertid );

CREATE TABLE ft_applicants (
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

ALTER TABLE ft_applicants ADD CONSTRAINT ft_applicants_pk PRIMARY KEY ( applicantid );
