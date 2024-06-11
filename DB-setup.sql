-- TEAM DATACRATS
-- Jin Wu
-- Last Updated: 6/10/2024

-- Entity Sets

CREATE TABLE Filer (
    FilerId INT PRIMARY KEY
);

CREATE TABLE Persons (
    FilerId INT,
    PersonId INT,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    MiddleName VARCHAR(100),
    Suffix VARCHAR(10),
    Prefix VARCHAR(10),
    PRIMARY KEY (FilerId),
    FOREIGN KEY (FilerId) REFERENCES Filer(FilerId)
);

CREATE TABLE Lobbyists (
    FilerId INT,
    PersonId INT,
    EthicsCourseCompleted DATE,
    PRIMARY KEY (FilerId),
    FOREIGN KEY (FilerId) REFERENCES Filer(FilerId)
);

CREATE TABLE Organizations (
    FilerId INT,
    OrgName VARCHAR(100),
    OrgType VARCHAR(100),
    PRIMARY KEY (FilerId),
    FOREIGN KEY (FilerId) REFERENCES Filer(FilerId)
);

CREATE TABLE LobbyingFirms (
    FilerId INT,
    FirmName VARCHAR(255),
    FirmAddress VARCHAR(255),
    DateRegistered DATE,
    PRIMARY KEY (FilerId),
    FOREIGN KEY (FilerId) REFERENCES Filer(FilerId)
);

CREATE TABLE Bills (
    -- From DDDB Bill
    BillId VARCHAR(23),
    BillType VARCHAR(5),
    BillNumber VARCHAR(10),
    BillState VARCHAR(100),
    BillStatus VARCHAR(60),
    BillHouse VARCHAR(100),
    BillSession INT,
    BillUSState VARCHAR(2),
    SessionYear YEAR,
    -- From DDDB BillVersion
    BillTitle TEXT,
    BillDigest MEDIUMTEXT,
    PRIMARY KEY (BillId)
);



-- Relationship Sets

CREATE TABLE HireFirm (
    OrgId INT,
    FirmId INT,
    StartDate DATE,
    EndDate DATE,
    FOREIGN KEY (OrgId) REFERENCES Filer(FilerId),
    FOREIGN KEY (FirmId) REFERENCES Filer(FilerId)
);

CREATE TABLE OrgHireLobbyist (
    OrgId INT,
    LobbyistId INT,
    StartDate DATE,
    EndDate DATE,
    DisclosureForm VARCHAR(255),
    FOREIGN KEY (OrgId) REFERENCES Filer(FilerId),
    FOREIGN KEY (LobbyistId) REFERENCES Filer(FilerId)
);

CREATE TABLE FirmHireLobbyist (
    FirmId INT,
    LobbyistId INT,
    StartDate DATE,
    EndDate DATE,
    DisclosureForm VARCHAR(255),
    FOREIGN KEY (FirmId) REFERENCES Filer(FilerId),
    FOREIGN KEY (LobbyistId) REFERENCES Filer(FilerId)
);

CREATE TABLE InfluenceBill (
    OrgId INT,
    FirmId INT,
    BillId VARCHAR(23),
    Stance VARCHAR(10) CHECK (Stance in ('support', 'oppose')),
    StartDate Date,
    EndDate Date,
    FOREIGN KEY (OrgId) REFERENCES Filer(FilerId),
    FOREIGN KEY (FirmId) REFERENCES Filer(FilerId),
    FOREIGN KEY (BillId) REFERENCES Bills(BillId)
);

