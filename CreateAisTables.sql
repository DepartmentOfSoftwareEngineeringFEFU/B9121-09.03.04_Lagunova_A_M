CREATE DATABASE aisstream;
USE aisstream;

CREATE TABLE DynamicData (
    UserID INT NOT NULL,
    Cog FLOAT,
    Sog FLOAT,
    Latitude FLOAT NOT NULL,
    Longitude FLOAT NOT NULL,
    Datetime DATETIME NOT NULL,
    PRIMARY KEY (UserID, Datetime)
);

CREATE TABLE StaticData (
    UserID INT NOT NULL,
    A INT, 
    B INT, 
    C INT, 
    D INT, 
    ImoNumber INT,
    Type INT,
    Datetime DATETIME NOT NULL,
    PRIMARY KEY (UserID, Datetime)
);

CREATE TABLE Aquatories (
    id INT AUTO_INCREMENT PRIMARY KEY, 
    name VARCHAR(255),
    right_top_lat FLOAT NOT NULL,
    left_bottom_lat FLOAT NOT NULL,
    right_top_lon FLOAT NOT NULL,
    left_bottom_lon FLOAT NOT NULL
);

CREATE TABLE Aquatories_vessels (
    aquatory_id INT,
    vessel_id INT,
    Datetime DATETIME NOT NULL,
    PRIMARY KEY (aquatory_id, vessel_id, Datetime),
    FOREIGN KEY (aquatory_id) REFERENCES Aquatories(id) ON DELETE CASCADE,
    FOREIGN KEY (vessel_id) REFERENCES DynamicData(UserID) ON DELETE CASCADE
);