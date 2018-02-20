CREATE TABLE Teams (

	TeamID character varying(3),
	TNAME character varying(100) NOT NULL,
	TCONF character varying(50) NOT NULL,
	TDIV character varying(50) NOT NULL,
	PRIMARY KEY (TeamID),
	CONSTRAINT valid_conference CHECK (TCONF IN ('West','East')),
	CONSTRAINT valid_division CHECK (TDIV IN ('Pacific','Central','Metropolitan','Atlantic'))
);

CREATE TABLE Teams_Stats (

	TSID bigserial NOT NULL,	
	SDATE date NOT NULL,
	TeamID character varying(3),
	GP int,
	W int,
	L int,
	OL int,
	PTS int,
	PTSPER real,
	GF int,
	GA int,
	SOW int,
	SOL int,
	SRS real,
	SOS real,
	TGG real,
	EVGF int,
	EVGA int,
	PP int,
	PPO int,
	PPPER real,
	PPA int,
	PPOA int,
	PKPER real,
	SH int,
	SHA int,
	PIMG real,
	oPIMG real,
	S int,
	SPER real,
	SA int,
	SVPER real,
	PDO real,
	PRIMARY KEY (TSID),
	FOREIGN KEY (TeamID) REFERENCES Teams(TeamID)
);

CREATE TABLE Players (

	PlayerID character varying(10),
	PNAME character varying(100) NOT NULL,
	PTEAM character varying(3),
	PNUM int,
	PCNTRY character varying(2),
	PPOS character varying(2),
	PAGE int,
	HEIGHT character varying(10),
	WEIGHT character varying(10),
	PHAND char,
	PYRSEXP int,
	PBDATE date,
	PSAL real,
	PDRAFT character varying(50),
	PRIMARY KEY (PlayerID)
);

CREATE TABLE Players_Activity (
	
	AID bigserial NOT NULL,	
	ADATE date NOT NULL,
	PlayerID character varying(10) NOT NULL,
	PNAME character varying(100) NOT NULL,
	Position character varying(2) NOT NULL,
	TeamID character varying(3) NOT NULL,
	HomeAway char(1), 
	Opponent character varying(3) NOT NULL,
	Result character varying(4) NOT NULL,
	G int,
	A int,
	PTS int,
	PlusMinus int,
	PIM int,
	EVG int,
	PPG int,
	SHG int,
	GWG int,
	EVA int,
	PPA int,
	SHA int,
	S int,
	SPER real,
	SHFT int,
	TOI time,
	HIT int,
	BLK int,
	FOW int,
	FOL int,
	FOPER real,
	DEC character varying(4) NOT NULL,
    GA int,
	SA int, 
	SV int, 
	SVPER real, 
	SO int,
	PRIMARY KEY (AID),
	FOREIGN KEY (TeamID) REFERENCES Teams(TeamID),
	FOREIGN KEY (Opponent) REFERENCES Teams(TeamID),
	FOREIGN KEY (PlayerID) REFERENCES Players(PlayerID)
);