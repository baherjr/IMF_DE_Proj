CREATE TABLE imf_values(
	country VARCHAR(40),
	indicator VARCHAR(40),
	year INT,
	value INT
);

CREATE TABLE imf_indicators(
	id VARCHAR(40),
	title TEXT,
	dexcription TEXT,
	units TEXT,
	scale TEXT
);