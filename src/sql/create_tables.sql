CREATE TABLE hamsterwheel(
    hamsterwheel_id INT NOT NULL AUTO_INCREMENT,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    flag TINYINT(1) NOT NULL,
    PRIMARY KEY ( hamsterwheel_id )
);

CREATE TABLE session(
    session_id INT NOT NULL AUTO_INCREMENT,
    hamsterwheel_id_start INT NOT NULL,
    start_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    end_time TIMESTAMP(6),
    end_type VARCHAR(50),
    PRIMARY KEY ( session_id ),
    FOREIGN KEY ( hamsterwheel_id_start ) REFERENCES hamsterwheel( hamsterwheel_id )
);

CREATE TABLE wallet(
    wallet_id INT NOT NULL AUTO_INCREMENT,
    currency_symbol VARCHAR(50) NOT NULL,
    amount FLOAT NOT NULL,
    PRIMARY KEY ( wallet_id )
);

CREATE TABLE tradebook(
    tradebook_id INT NOT NULL AUTO_INCREMENT,
    session_id INT,
    direction VARCHAR(50) NOT NULL,
    currency_symbol VARCHAR(50) NOT NULL,
    amount FLOAT NOT NULL,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY ( tradebook_id ),
    FOREIGN KEY ( session_id ) REFERENCES session( session_id )
);

CREATE TABLE decision(
    decision_id INT NOT NULL AUTO_INCREMENT,
    session_id INT,
    type VARCHAR(50) NOT NULL,
    result VARCHAR(50),
    start_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    end_time TIMESTAMP(6),
    hamsterwheel_id_start INT NOT NULL,
    hamsterwheel_id_end INT,
    wheel_turns INT,
    PRIMARY KEY ( decision_id ),
    FOREIGN KEY ( hamsterwheel_id_start ) REFERENCES hamsterwheel( hamsterwheel_id ),
    FOREIGN KEY ( hamsterwheel_id_end ) REFERENCES hamsterwheel( hamsterwheel_id )
);

INSERT INTO decision (session_id, type, hamsterwheel_id_start) VALUES (1, 'test', 5);

UPDATE decision SET type = 'test2' WHERE session_id = 1;
UPDATE decision SET end_time = "2022-06-24 00:09:51.123456" WHERE decision_id = 1;

TRUNCATE TABLE decision;
DROP TABLE decision;