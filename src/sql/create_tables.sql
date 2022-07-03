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
    buy_sell VARCHAR(50) NOT NULL,
    currency_symbol VARCHAR(50) NOT NULL,
    cash_amount FLOAT NOT NULL,
    ccy_amount FLOAT NOT NULL,
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


CREATE TABLE TEST_hamsterwheel(
    hamsterwheel_id INT NOT NULL AUTO_INCREMENT,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    flag TINYINT(1) NOT NULL,
    PRIMARY KEY ( hamsterwheel_id )
);

CREATE TABLE TEST_session(
    session_id INT NOT NULL AUTO_INCREMENT,
    hamsterwheel_id_start INT NOT NULL,
    start_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    end_time TIMESTAMP(6),
    end_type VARCHAR(50),
    PRIMARY KEY ( session_id ),
    FOREIGN KEY ( hamsterwheel_id_start ) REFERENCES hamsterwheel( hamsterwheel_id )
);

CREATE TABLE TEST_wallet(
    wallet_id INT NOT NULL AUTO_INCREMENT,
    currency_symbol VARCHAR(50) NOT NULL,
    amount FLOAT NOT NULL,
    PRIMARY KEY ( wallet_id )
);

CREATE TABLE TEST_tradebook(
    tradebook_id INT NOT NULL AUTO_INCREMENT,
    session_id INT,
    buy_sell VARCHAR(50) NOT NULL,
    currency_symbol VARCHAR(50) NOT NULL,
    cash_amount FLOAT NOT NULL,
    ccy_amount FLOAT NOT NULL,
    time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY ( tradebook_id ),
    FOREIGN KEY ( session_id ) REFERENCES session( session_id )
);

CREATE TABLE TEST_decision(
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

-- Give the hamster 10k USD to start
INSERT INTO TEST_wallet (currency_symbol, amount) VALUES ("CASH", 10000.0);
INSERT INTO TEST_wallet (currency_symbol, amount) VALUES ("BTC", 9999.0);