CREATE TABLE IF NOT EXISTS pilotatge_kpis_description (
    id serial PRIMARY KEY,
    name VARCHAR ( 70 ) UNIQUE NOT NULL,
    description VARCHAR ( 700 ),
    filter VARCHAR ( 1000 ),
    erp_model VARCHAR ( 50 ),
    context VARCHAR ( 50 ),
    field VARCHAR ( 50 ),
    function VARCHAR ( 50 ),
    freq VARCHAR ( 50 ),
    type_value VARCHAR ( 50 ),
    teams VARCHAR ( 50 ),
    create_date timestamp with time zone
);

CREATE TABLE IF NOT EXISTS pilotatge_int_kpis (
    kpi_id INTEGER,
    value BIGINT NOT NULL,
    create_date timestamp with time zone,
    --CONSTRAINT fk_pilotatge_kpi
        --FOREIGN KEY(kpi_id)
            --REFERENCES pilotatge_kpis_description(id)
);

CREATE TABLE IF NOT EXISTS pilotatge_float_kpis (
    kpi_id INTEGER,
    value DECIMAL (100,5) NOT NULL,
    create_date timestamp with time zone,
    --CONSTRAINT fk_pilotatge_kpi
        --FOREIGN KEY(kpi_id)
            --REFERENCES pilotatge_kpis_description(id)
);