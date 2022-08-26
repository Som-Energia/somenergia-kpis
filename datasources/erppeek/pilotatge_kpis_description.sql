CREATE TABLE pilotatge_kpis_description (
    id serial PRIMARY KEY,
    name VARCHAR ( 50 ) UNIQUE NOT NULL,
    description VARCHAR ( 700 ),
    filter VARCHAR ( 1000 ),
    erp_model VARCHAR ( 50 ),
    field VARCHAR ( 50 ),
    function VARCHAR ( 50 ),
    freq VARCHAR ( 50 ),
    type_value VARCHAR ( 50 ),
    teams VARCHAR ( 50 ),
    create_date timestamp with time zone
);

CREATE TABLE pilotatge_int_kpis (
    kpi_id INTEGER,
    value BIGINT NOT NULL,
    create_date timestamp with time zone,
    CONSTRAINT fk_pilotatge_kpi
        FOREIGN KEY(kpi_id) 
            REFERENCES pilotatge_kpis_description(id)
);

CREATE TABLE pilotatge_float_kpis (
    kpi_id INTEGER,
    value DECIMAL (100,5) NOT NULL,
    create_date timestamp with time zone,
    CONSTRAINT fk_pilotatge_kpi
        FOREIGN KEY(kpi_id) 
            REFERENCES pilotatge_kpis_description(id)
);

INSERT INTO pilotatge_kpis_description(
	id, name, description, filter, erp_model, field, function, freq, type_value, teams, create_date)
	VALUES
        (1, 'Número de factures pendents', 'num factures amb deute',  '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0)]', 'giscedata.facturacio.factura', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (2, 'Saldo pendent_a', 'import de les factures amb deute (sumen els "pendent" de les factures amb deute)',  '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0)]', 'giscedata.facturacio.factura', 'residual', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        (3, 'Saldo pendent_b', 'import de les extra lines de les factures amb Descripcio = fracció. Aquest resultat és el que queda pendent dels fraccionaments', '[("name", "ilike", "%fraccio"), ("active", "=", "TRUE")]' , 'giscedata.facturacio.extra', 'amount_pending', 'sum', 'daily', 'float', 'Cobraments', NOW())
        (4, 'Advocats: Import', 'Import de les factures que estan en advocats', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%advocats")]', 'giscedata.facturacio.factura', 'residual', 'sum', 'daily', 'float', 'Cobraments', NOW()
        );





-- A Factures Client amb deute (giscedata.facturacio.factura)
    -- 'Pendent' = 'signed_residual' o 'residual'
    -- 'Total' = 'amount_total' o 'signed_amount_total' o 'saldo'


TRUNCATE TABLE pilotatge_int_kpis, pilotatge_float_kpis, pilotatge_kpis_description;
