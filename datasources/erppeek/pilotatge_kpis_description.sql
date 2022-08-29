CREATE TABLE pilotatge_kpis_description (
    id serial PRIMARY KEY,
    name VARCHAR ( 50 ) UNIQUE NOT NULL,
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
	id, name, description, filter, erp_model, context, field, function, freq, type_value, teams, create_date)
	VALUES
        (1, 'Número de factures pendents', 'Num factures amb deute',  '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("date_due", "<", "__today__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (2, 'Saldo pendent_a', 'Import de les factures amb deute (sumen els "pendent" de les factures amb deute)',  '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0)]', 'giscedata.facturacio.factura','{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        (3, 'Saldo pendent_b', 'Import de les extra lines de les factures amb Descripcio = fracció. Aquest resultat és el que queda pendent dels fraccionaments', '[("name", "ilike", "%fraccio"), ("active", "=", "TRUE")]' , 'giscedata.facturacio.extra', '{"active_test": False}', 'amount_pending', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        (4, 'Advocats: Import', 'Import de les factures que estan en advocats amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%advocats")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        (5, 'Advocats: Numero factures', 'Num de factures que estan en advocats amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%advocats")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (6, 'R1: Import', 'Import de les factures que estan en R1 amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%r1")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'int', 'Cobraments', NOW()),
        (7, 'R1: Número de factures', 'Num de factures que estan en R1 amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%r1")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (8, 'Pobresa: Import', 'Import de les factures que estan en pobresa energètica amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%energetica")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'int', 'Cobraments', NOW()),
        (9, 'Pobresa: Número de factures', 'Num de factures que estan en pobresa energètica amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%energetica")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (10, 'Fraccionament: Import_a', 'Import de les extra lines de les factures amb Descripcio = fracció. Aquest resultat és el que queda pendent dels fraccionaments', '[("name", "ilike", "%fraccio"), ("active", "=", "TRUE")]' , 'giscedata.facturacio.extra', '{"active_test": False}', 'amount_pending', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        (11, 'Fraccionament: Import_b', 'Import de les factures que estan en estat pendent "fracció"', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%fraccio")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'int', 'Cobraments', NOW()),
        (12, 'Fraccionament: Número de factures_b', 'Num de factures que estan en pobresa energètica amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%fraccio")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (13, 'Indic Procés - Total F1s amb error', 'F1s amb error dimportació', '[("state", "=", "Error en la importació")]', 'giscedata.facturacio.importacio.linia', '', '', 'count', 'daily', 'int', 'Factura', NOW())
        (14, 'Indic Procés - Total pòlisses amb incidència', '', '[("state", "=", "Error en la importació")]', 'giscedata.facturacio.lot', '', '', 'count', 'daily', 'int', 'Factura', NOW())

;



-- A Factures Client amb deute (giscedata.facturacio.factura)
    -- 'Pendent' = 'signed_residual' o 'residual'
    -- 'Total' = 'amount_total' o 'signed_amount_total' o 'saldo'


TRUNCATE TABLE pilotatge_int_kpis, pilotatge_float_kpis, pilotatge_kpis_description;

-- Taula de KPIs Pilotatge
SELECT pkd.name, pkd.description, pfk.value, pfk.create_date
FROM pilotatge_kpis_description as pkd
LEFT JOIN pilotatge_float_kpis as pfk
ON pkd.id = pfk.kpi_id
where type_value = 'float'
UNION
SELECT pkd.name, pkd.description, pik.value, pik.create_date
FROM pilotatge_kpis_description as pkd
LEFT JOIN pilotatge_int_kpis as pik
ON pkd.id = pik.kpi_id
where type_value = 'int'
