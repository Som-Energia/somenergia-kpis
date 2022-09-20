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
    CONSTRAINT fk_pilotatge_kpi
        FOREIGN KEY(kpi_id)
            REFERENCES pilotatge_kpis_description(id)
);

CREATE TABLE IF NOT EXISTS pilotatge_float_kpis (
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
        (1, 'Número de factures pendents', 'Num factures amb deute',  '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (2, 'Saldo pendent_a', 'Import de les factures amb deute (sumen els "pendent" de les factures amb deute)',  '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura','{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        (3, 'Saldo pendent_b', 'Import de les extra lines de les factures amb Descripcio = fracció. Aquest resultat és el que queda pendent dels fraccionaments', '[("name", "ilike", "%fraccio"), ("active", "=", "TRUE")]' , 'giscedata.facturacio.extra', '{"active_test": False}', 'amount_pending', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        -- (4, 'Advocats: Import', 'Import de les factures que estan en advocats amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%advocats"), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        -- (5, 'Advocats: Numero factures', 'Num de factures que estan en advocats amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%advocats"), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (6, 'R1: Import', 'Import de les factures que estan en R1 amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%r1"), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'int', 'Cobraments', NOW()),
        (7, 'R1: Número de factures', 'Num de factures que estan en R1 amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%r1"), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (8, 'Pobresa: Import', 'Import de les factures que estan en pobresa energètica amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%energetica"), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'int', 'Cobraments', NOW()),
        (9, 'Pobresa: Número de factures', 'Num de factures que estan en pobresa energètica amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%energetica"), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (10, 'Fraccionament: Import_a', 'Import de les extra lines de les factures amb Descripcio = fracció. Aquest resultat és el que queda pendent dels fraccionaments', '[("name", "ilike", "%fraccio"), ("active", "=", "TRUE")]' , 'giscedata.facturacio.extra', '{"active_test": False}', 'amount_pending', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        (11, 'Fraccionament: Import_b', 'Import de les factures que estan en estat pendent "fracció"', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%fraccio"),("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'int', 'Cobraments', NOW()),
        (12, 'Fraccionament: Número de factures', 'Num de factures que estan en fraccionament', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%fraccio"),("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        (23, 'Monitori: Import', 'Import de les factures que estan en R1 amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%monitori"), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'int', 'Cobraments', NOW()),
        (24, 'Monitori: Número de factures', 'Num de factures que estan en R1 amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("pending_state", "ilike", "%monitori"), ("date_due", "<=", "__7_days_ago__" )]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW()),
        -- (13, 'Indic Procés - Total F1s amb error', 'F1s amb error dimportació', '[("state", "=", "erroni")]', 'giscedata.facturacio.importacio.linia', '{}', '', 'count', 'daily', 'int', 'Factura', NOW()),
        -- (14, 'Indic Procés - Total pòlisses amb incidència', 'Polisses en lot de facturació actiu, que tenen "state" = "facturat_incident', '[("state", "=", "facturat_incident")]', 'giscedata.facturacio.contracte_lot', '{"active_test": False}', '', 'count', 'daily', 'int', 'Factura', NOW()),
        -- (15, 'Indic Procés - Pòlisses amb incidència (7 dies o més)', 'Polisses en lot de facturació actiu, que tenen "state" = "facturat_incident i que la data demissió de la factura (data factura) es dels últims 7 dies', '[("state", "=", "facturat_incident"),("date_invoice","<=","__7_days_ago__")]', 'giscedata.facturacio.contracte_lot', '{"active_test": False}', '', 'count', 'daily', 'int', 'Factura', NOW()),
        -- (16, 'Indic Procés - Factures emeses (dia anterior)', 'Factures emeses el dia anterior a avui (ahir)', '[("type", "in", ["out_invoice", "out_refund"]),("date_invoice","=","__yesterday__"),("state","!=","draft")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Factura', NOW()),
        -- (17, 'Indic Procés - Import facturat (dia anterior)*', 'Importo total de les factures emeses el dia anterior a avui (ahir)', '[("type", "in", ["out_invoice", "out_refund"]),("date_invoice","=","__yesterday__"),("state","!=","draft")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'amount_total', 'sum', 'daily', 'int', 'Factura', NOW()),
        -- (18, 'Indic CACs - Total CACs oberts/pendents', 'Total CACs oberts o pendents', '[("section_id","ilike","%factura"),("state","in",["pending","open"])]', 'giscedata.atc', '{}', '', 'count', 'daily', 'int', 'Factura', NOW()),
        (19, 'Rebuts retornats: número', 'Número de rebut retornats ahir', '["&", ("account_id.name","=","DIARI DEVOLUCIONS REBUTS"), "&", ("date","=","__3_days_ago__"), "|", ("ref","ilike","FE%"), "|", ("ref","ilike","RE%"), ("ref","ilike","AB%")]', 'account.move.line', '{}', '', 'count', 'daily', 'int', 'Cobrament', NOW()),
        (20, 'Rebuts retornats: import', 'Import dels rebuts retornats ahir', '["&", ("account_id.name","=","DIARI DEVOLUCIONS REBUTS"), "&", ("date","=","__3_days_ago__"), "|", ("ref","ilike","FE%"), "|", ("ref","ilike","RE%"), ("ref","ilike","AB%")]', 'account.move.line', '{}', 'amount_to_pay', 'sum', 'daily', 'float', 'Cobrament', NOW()),
        -- (21, 'Indic CACs - Total R1 03 pendents de gestió (oberts)', 'Total CACs amb filtre STATUS = obert, pas = 03, seccio = factura', '[("state", "=", "open"), ("process_step", "=", "03"),("section_id","ilike","%factura")]', 'giscedata.atc', '{}', '', 'count', 'daily', 'int', 'Factura', NOW()),
        -- (22, 'Indic CACs - Total R1 02 (oberts) pendents de gestió', 'Total CACs amb filtre STATUS = obert, pas = 02, seccio = factura', '[("state", "=", "open"), ("process_step", "=", "02"),("section_id","ilike","%factura")]', 'giscedata.atc', '{}', '', 'count', 'daily', 'int', 'Factura', NOW()),
        (25, 'Factures impagades de contractes de baixa: Import', 'Import de les factures que estan en impagades amb data venciment avui', '[("type", "=", "out_invoice"),   ("invoice_id.pending_state.weight", ">", 0), ("date_due", "<=", "__7_days_ago__" ), "|", ("pending_state", "ilike", "%advocats"), ("pending_state", "ilike", "%tugesto")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', 'residual', 'sum', 'daily', 'float', 'Cobraments', NOW()),
        (26, 'Factures impagades de contractes de baixa: Número de factures', 'Num de factures que estan impagades amb data venciment avui', '[("type", "=", "out_invoice"), ("invoice_id.pending_state.weight", ">", 0), ("date_due", "<=", "__7_days_ago__" ), "|", ("pending_state", "ilike", "%advocats"), ("pending_state", "ilike", "%tugesto")]', 'giscedata.facturacio.factura', '{"type":"out_invoice"}', '', 'count', 'daily', 'int', 'Cobraments', NOW())

-- FACTURES EN PROCEDIMENT DE TALL:  import = 2 - 25 - 6 - 8 - (10+11) - 23
-- FACTURES EN PROCEDIMENT DE TALL:  numero = 1 - 26 - 7 - 9 - (12) - 24

