-- DAY
BEGIN;
INSERT INTO meff_closing_prices
(
	select
		'Day ' || TO_CHAR(dia, 'FMDD-Mon-YYYY') as price_date,
		base_precio,
		base_dif,
		base_dif_per,
		punta_precio,
		punta_dif,
		punta_dif_per,
		NULL as emission_date,
		request_time as create_time
	from meff_precios_cierre_dia
)
COMMIT;

-- MONTH:
BEGIN;
INSERT INTO meff_closing_prices
(
	select
		'Month ' || TO_CHAR(dia, 'Mon-YYYY') as price_date,
		base_precio,
		base_dif,
		base_dif_per,
		punta_precio,
		punta_dif,
		punta_dif_per,
		NULL as emission_date,
		request_time as create_time
	from meff_precios_cierre_mes
)
COMMIT;