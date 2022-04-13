--- Non active status contracts with no cancelation date
SELECT
	contract.state,
	contract.data_alta,
	contract.data_baixa,
	contract.active,
	TRUE
FROM giscedata_polissa AS contract
WHERE
	contract.data_baixa is NULL AND
	contract.data_alta is NOT NULL AND
	contract.state != 'activa' AND
	--contract.active is TRUE AND
	TRUE
ORDER BY
	contract.data_alta,
	contract.state,
	TRUE
;
--- Active status contracts with cancellation date
SELECT
	contract.state,
	contract.data_alta,
	contract.data_baixa,
	contract.active,
	TRUE
FROM giscedata_polissa AS contract
WHERE
	contract.data_baixa is NOT NULL AND
	contract.state = 'activa' AND
	TRUE
ORDER BY
	contract.data_alta,
	contract.state,
	TRUE
;
--- contracts marked as inactive
SELECT
	contract.state,
	contract.data_alta,
	contract.data_baixa,
	contract.active,
	TRUE
FROM giscedata_polissa AS contract
WHERE
	contract.active is FALSE AND
	TRUE
ORDER BY
	contract.data_alta,
	contract.state,
	TRUE
