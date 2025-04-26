CREATE OR REPLACE FUNCTION core_energy.refresh_v_series_wide()
RETURNS void AS
$$
DECLARE
    col_list text;
BEGIN
    SELECT string_agg(
        format('    MAX(value) FILTER (WHERE d.model_col = %L) AS %I', model_col, model_col),
        E',\n'
        ORDER BY model_col)
    INTO col_list
    FROM core_energy.dim_series;

    EXECUTE '
        CREATE OR REPLACE VIEW core_energy.v_series_wide AS
        SELECT
            v.obs_date::date AS date,
' || col_list || '
        FROM core_energy.fact_series_value v
        JOIN core_energy.fact_series_meta m USING (series_id)
        JOIN core_energy.dim_series d ON d.series_code = m.series_code
        GROUP BY v.obs_date
        ORDER BY v.obs_date';
END;
$$ LANGUAGE plpgsql;
