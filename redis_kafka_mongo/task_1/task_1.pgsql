CREATE FUNCTION get_index_data(index_name text, output_table_name text)
RETURNS void AS $$
DECLARE
	create_table_query text;
	create_table_name text;
	csv_column_names text[];
	csv_headers text;
	col_name text;
	new_column_type text;
	new_column_type_suffix text;
BEGIN
	
END;
$$ LANGUAGE plpgsql;



--  Получение имени таблицы по имени индекса к которой он относится
-- SELECT * 
SELECT t.relname
FROM pg_class i
INNER JOIN pg_index idx ON i.oid = idx.indexrelid
INNER JOIN pg_class t ON t.oid = idx.indrelid
WHERE i.relname = 'idx_v1';



-- Установка расширения pageinspect


-- SELECT * FROM pg_available_extensions WHERE name = 'pageinspect';
-- CREATE EXTENSION pageinspect;




-- Получение информации о b-tree index по имени (есть поле level)

-- SELECT * FROM bt_metap('idx_v1');