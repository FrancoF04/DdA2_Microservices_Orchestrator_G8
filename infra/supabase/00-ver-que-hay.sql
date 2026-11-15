-- Paso 0 (solo lectura): qué schemas, roles y tablas hay hoy en Supabase, antes de borrar nada.
-- Correr en el SQL Editor de Supabase y revisar el resultado (ver docs/09 §3).

SELECT nspname AS schema, pg_get_userbyid(nspowner) AS duenio
FROM pg_namespace
WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema'
ORDER BY 1;

SELECT rolname AS rol, rolcanlogin AS puede_entrar
FROM pg_roles
WHERE rolname NOT LIKE 'pg\_%'
ORDER BY 1;

SELECT table_schema AS schema, count(*) AS tablas
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
GROUP BY 1
ORDER BY 1;
