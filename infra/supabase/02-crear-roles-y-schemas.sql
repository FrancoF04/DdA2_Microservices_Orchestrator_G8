-- Paso 2: un rol y un schema por servicio, como en la infra local (infra/postgres/init/).
-- Cada rol es dueño de su schema y lo tiene como search_path, así los servicios que no fijan schema crean sus
-- tablas en el suyo y no en `public`. Se puede volver a correr: si el rol existe, solo le cambia la clave.
--
-- NO correr esta plantilla tal cual: `scripts/preparar_despliegue.py` genera
-- despliegue.local/supabase-roles.sql con las claves reales en lugar de los {{CLAVE_...}}.
--
-- Los servicios entran por el session pooler (puerto 5432) con el usuario `<rol>.<project-ref>`.

DO $$
DECLARE
    fila record;
BEGIN
    FOR fila IN SELECT * FROM (VALUES
        ('orquestador',  '{{CLAVE_DB_ORQUESTADOR}}'),
        ('crm_auth',     '{{CLAVE_DB_AUTH}}'),
        ('leads',        '{{CLAVE_DB_LEADS}}'),
        ('calificacion', '{{CLAVE_DB_CALIFICACION}}'),
        ('clientes',     '{{CLAVE_DB_CLIENTES}}'),
        ('vehiculos',    '{{CLAVE_DB_VEHICULOS}}')
    ) AS s (rol, clave) LOOP
        IF fila.clave LIKE '{{%' THEN
            RAISE EXCEPTION 'Falta la clave del rol %: usar el SQL que genera scripts/preparar_despliegue.py', fila.rol;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = fila.rol) THEN
            EXECUTE format('ALTER ROLE %I LOGIN PASSWORD %L', fila.rol, fila.clave);
        ELSE
            EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', fila.rol, fila.clave);
        END IF;
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION %I', fila.rol, fila.rol);
        EXECUTE format('ALTER ROLE %I SET search_path = %I', fila.rol, fila.rol);
    END LOOP;
END
$$;
