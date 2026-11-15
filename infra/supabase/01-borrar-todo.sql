-- Paso 1 (DESTRUCTIVO): borra los schemas y los roles de los servicios del CRM, con todos sus datos.
-- Correr en el SQL Editor de Supabase recién después de revisar 00-ver-que-hay.sql (ver docs/09 §3).
--
-- Solo toca los schemas y roles de esta lista. Si 00-ver-que-hay.sql mostró otros nombres de un despliegue
-- anterior (por ejemplo `orquestador_app`), agregarlos a la lista antes de correrlo. No toca `public` ni los
-- schemas propios de Supabase.
--
-- OJO con `auth`: Supabase ya tiene un schema `auth` (el de su servicio de autenticación) y NO hay que borrarlo.
-- Por eso el servicio auth del CRM usa el schema y el rol `crm_auth` (ver DB_SCHEMA en render.yaml). Si un
-- despliegue anterior creó tablas del CRM dentro de `auth`, borrarlas una por una, nunca el schema.

DO $$
DECLARE
    nombre text;
BEGIN
    FOREACH nombre IN ARRAY ARRAY[
        'orquestador', 'crm_auth', 'leads', 'calificacion', 'clientes', 'vehiculos'
    ] LOOP
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', nombre);
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = nombre) THEN
            -- Lo que el rol tenga en otros schemas (permisos, objetos sueltos) se va con él.
            EXECUTE format('DROP OWNED BY %I CASCADE', nombre);
            EXECUTE format('DROP ROLE %I', nombre);
        END IF;
    END LOOP;
END
$$;
