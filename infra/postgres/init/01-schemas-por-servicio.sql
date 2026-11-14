-- Imita la organización de Supabase: una sola base (`postgres`) con un schema
-- y un rol por servicio. Cada rol es dueño de su schema y lo tiene como
-- search_path, así los servicios que no fijan schema (ej. leads) crean sus
-- tablas en el suyo y no en `public`.
--
-- Credenciales SOLO para desarrollo local: usuario = contraseña = nombre del servicio.
-- Corre una única vez, cuando el volumen de datos está vacío.

DO $$
DECLARE
    servicio text;
BEGIN
    FOREACH servicio IN ARRAY ARRAY[
        'leads', 'calificacion', 'vehiculos', 'clientes',
        'monitor', 'orquestador', 'auth', 'multas'
    ] LOOP
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', servicio, servicio);
        EXECUTE format('CREATE SCHEMA %I AUTHORIZATION %I', servicio, servicio);
        EXECUTE format('ALTER ROLE %I SET search_path = %I', servicio, servicio);
    END LOOP;
END
$$;
