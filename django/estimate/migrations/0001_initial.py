from django.db import migrations


class Migration(migrations.Migration):
    initial = True

    dependencies = [
    ]

    operations = [
        migrations.RunSQL(
            sql='''
                    CREATE FUNCTION count_estimate(query text) RETURNS integer AS $$
                    DECLARE
                        rec   record;
                        rows  integer;
                    BEGIN
                        FOR rec IN EXECUTE 'EXPLAIN ' || query LOOP
                            rows := substring(rec."QUERY PLAN" FROM ' rows=([[:digit:]]+)');
                            EXIT WHEN rows IS NOT NULL;
                        END LOOP;

                        RETURN rows;
                    END;
                    $$ LANGUAGE plpgsql VOLATILE STRICT;
                    ''',
            reverse_sql='DROP FUNCTION IF EXISTS count_estimate;'
        )
    ]
