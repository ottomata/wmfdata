import os
import socket
import prestodb
import pandas

def run(
    sql,
    presto_catalog='analytics_hive',
    presto_host='an-coord1001.eqiad.wmnet',
    presto_port=8281,
    presto_http_scheme='https',
    presto_user=os.getenv('USER'),
    presto_isolation_level=1,  #Allows us to cancel queries
    presto_source='presto-python-client - {}'.format(os.getenv('USER')),
    presto_auth=prestodb.auth.KerberosAuthentication(
        config='/etc/krb5.conf',
        service_name='presto',
        principal='{}@WIKIMEDIA'.format(os.getenv('USER')),
        ca_bundle='/etc/presto/ca.crt.pem',
    )
):
    """
    Runs a Presto SQL query and returns the result in a Pandas DataFrame.
    Presto is an abstracted in  memory query engine, and can be connected
    to many different backend data stores AKA catalogs.
    'analytics_hive' is the default catalog, so you should be able to
    Run queries against the analytics Hive database just like you would
    via Hive.  To be precise, the tables are named <catalog>.<database>.<table>,
    e.g. 'analytics_hive.event.mediawiki_revision_create'.

    Example:
        from wmfdata.presto import run as presto
        edit_count_per_db_sql = "SELECT database, count(*) FROM analytics_hive.event.mediawiki_revision_create WHERE year=2020 and month=2 and day=23 and hour=0 group by database order by count(*) desc limit 10"
        pandas_df = presto(edit_count_per_db_sql)

    """

    presto_connection = prestodb.dbapi.connect(
        catalog=presto_catalog,
        host=presto_host,
        port=presto_port,
        http_scheme=presto_http_scheme,
        user=presto_user,
        isolation_level=presto_isolation_level,
        auth=presto_auth,
    )

    presto_cursor = presto_connection.cursor()
    presto_cursor.execute(sql)

    # Fetch and convert all results to pandas.  Make sure to limit your queries!
    # Taken from: https://github.com/prestodb/presto-python-client/issues/56#issuecomment-367432438
    query_results = presto_cursor.fetchall()
    colnames = [part[0] for part in presto_cursor.description]
    presto_cursor.cancel()
    presto_connection.close()

    pdf = pandas.DataFrame(query_results, columns=colnames)
    return pdf
