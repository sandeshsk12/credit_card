from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement



cloud_config= {
        'secure_connect_bundle': 'secure-connect-ineuron.zip'
}
auth_provider = PlainTextAuthProvider('FfdDwAlGnqNUwQSlnZHEXhZg', '-WT.2BcPIAquAyRmrPQrI6-DT3IydQ1nT,IRTkorOWI1pUsxhZNYbxJMYRqFXjg+TFpB0Iox7fkfw74Nqpo9cWSp-OgmQ57CFYJb0OCiHDr7_79ULfWcNHEN3G+4tued')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

session = cluster.connect()

session.execute('create table if not exists ccdp.training_database  \
                                ( ID float,LIMIT_BAL float, SEX float, EDUCATION float, MARRIAGE float, AGE float, PAY_0 float, \
                                PAY_2 float, PAY_3 float, PAY_4 float, PAY_5 float, PAY_6 float, BILL_AMT1 float, BILL_AMT2 float, \
                                BILL_AMT3 float, BILL_AMT4 float, BILL_AMT5 float, BILL_AMT6 float, PAY_AMT1 float, \
                                PAY_AMT2 float, PAY_AMT3 float, PAY_AMT4 float, PAY_AMT5 float, PAY_AMT6 float, Defaulted float, primary key(ID))')

session.execute("insert into ccdp.training_database (ID,LIMIT_BAL,SEX,EDUCATION, \
    MARRIAGE,AGE,PAY_0,PAY_2,PAY_3,PAY_4,PAY_5,PAY_6,BILL_AMT1,BILL_AMT2,BILL_AMT3,BILL_AMT4,BILL_AMT5, \
        BILL_AMT6,PAY_AMT1,PAY_AMT2,PAY_AMT3,PAY_AMT4,PAY_AMT5,PAY_AMT6,Defaulted) VALUES \
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
            ,[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1])
print(list(session.execute('select * from ccdp."training_database"')))