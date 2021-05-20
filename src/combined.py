#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

# Playground file to run & debug different code snippets.
from pprint import pprint

from functools import partial
def add(a,b,c):
    return 100*a

## MVP Demo

# Create session and connect

conf = {"url": "mludf.preprod3.int.snowflakecomputing.com",
        "account": "mludf",
        "port": 8085,
        'user': '',
        'password': '',
        'protocol': 'https'
        }

# conf = {"url": "snowflake.test19.int.snowflakecomputing.com",
#         "port": 8084,
#         'account': 'snowflake',
#         'user': 'jdu',
#         'password': '',
#         'protocol': 'http',
#         'role': 'accountadmin',
#         'ssl': 'off'}

#####

from src.snowflake.snowpark.PSession import PSession
p_session = PSession(conf, False, True)

# Setup VWH and schemas
p_session.sql('use warehouse JAVA_UDF_ARIMA_WAREHOUSE').collect()
output = p_session.sql('select current_warehouse()').collect()
print(output)


df = p_session.range(1, 10, 2).select('ID').filter('id > 4')
output = df.collect()
print(output)


df2 = p_session.table('JAVA_UDF_RPOC_DATABASE.JAVA_UDF_RPOC_SCHEMA.AIRPASSENGERS')

output = df2.filter("month >= '1950'").select(['Passengers']).collect()
print(output)

df_pax = df2.filter("month >= '1950'").to_df(['d', 'pax'])
output = df_pax.select(['d', 'pax']).collect()
print(output)

output = df_pax.drop('d').collect()
print(output)

# test different types -- run a debugger
df3 = p_session.table('JAVA_UDF_RPOC_DATABASE.JAVA_UDF_RPOC_SCHEMA.TEST_DataType').collect()
res = [[r.get(i) for i in range(r.length())] for r in df3]
pprint(res)

## End of MVP demo








#####################################################################
#####################################################################

#####################################################################
# Try dataframes with Python plans

p_session = PSession(conf, False, False)

# Setup VWH and schemas
p_session.sql('use warehouse JAVA_UDF_ARIMA_WAREHOUSE').collect()
output = p_session.sql('select current_warehouse()').collect()
print(output)


df = p_session.range(1, 10, 2).select('ID').filter('id > 4')
output = df.collect()
print(output)


df2 = p_session.table('JAVA_UDF_RPOC_DATABASE.JAVA_UDF_RPOC_SCHEMA.AIRPASSENGERS')

output = df2.filter("month >= '1950'").select(['Passengers']).collect()
print(output)

output = df2.filter("month >= '1950'").drop('month').collect()
print(output)

df_pax = df2.filter("month >= '1950'").to_df(['d', 'pax'])
output = df_pax.select(['d', 'pax']).collect()
print(output)

output = df_pax.drop('d').collect()
print(output)

# test different types -- run a debugger
df3 = p_session.table('JAVA_UDF_RPOC_DATABASE.JAVA_UDF_RPOC_SCHEMA.TEST_DataType').collect()
res = [[r.get(i) for i in range(r.length())] for r in df3]
pprint(res)

#####################################################################

from py4j.java_gateway import JavaGateway, GatewayParameters, java_import

def ref_scala_object(jvm, object_name):
    clazz = jvm.java.lang.Class.forName(object_name + "$")
    ff = clazz.getDeclaredField("MODULE$")
    o = ff.get(None)
    return o


def getSession(jvm):
    config = jvm.java.util.HashMap()
    config["URL"] = conf['url'] + ':' + str(conf['port'])
    config["USER"] = conf['user']
    config["PASSWORD"] = conf['password']
    session = jvm.com.snowflake.snowpark.Session.builder().configs(config).create()
    return session

def results_to_python_lists(res):
    return [[r.get(i) for i in range(r.length())] for r in res]


gateway = JavaGateway(gateway_parameters=GatewayParameters(port=25335, auto_convert=True))
entry_point = gateway.entry_point
jvm = gateway.jvm
session = getSession(jvm)

# In lieu of imports
java_import(jvm, 'com.snowflake.snowpark.functions.col')
java_import(jvm, 'com.snowflake.snowpark.Column')
col = gateway.jvm.col
java_import(jvm, 'import scala.collection.JavaConverters.*')

# Setup VWH and schemas
session.sql('use warehouse JAVA_UDF_ARIMA_WAREHOUSE').collect()
res = session.sql('select current_warehouse()').collect()
print(res.toString())

# Get refs to scala-side Dataframes
scala_df = session.range(1, 10, 2)
scala_df2 = session.table('JAVA_UDF_RPOC_DATABASE.JAVA_UDF_RPOC_SCHEMA.AIRPASSENGERS')

# Collect scala_df and place values in python-list
res = scala_df.collect()
df_v1 = [r.get(0) for r in res]
df_v2 = [[r.get(i) for i in range(r.length())] for r in res]
print(df_v1)
print(df_v2)

res = scala_df2.collect()
df2_v2 = [[r.get(i) for i in range(r.length())] for r in res]
print(df2_v2)

# Get column names from scala_df
schema = scala_df.schema()
namesSeq = schema.names().toSeq()
names = [namesSeq.apply(i) for i in range(namesSeq.size())]
# Try to filter based on the first column
# expr = jvm.org.apache.spark.sql.catalyst.expressions.Literal(names[0]+"> 5")
Column_ref = ref_scala_object(jvm, 'com.snowflake.snowpark.Column')
c1 = Column_ref.expr(names[0])
c2 = Column_ref.expr(names[0] + "> 5")

# col = jvm.com.snowflake.snowpark.Column(names[0] + "> 5")
res = scala_df.select(c1).filter(c2).collect()
filter_output = [[r.get(i) for i in range(r.length())] for r in res]

######
# This fails:
# res = session.sql("select 1; select 2; ").collect()
res = session.sql("select 1;").collect()
output = [[r.get(i) for i in range(r.length())] for r in res]


########################################
# A few more complicated operations on DFs
# "SELECT" was problematic with existing implementation
# Required changes:
# 1. adding new definition that takes as input java...List<String>
# TODO: revisit for more meaningful select statements
l = gateway.jvm.java.util.ArrayList()
l.append('month')
res = scala_df2.select(l).collect()

# Same as above, with auto Python->Java tranformation for lists
res = scala_df2.select(['month']).collect()
output = [[r.get(i) for i in range(r.length())] for r in res]

########################################
# Get queries from DataFrame.
#analyzer = session.getAnalyzer()
#plan = scala_df2.getPlan()
#q = analyzer.resolve(plan).queries().toList()
def get_queries(session, df):
    scala_list = session.getAnalyzer().resolve(df.getPlan()).queriesAsStrings()
    java_list = jvm.scala.collection.JavaConverters.seqAsJavaList(scala_list)
    return [el for el in java_list]

#q2 = get_queries(session, scala_df2)


# Required changes:
# Function in Session
def get_queries2(session, df):
    scala_list = session.queries(df)
    java_list = jvm.scala.collection.JavaConverters.seqAsJavaList(scala_list)
    return [el for el in java_list]

q = get_queries2(session, scala_df2)


# # Join Dataframes
res = scala_df2.join(scala_df2).collect()
output = results_to_python_lists(res)



# ########################################################
print("##########\n\tUSE PSession")
# Use PSession
from snowflake.snowpark.PSession import PSession

p_session = PSession(conf, True, True)

# Set current warehouse and get current_warehouse()
p_session.sql('use warehouse JAVA_UDF_ARIMA_WAREHOUSE').collect()
res = p_session.sql('select current_warehouse()').collect()
print("Current wareshouse is:" + str(res))

# Get refs to scala-side Dataframes
p_df = p_session.range(1, 10, 2)
# Collect scala_df and place values in python-list
res = p_df.collect()
df_v2 = [[r.get(i) for i in range(r.length())] for r in res]
print(df_v2)

res = p_session.table('JAVA_UDF_RPOC_DATABASE.JAVA_UDF_RPOC_SCHEMA.AIRPASSENGERS').collect()
df2_v2 = [[r.get(i) for i in range(r.length())] for r in res]
print(df2_v2)

########################################3
# DO the same, but using the python connector for the wire protocol
print("Using python connector")
p_session = PSession(conf, False, True)

# Set current warehouse and get current_warehouse()
p_session.sql('use warehouse JAVA_UDF_ARIMA_WAREHOUSE').collect()
res = p_session.sql('select current_warehouse()').collect()
print("Current wareshouse is:" + str(res))

# Get refs to scala-side Dataframes
p_df = p_session.range(1, 10, 2)
res = p_df.collect()
print(res)

res = p_session.table('JAVA_UDF_RPOC_DATABASE.JAVA_UDF_RPOC_SCHEMA.AIRPASSENGERS').collect()
df_output_v2 = [[r.get(i) for i in range(r.size())] for r in res]
print(df_output_v2)


# Try range variations
# Get refs to scala-side Dataframes
p_df = p_session.range(1, 10, 1)
res = p_df.collect()
print(res)

p_df = p_session.range(1, 10)
res = p_df.collect()
print(res)

p_df = p_session.range(10)
res = p_df.collect()
print(res)

p_df = p_session.range(1, 10, 2)
res = p_df.collect()
print(res)


pass
