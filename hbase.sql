--1.hbase已经有表了，想要通过hive形式去分析里面的数据，通过sql对hbase表做统计分析
create external table h_tab(rowkey string,age1 string,name string,age2 string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,col_f1:age,col_f1:name,col_f2:age")
tblproperties("hbase.table.name"="tab1");
--hive sql进行统计分析的时候，建表只有元数据信息，没有增加数据
--在建表时，不一定将hbase所有的column都放到要统计的hive里面，只需要对统计需求需要用到的字段建表
--想要用全部字段，有很多null
create external table all_h_tab(rowkey string,col_f1 map<string,string>,col_f2 map<string,string>)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,col_f1:,col_f2:")
tblproperties("hbase.table.name"="tab1");

--2.另一种hive关联hbase
--建立hive表，关联hbase，插入数据到hive表中同时能影响hbase表
create table h_orders(order_id string,user_id string,order_dow string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,id:order,num:dow")
tblproperties("hbase.table.name"="hive.orders");

--不兼容,hive_1.2.2与hbase_1.3.1不兼容，重新对hive-hbase-handler-1.2.2.jar进行编译
--FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. 
--org.apache.hadoop.hbase.HTableDescriptor.addFamily(Lorg/apache/hadoop/hbase/HColumnDescriptor;)V