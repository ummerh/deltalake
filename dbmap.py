## START module dbmap
class ColumnDescriptor:    
  def __init__(self, column):   
    self.name = column.name.lower()
    self.description = column.description
    self.dataType = column.dataType.lower()
    self.nullable = column.nullable
    self.isPartition = column.isPartition
    self.isBucket = column.isBucket
  
  def __eq__(self, other):
        if (isinstance(other, ColumnDescriptor)):          
          return self.name == other.name and self.dataType == other.dataType      
        
  def __repr__(self):
    return 'ColumnDescriptor({})'.format(self.name)
  
  def __str__(self):
    return 'Column - {}'.format(self.name)
  
class TableDescriptor:
  def __init__(self, table=None):
    self.columns=[]
    self.name = ''
    self.columnMap = dict()
    self.newColumns=[]
    self.missingColumns=[]
    self.mismatchColumns=[]
    if isinstance(table, str):
      self.name = table.lower()
    else:
      self.name = table.name.lower()
  
  def addColumn(self, column):
    self.columnMap[column.name] = column;
    self.columns.append(column)
  
  def diff(self, other):    
    diff = []    
    if(isinstance(other, TableDescriptor)):        
      for key, value in self.columnMap.items():        
        if key in other.columnMap.keys() and value != other.columnMap[key]:
          diff.append('Mismatch {} '.format(key))
          self.mismatchColumns.append(other.columnMap[key])
        elif key not in other.columnMap.keys() and not key.startswith('edp_'):
          diff.append('Missing {} '.format(value))        
          self.missingColumns.append(value)
      for key, value in other.columnMap.items():        
        if key not in self.columnMap.keys() and not key.startswith('edp_'):
          diff.append('New {}'.format(other.columnMap[key]))
          self.newColumns.append(other.columnMap[key])
    
    return diff
  
  def __eq__(self, other):    
    if (isinstance(other, TableDescriptor)):      
      return self.name == other.name and len(self.mismatchColumns) == 0 and len(self.missingColumns) == 0 and len(self.newColumns) == 0
  
  def __repr__(self):
    return 'TableDescriptor({})'.format(self.name)
  
  def __str__(self):
    return ' Table - {}'.format(self.name)

class PKDescriptor:
  def __init__(self):    
    self.name = ''
    self.schemaName=''
    self.tableName=''
    self.columns=[]
  
  def updatePKDefn(self, defns):
    for d in defns:
      self.name = d.pkname
      self.tableName=d.tablename
      self.columns.append(d.columnname)
      
class FKDescriptor:
  def __init__(self):    
    pass    
  
class DBMap:  
  def __init__(self, catalog, dbName):    
    self.catalog = catalog
    self.dbName = dbName
  def load(self):
    ## reset
    self.tables = []
    self.tableMap = dict()
    self.newTables=[]
    self.missingTables=[]
    self.mismatchTables=[]
    catalog = self.catalog
    dbName = self.dbName
    tables = self.catalog.listTables(self.dbName)
        
    for table in tables:      
      if table.tableType not in ['TEMPORARY']:        
        try:        
          tbl = TableDescriptor(table)
          tbl.dbName = dbName
          self.tables.append(tbl)      
          self.tableMap[tbl.name] = tbl
          columns = self.catalog.listColumns(tbl.name, dbName)
          for col in columns:
            tbl.addColumn(ColumnDescriptor(column=col))
        except BaseException as err:
          print('Failed to load ' + table.name)
          print(err)
  
  def diff(self, other):    
    diff = []    
    if(isinstance(other, DBMap)):        
      for key, value in self.tableMap.items():        
        if key in other.tableMap.keys():
          otherTable = other.tableMap[key];
          value.diff(otherTable)                    
          if value != otherTable:
            diff.append('Mismatch {} to {} '.format(value, otherTable))          
            self.mismatchTables.append(value)
          
        elif key not in other.tableMap.keys():
          diff.append('Missing {} '.format(value))        
          self.missingTables.append(value)
      
      for key, value in other.tableMap.items():        
        if key not in self.tableMap.keys():
          diff.append('New {}'.format(value))
          self.newTables.append(value)
    
    return diff
  
class MemTableSpec:
  def __init__(self, name):
    self.name = name
    self.tableType='MEMORY'
    
class MemColumnSpec:
  def __init__(self, name, dataType):
    self.name = name
    self.dataType = dataType   
    self.description = ''    
    self.nullable = ''
    self.isPartition = ''
    self.isBucket = ''
  
class MemCatalog:
  def __init__(self):
    self.tables={}
  
  def addTable(self, tableName, dtypes):
    self.tables.update({tableName:dtypes})
  
  def listTables(self, dbName=None):
    tables = []
    for tableName in self.tables.keys():
      tables.append(MemTableSpec(tableName))
    return tables
  
  def listColumns(self, tableName, dbName=None):
    cols = self.tables.get(tableName)
    columns = []
    for col in cols:
      columns.append(MemColumnSpec(col[0], col[1]))
    return columns

class HiveSQLGenerator:
  def __init__(self):
    pass
  @staticmethod
  def deltaTableDDL(dbName, tableName, columns, adls_loc):
    sql = []
    for col in columns:      
      sql.append(" {} {}".format(col.name, col.dataType))
    
    sql.append(" {} {}".format('edp_last_update_id', 'string'))
    sql.append(" {} {}".format('edp_last_update_type', 'string'))
    sql.append(" {} {}".format('edp_last_update_ts', 'timestamp'))
    colstring = ",\n".join(sql)
    return "create table {}.{}(\n{}\n)\n using delta \n location '{}/{}' TBLPROPERTIES (delta.enableChangeDataFeed = true);".format(dbName, tableName, colstring, adls_loc, tableName).lower()
  
  @staticmethod
  def deltaDiffDDL(dbName, dbmap, adls_loc):
    sqls = [];    
    newTables = dbmap.newTables
    for table in newTables:
      sqls.append(HiveSQLGenerator.deltaTableDDL(dbName, table.name, table.columns, adls_loc))
      
    for table in dbmap.mismatchTables:
      for col in table.newColumns:
        sqls.append("alter table {}.{} add columns ({} {});".format(dbName, table.name, col.name, col.dataType).lower())
      
      for col in table.mismatchColumns:
        # Column types cannot be changed - research this ????
        #sqls.append("alter table {}.{} alter column {} TYPE {};".format(dbName, table.name, col.name, col.dataType).lower())
        pass
      
      for col in table.missingColumns:
        #Do not drop columns
        #sqls.append("alter table {}.{} drop column {};".format(dbName, table.name, col.name).lower())        
        pass
    
    for table in dbmap.missingTables:      
      #Do not drop tables
      #sqls.append("drop table {}.{};".format(dbName, table.name).lower())
      pass    
    
    return sqls
  
  @staticmethod
  def selectSql(tableDesc, table, edp_last_update_id, edp_last_update_type):
    colsql = []
    for col in tableDesc.columns:
      if not col.name.startswith('edp_'):
        colsql.append(col.name)
    
    return "select {}, '{}' edp_last_update_id, '{}' edp_last_update_type, current_timestamp() edp_last_update_ts  from {}".format(','.join(colsql),edp_last_update_id, edp_last_update_type, table )
    
  @staticmethod
  def deltaInsert(currTable, newTable, alias, edp_last_update_id, edp_last_update_type):
    tableName = currTable.name
    dbName = currTable.dbName

    #run diff
    currTable.diff(newTable)
    
    cols = []
    icols = []
    for col in newTable.columns:      
      cols.append(col.name)
      icols.append(col.name)

    for col in currTable.missingColumns:
      cols.append('null '+ col.name)
      icols.append(col.name)

    # add edp columns
    cols.append("'{}' edp_last_update_id".format(edp_last_update_id))
    cols.append("'{}' edp_last_update_type".format(edp_last_update_type))
    cols.append("current_timestamp() edp_last_update_ts")
    
    icols.append("edp_last_update_id")
    icols.append("edp_last_update_type")
    icols.append("edp_last_update_ts")
      
    sqlStr = "insert into {dbName}.{tableName} ({icolsql}) select {colsql} from {temp} tbl".format(dbName=dbName, tableName=tableName, icolsql=', '.join(icols), colsql=', '.join(cols), temp=alias)
    print(sqlStr)
    return sqlStr
  
  @staticmethod
  def insertSql(dbName, tableName, temp, dtypes):
    cols = []
    for col in dtypes:      
      cols.append(col[0])
    sqlStr = "insert into {dbName}.{tableName}({colsql}) select {colsql} from {temp} tbl"
    return sqlStr.format(dbName=dbName, tableName=tableName, colsql=', '.join(cols), temp=temp)
  
  @staticmethod  
  def deltaMergeByPK(src, tgt, pkcols):        
    columnMapping = []
    for col in pkcols:
      columnMapping.append('src.'+col+' = tgt.'+col)
    
    sql = "MERGE INTO {src} src USING {tgt} tgt ON {columnMapping} \
            WHEN MATCHED \
              THEN UPDATE SET * \
            WHEN NOT MATCHED \
              THEN INSERT * ".format(src=src, tgt=tgt,columnMapping=(' and '.join(columnMapping)))
    
    return sql;
      
class SchemaActions:
  def __init__(self):
    self.name='Schema helper class'
  
  def checkAndCreate(self, dbname):
    try:
      spark.sql("create database {}".format(dbname))      
    except:
      pass    

  def checkAndReplaceTable(self, db_table_name, location):
    try:
      spark.sql("drop table {}".format(db_table_name))      
    except:
      pass      
    res = spark.sql("create table {} using delta location '{}' ".format(db_table_name, location))
    res.show()

  def addEdpColumns(self, df, edp_last_update_id, edp_last_update_type, edp_last_update_ts):
    df.createOrReplaceTempView('old')    
    newDf = spark.sql("select old.*, '{}' edp_last_update_id, '{}' edp_last_update_type, current_timestamp() edp_last_update_ts from old old".format(edp_last_update_id, edp_last_update_type))    
    return newDf
    
  def adjustTableSchema(self, dbName, tableName, dtypes):    
    #load spark catalog      
    dbMap = DBMap(catalog=spark.catalog, dbName=dbName)
    dbMap.load()
    currTable = dbMap.tableMap[tableName]            
    print(currTable)
    #load in-memory catalog
    memCatalog = MemCatalog()
    memCatalog.addTable(tableName, dtypes )
    newDBMap = DBMap(catalog=memCatalog, dbName=dbName)
    newDBMap.load()
    newTable = newDBMap.tableMap[tableName]    

    #run diff
    currTable.diff(newTable)      

    for col in currTable.newColumns:
      try:
        sql = "alter table {}.{} add columns ({} {});".format(dbName, tableName, col.name, col.dataType)
        print(sql)
        spark.sql(sql)
      except BaseException as err:
        print('Failed to add new column ' + col.name)
        print(err)
    
    return currTable
  
 
  def saveAsNewTable(self, dbName, tableName, location, tempDf, alias):
    try:
      dtypes = tempDf.dtypes
      cols = []
      for col in dtypes:      
        cols.append(" {} {}".format(col[0], col[1]))
      sql_string = "create table {}.{} ({}) using delta location '{}'".format(dbName, tableName,','.join(cols), location)
      spark.sql(sql_string)
      spark.sql("insert into {}.{} select * from {}".format(dbName, tableName, alias))
      print('Created and table using ' + sql_string)
    except BaseException as err:
        print('Failed to save as new table' + tableName)
        print(err)
    
    return tempDf
  
  def checkAndOverwrite(self, dbName, tableName, location, tempDf, alias, delete):
    try:
      #check if table exists
      spark.sql("describe {}.{}".format(dbName, tableName))        
    except:
      #create it as a new table
      self.saveAsNewTable(dbName, tableName, location, tempDf, alias)
      return tempDf
    
    try:
      # add new columns if any found
      currTable = self.adjustTableSchema(dbName, tableName, tempDf.dtypes)      
      cols = []
      icols = []
      for col in tempDf.dtypes:      
        cols.append(col[0])
        icols.append(col[0])
      
      for col in currTable.missingColumns:
        cols.append('null '+ col.name)
        icols.append(col.name)
      
      if delete:
        spark.sql("delete from {}.{}".format(dbName, tableName))
        
      sqlStr = "insert into {dbName}.{tableName} ({icolsql}) select {colsql} from {temp} tbl".format(dbName=dbName, tableName=tableName, icolsql=', '.join(icols), colsql=', '.join(cols), temp=alias)
      print(sqlStr)
      res = spark.sql(sqlStr)
      res.show()
      return tempDf    
    except BaseException as err:          
      print(err)
      return tempDf
  
  def checkAndMerge(self, dbName, tableName, location, tempDf, alias, pkcols):    
    try:
      #check if table exists
      spark.sql("describe {}.{}".format(dbName, tableName))        
    except:
      #create it as a new table
      self.saveAsNewTable(dbName, tableName, location, tempDf, alias)
      return tempDf
        
    try:
      # add new columns if any found
      currTable = self.adjustTableSchema(dbName, tableName, tempDf.dtypes )
      srcCols = []
      tgtCols = []
      updateCols = []
      
      for col in tempDf.dtypes:      
        srcCols.append(col[0])
        tgtCols.append('tgt.' + col[0])
        updateCols.append('src.' + col[0] + '='+'tgt.' + col[0])

      for col in currTable.missingColumns:
        srcCols.append(col.name)
        tgtCols.append('null')

      insertSqlStr = "insert ({srcCols}) values ({tgtCols})".format(srcCols=', '.join(srcCols), tgtCols=', '.join(tgtCols))
      updateSqlStr = "update set {updateCols}".format(updateCols=','.join(updateCols))
      
      columnMapping = []
      for col in pkcols:
        columnMapping.append('src.' + col + ' = tgt.' + col)

      mergeSql = """
            MERGE INTO {dbName}.{tableName} src USING {alias} tgt ON {columnMapping}
            WHEN MATCHED 
              THEN {updateSqlStr}
            WHEN NOT MATCHED 
              THEN {insertSqlStr}
              """.format(dbName=dbName, tableName=tableName, alias=alias, columnMapping=(' and '.join(columnMapping)), updateSqlStr=updateSqlStr, insertSqlStr=insertSqlStr)
      print(mergeSql)
      res = spark.sql(mergeSql)      
      res.show()
      return tempDf    
    except BaseException as err:          
      print("Error: {0}".format(err))
      ##TODO log error
      return tempDf  

class DefaultDataActions:
  def __init__(self):
    self.name=''
    
  #overwrite existing data by issuing a delete and then insert
  def overwrite(self, target, edp_last_update_id, edp_last_update_type, edp_last_update_ts):    
    dbName = target['dbname']
    tableName = target['table']    
    table_location = "{}/{}".format(target['location'], target['table'])
    #creat a temp view using the SQL
    print('Loading using sql - ' + target['sql'])
    pdf = spark.sql(target['sql'])      
    df = SchemaActions().addEdpColumns(pdf, edp_last_update_id, edp_last_update_type, edp_last_update_ts)
    df.createOrReplaceTempView(target['alias'])
    SchemaActions().checkAndOverwrite(dbName, tableName, table_location, df, target['alias'], True)
  
  #append to existing data
  def append(self, target, edp_last_update_id, edp_last_update_type, edp_last_update_ts):
    dbName = target['dbname']
    tableName = target['table']    
    table_location = "{}/{}".format(target['location'], target['table'])
    #creat a temp view using the SQL
    print('Loading using sql - ' + target['sql'])
    pdf = spark.sql(target['sql'])      
    df = SchemaActions().addEdpColumns(pdf, edp_last_update_id, edp_last_update_type, edp_last_update_ts)
    df.createOrReplaceTempView(target['alias'])
    SchemaActions().checkAndOverwrite(dbName, tableName, table_location, df, target['alias'], False) 
  
  #replace table data and schema using the new select statement
  def replace(self, target, edp_last_update_id, edp_last_update_type, edp_last_update_ts):    
    db_table_name = "{}.{}".format(target['dbname'], target['table'])
    table_location = "{}/{}".format(target['location'], target['table'])
    #creat a temp view using the SQL
    print('Loading using sql - ' + target['sql'])
    pdf = spark.sql(target['sql'])      
    df = SchemaActions().addEdpColumns(pdf, edp_last_update_id, edp_last_update_type, edp_last_update_ts)    
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_location)    
    SchemaActions().checkAndReplaceTable(db_table_name, table_location)
    df.show()
  
  #rewrite table data and schema using the existing contents of the table, typically used to heal the table
  def rewrite(self, target, edp_last_update_id, edp_last_update_type, edp_last_update_ts):    
    db_table_name = "{}.{}".format(target['dbname'], target['table'])
    table_location = "{}/{}".format(target['location'], target['table'])
    #creat a temp view using the SQL
    print('Loading using sql - ' + target['sql'])
    pdf = spark.sql(target['sql'])      
    df = SchemaActions().addEdpColumns(pdf, edp_last_update_id, edp_last_update_type, edp_last_update_ts)
    tempTable = db_table_name + "_temp"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tempTable)
    spark.sql("drop table " + db_table_name)
    spark.read.table(tempTable).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(db_table_name)
    spark.sql("drop table " + tempTable)

  #merge data into existing table using the new select statement and specified pk columns
  def merge(self, target, edp_last_update_id, edp_last_update_type, edp_last_update_ts):
    dbName = target['dbname']
    tableName = target['table']    
    table_location = "{}/{}".format(target['location'], target['table'])
    #creat a temp view using the SQL
    print('Loading using sql - ' + target['sql'])
    pdf = spark.sql(target['sql'])      
    df = SchemaActions().addEdpColumns(pdf, edp_last_update_id, edp_last_update_type, edp_last_update_ts)
    df.createOrReplaceTempView(target['alias'])
    SchemaActions().checkAndMerge(dbName, tableName, table_location, df, target['alias'], target['pkcols'])       
##END module dbmap
