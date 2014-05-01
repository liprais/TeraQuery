module Teradata
  require 'rubygems'
  require 'jdbc/teradata'
  require 'java'
  
  Jdbc::Teradata.load_driver
  import Java::com.teradata.jdbc.TeraDriver
  
  def self.connect(box,id,pwd)
    userurl = "jdbc:teradata://#{box}/LOB_SUPPORT=on"
    java.sql.DriverManager.set_login_timeout(1)
    begin
      con = java.sql.DriverManager.get_connection(userurl,id,pwd)
    rescue StandardError => bang
      #do nothing
    end
    con.create_statement.execute_query("diagnostic nocache on for session;")
    return con.create_statement
  end
  
  def self.get_result_count(qry,diag,conn)
    conn.execute_query("diagnostic #{diag} on for session;")
    result_count = conn.execute_query(qry).row_count
    clean(conn,diag)
    return { diag => result_count }
  end
  
  def self.get_result_set(qry,diag,conn)
    conn.execute_query("diagnostic #{diag} on for session")
    rs = conn.execute_query(qry)
    cc = rs.getMetaData.get_column_count
    result_set = {}
    while rs.next
      result_set.store( rs.get_row,1.upto(cc).map { |i| rs.getObject(i).to_s.strip } )
    end
    clean(conn,diag)
    return result_set
  end
  
  def clean(conn,diag)
    Proc.new {
      |conn,diag|
      conn.execute_query("diagnostic #{diag} not on for session;")
      conn.execute_query("diagnostic spoil;")
      conn.execute_query("diagnostic spoilc;")
      conn.execute_query("diagnostic nocache on for session;")
    }.call(conn,diag)
  end
  
end
