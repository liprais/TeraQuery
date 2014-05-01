require "rubygems"
require "thor"
require "./teradata.rb"

include Teradata

class App
  
  def exec(conn)
    loop do
      user_query_exec conn
    end
  end

  def user_query_exec(conn)
    Proc.new {
      |v|
      sql = ""
      loop do 
        
        sql.clear
        puts "NOW PLEASE INPUT"
  
        loop do
          sql << gets.chomp
          break if sql[-1] == ";"
        end
  
        begin
          Teradata.get_result_set(sql,"nocache",v).values.map { |i| puts i.join("\s") }
        rescue StandardError => bong
          puts bong
        end
      end
    }.call(conn)
  end
  
end


conn = Teradata.connect("vm", "dbc", "dbc")

App.new.exec(conn)
