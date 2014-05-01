#TeraQuery:a Simple TERADATA Query Tool using JDBC & JRuby

##Goal
* This is meant to be a simple command-line tool which can be used to query TERADATA DATABASE using JDBC & JRuby

##DEPLOY && USAGE
* JRUBY (RVM highly recommended)
* gem install jdbc-teradata ( Many thanks to Chris Parker)
* ruby tdQuery.rb
  * For now you have to write down your database ip,username & password manually into this file,a interactive logon method will be added later.

##LIMIT
* only interactive mode supported.
* ony tested against my own vm running Teradata 15.
* must have unknown bugs.

