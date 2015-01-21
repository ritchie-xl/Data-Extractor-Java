package loader;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.sql.Connection;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.apache.commons.configuration.*;




public class dataExtractor {
    public static void main(String[] args) throws Exception{
        /*
        Usage: $java data_extraction
        All the parameters are saved in the separated configuration file
         */

        //TODO: Handle different database system -- in process
        //TODO: Handle sql query
        //TODO: Handle the file query

        // Load configuration file
        String confPath = "conf/mysql.xml";
        XMLConfiguration config = new XMLConfiguration();
        config.setFile(new File(confPath));
        config.load(new File(confPath));

        // Create the connection to the database and dump out the file
        // Create variables that will be used by jdbc connector
        Connection connect;
        Statement statement;
        ResultSet resultSet;

        // Read all the configurations from the configuration file
        String ip = config.getString("ip");
        String port = config.getString("port");
        String database = config.getString("database");
        String table = config.getString("table");
        String user = config.getString("user");
        String password = config.getString("password");
        String dumpFolder = config.getString("output");
        String fileRecords = config.getString("fileRecords");
        String driver = config.getString("driver");
        String dataResource = config.getString("resource");
        String useCompression = config.getString("useCompression");
        int connectionPoolSize = config.getInt("connectionPoolSize");
        int fetchSize = config.getInt("fetchSize");

        // Construct the url for JDBC connector to connect to the database
        // This is the url for mysql server
        String MySQLUrl = "jdbc:mysql://" + ip + ":" + port + "/" + database+"?useCompression="+useCompression;
        //+ "?useServerPrepStmts=false&rewriteBatchedStatements=true";

        // Query to read all the content from the assigned table
        String query = "select * from " + table;

        // Get the day of year in the current date when the application is called
        Calendar calender = Calendar.getInstance();
        int currentYear = calender.get(Calendar.YEAR);
        int currentDayOfYear = calender.get(Calendar.DAY_OF_YEAR);

        String curDumpPath;
        String curDumpFolder;

        curDumpFolder = "Job_"+currentYear +String.format("%03d",currentDayOfYear);
        curDumpPath = dumpFolder + curDumpFolder;

        // try use Universal Connection Pool
        PoolDataSource pds = new PoolDataSourceFactory().getPoolDataSource();

        // TODO: add the class name for Sybase

        pds.setConnectionFactoryClassName(dataResource);
        pds.setURL(MySQLUrl);
        pds.setUser(user);
        pds.setPassword(password);
        pds.setValidateConnectionOnBorrow(true);

        pds.setInitialPoolSize(connectionPoolSize);
        // Connect to the database and execute the query
        System.out.println("Reading data from database, please wait....");

        Long before = System.currentTimeMillis();
        Class.forName(driver).newInstance();
        connect = pds.getConnection();

        statement = connect.createStatement();
        resultSet = statement.executeQuery(query);

        resultSet.setFetchSize(fetchSize);
        // Find out how many columns in the table
        ResultSetMetaData colMetaData = resultSet.getMetaData();
        int numColumn = colMetaData.getColumnCount();
        String[] outputFiles = new String[numColumn];
        String description = "";
        for (int i = 1; i < numColumn; i++) {
            String colName = colMetaData.getColumnName(i);
            description = description + colName +
                    ":" + colMetaData.getColumnTypeName(i) + ",";
            outputFiles[i-1]=curDumpPath+"/"+table+"_"+colName+"_"+i+".raw";
        }
        description = description + colMetaData.getColumnName(numColumn) +
                ":" + colMetaData.getColumnTypeName(numColumn);
        outputFiles[numColumn-1]=curDumpPath+"/"+table+"_"+colMetaData.getColumnName(numColumn)+"_"+numColumn+".raw";

        // Create the file to save the data dump out from the database by columns
        File file = new File(curDumpPath);
        file.mkdir();

        BufferedWriter[] bw = new BufferedWriter[numColumn];
        for(int i=0;i<numColumn;i++){
            bw[i] = new BufferedWriter(new FileWriter(outputFiles[i], true));
        }

        // Start writing data to the file
        int lineCount = 0;

        while (resultSet.next()) {
            // Read all the data from the table and write it into the data file
            for (int i = 1; i <= numColumn; i++) {
                bw[i-1].write(resultSet.getString(i));
                bw[i-1].newLine();
            }
            lineCount++;
        }

        for(int i = 0;i<numColumn;i++){
            bw[i].close();
        }

       /*
       Write the new file name into the file records
       The file records is used to save all the files that dump out from the database before
       */
        System.out.println(fileRecords);

        FileWriter fwRecords = new FileWriter(fileRecords, true);
        BufferedWriter bwRecords = new BufferedWriter(fwRecords);
        String fileRecord = curDumpFolder + "\t" + ip + "\t" + database + "\t" + table +
                "\t" + numColumn + "(" + description + ")" + "\t" + lineCount;
        bwRecords.write(fileRecord);
        bwRecords.newLine();
        bwRecords.close();
        Long after = System.currentTimeMillis();
        System.out.println("The running time is: " + (after - before) / 1000.0 + "s");
        System.out.println("Data transfer completed!");
    }
}
