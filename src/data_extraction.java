import java.io.*;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

/**
 * Created by ritchie on 12/9/14.
 */
public class data_extraction {
    public static void main(String[] args) throws Exception{
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String database = args[0];
        String table = args[1];
        int pathLen = args[2].length();
        String outputFolder = (args[2].substring(pathLen-1).equals("/")) ? args[2] : (args[2] + "/") ;

        String user="root";
        String password ="";


        // Following variables are used for testing and should be received from arguments
        String fileRecords = "/Users/ritchie/IdeaProjects/Java_ETL/data/fileRecords";
        /*String outputFolder = "/Users/ritchie/IdeaProjects/Java_ETL/output/";
        String database = "test";
        String table = "messages";
        */
        // Construct the url for jdbc connector to connect to the database
        String url = "jdbc:mysql://localhost/" + database;

        // Query to read all the content from the assigned table
        String query = "select * from " + table;

        Calendar calender = Calendar.getInstance();
        String currentYear = Integer.toString(calender.get(Calendar.YEAR));
        String currentDayOfYear = Integer.toString(calender.get(Calendar.DAY_OF_YEAR));
        String newFilePath;
        String newFileName;

        String line;
        String last="";

        try{
            // Check if this is the first dump
            File records = new File(fileRecords);
            if(records.length() == 0){
                newFileName = currentYear + currentDayOfYear + "001.dat";
            }else{
                try {
                    FileReader fr = new FileReader(fileRecords);
                    BufferedReader br = new BufferedReader(fr);
                    while((line = br.readLine())!=null){
                        last = line;
                    }

                    // Construct the file path for the file to save the data dump out from the database
                    String year = last.substring(0, 4);
                    String day = last.substring(4, 7);
                    int number = Integer.parseInt(last.substring(7,10));
                    if(currentDayOfYear.equals(day) && currentYear.equals(year)){
                        number = number + 1;
                        int r1 = number%100;
                        String s1 = Integer.toString(number/100);
                        int r2 = r1%10;
                        String s2 = Integer.toString(r1/10);
                        String s3 = Integer.toString(r2);
                        newFileName = year+day+s1+s2+s3+".dat";
                    }else{
                        newFileName = currentYear + currentDayOfYear + "001.dat";
                    }

                }catch(FileNotFoundException e) {
                    throw e;
                }
            }
        }catch(FileNotFoundException e){
            throw e;
        }
        newFilePath = outputFolder + newFileName;



        //Create the connection to the database and dump out the file
        // Create variables that will be used by jdbc connector
        Connection connect = null;
        java.sql.Statement statement = null;
        ResultSet resultSet = null;

        // Connect to the database and execute the query
        try{
            Class.forName(JDBC_DRIVER);
            connect = DriverManager.getConnection(url,user,password);
            statement = connect.createStatement();
            resultSet = statement.executeQuery(query);
//            System.out.println("id\t\tmessages\tcount");
            // Create the file to save the data dump out from the database
            File file = new File(newFilePath);
            file.createNewFile();
            FileWriter fw = new FileWriter(newFilePath,true);
            BufferedWriter bw = new BufferedWriter(fw);

            // Start writing data to the file
            while(resultSet.next()){
                String id = resultSet.getString("id");
                String date = resultSet.getString("date");
                String count = resultSet.getString("count");
                String newLine = id+","+date+","+count;
//                System.out.println(id+"\t"+date+"\t"+count);
                try{
                    bw.write(newLine);
                    bw.write("\n");
                }catch(FileNotFoundException e){
                    System.out.print(e);
                    System.exit(1);
                }
            }
            bw.close();
        }catch(SQLException e){
            throw e;
        }finally{
            resultSet.close();
            statement.close();
            connect.close();
        }

        try{
            //Write the new file name into the file records
            // The file records is used to save all the files that dump out from
            // the database before.
            FileWriter fw = new FileWriter(fileRecords,true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(newFileName);
            bw.write("\n");
            bw.close();
        }catch(Exception e){
            throw e;
        }
    }
}
