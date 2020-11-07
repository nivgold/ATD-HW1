import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Assignment {
    private String connectionString;
    private String DBUsername;
    private String DBPassword;

    public Assignment(String connectionString, String DBUsername, String DBPassword){
        this.connectionString = connectionString;
        this.DBUsername = DBUsername;
        this.DBPassword = DBPassword;

        // Registering the oracle driver
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void fileToDataBase(String filePath){
        // Reading the .csv rows into List of Lists
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // creating a connection to the DB and inserting all of the records of the file
        try {
            Connection connection = DriverManager.getConnection(this.connectionString, this.DBUsername, this.DBPassword);
            String insetQuery = "INSERT INTO MediaItems (TITLE, PROD_YEAR) VALUES (?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insetQuery);
            // iterating through the records and insert every record
            for (List record: records){
                String title = (String)record.get(0);
                Integer prodYear = Integer.parseInt((String)record.get(1));
                preparedStatement.setString(1, title);
                preparedStatement.setInt(2, prodYear);
                preparedStatement.executeQuery();
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void calculateSimilarity(){
        // creating a connection
        try {
            Connection connection = DriverManager.getConnection(this.connectionString, this.DBUsername, this.DBPassword);
            CallableStatement callableStatement = connection.prepareCall("{? = call MaximalDistance}");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.execute();
            int maximalDistance = callableStatement.getInt(1);

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT MID FROM MediaITEMS");
            List<Integer> mids = new ArrayList<>();
            while (resultSet.next()){
                mids.add(resultSet.getInt("MID"));
            }
            connection.close();

            // iterating through all of the pairs
            for(int i=0; i<mids.size(); i++){
                for (int j=i+1; j<mids.size(); j++){
                    connection = DriverManager.getConnection(this.connectionString, this.DBUsername, this.DBPassword);
                    CallableStatement callableStatement2 = connection.prepareCall("{? = call SimCalculation(?,?,?)}");
                    callableStatement2.registerOutParameter(1, Types.FLOAT);
                    callableStatement2.setInt(2, mids.get(i));
                    callableStatement2.setInt(3, mids.get(j));
                    callableStatement2.setInt(4, maximalDistance);
                    System.out.println("CALL SimCalculation("+mids.get(i)+","+mids.get(j)+","+maximalDistance+")");
                    callableStatement2.execute();
                    float pairSimilarity = callableStatement2.getFloat(1);
                    System.out.println(pairSimilarity);
                    if (isPairExist(connection, mids.get(i), mids.get(j))){
                        // update query
                        String update_query = "UPDATE Similarity SET SIMILARITY = ? WHERE MID1=? AND MID2=? ";
                        PreparedStatement preparedStatement = connection.prepareStatement(update_query);
                        preparedStatement.setFloat(1, pairSimilarity);
                        preparedStatement.setInt(2, mids.get(i));
                        preparedStatement.setInt(3, mids.get(j));
                        preparedStatement.executeUpdate();
                        System.out.println("updated similarity row");
                        preparedStatement.close();
                    }
                    else{
                        // insert query
                        String insert_query = "INSERT INTO Similarity (MID1, MID2, SIMILARITY) VALUES (?,?,?) ";
                        PreparedStatement preparedStatement = connection.prepareStatement(insert_query);
                        preparedStatement.setInt(1, mids.get(i));
                        preparedStatement.setInt(2, mids.get(j));
                        preparedStatement.setFloat(3, pairSimilarity);
                        preparedStatement.executeUpdate();
                        System.out.println("inserted similarity row");
                        preparedStatement.close();
                    }
                    connection.commit();
                    connection.close();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean isPairExist(Connection connection, int MID1, int MID2){
        try{
            PreparedStatement statement = connection.prepareStatement("SELECT MID1, MID2 FROM Similarity WHERE MID1=? AND MID2=? ");
            statement.setInt(1, MID1);
            statement.setInt(2, MID2);
            ResultSet resultSet = statement.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void printSimilarItems(long MID){
        try {
            Connection connection = DriverManager.getConnection(this.connectionString, this.DBUsername, this.DBPassword);
            String query = "SELECT TITLE, SIMILARITY FROM ((SELECT mi2.TITLE, SIMILARITY " +
                    "FROM MediaItems mi1 JOIN Similarity ON mi1.MID = Similarity.MID1 JOIN MediaItems mi2 ON mi2.MID = SIMILARITY.MID2 " +
                    "WHERE mi1.MID = ? AND SIMILARITY >= 0.3)" +
                    "UNION (" +
                    "SELECT mi4.TITLE, SIMILARITY " +
                    "FROM MediaItems mi3 JOIN Similarity ON mi3.MID = Similarity.MID2 JOIN MediaItems mi4 ON mi4.MID = SIMILARITY.MID1 " +
                    "WHERE mi3.MID = ? AND SIMILARITY >= 0.3)) " +
                    "ORDER BY SIMILARITY ASC ";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setLong(1, MID);
            preparedStatement.setLong(2, MID);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                System.out.println("("+resultSet.getString("TITLE")+","+resultSet.getString("SIMILARITY")+")");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
