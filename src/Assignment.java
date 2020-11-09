import oracle.jdbc.OracleTypes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Assignment {
    private String connectionString;
    private String DBUsername;
    private String DBPassword;
    private Connection connection;

    public Assignment(String connectionString, String DBUsername, String DBPassword){
        this.connectionString = connectionString;
        this.DBUsername = DBUsername;
        this.DBPassword = DBPassword;

        // Registering the oracle driver
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            this.connection = DriverManager.getConnection(this.connectionString, this.DBUsername, this.DBPassword);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void fileToDatabase(String filePath){
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
            String insetQuery = "INSERT INTO MediaItems (TITLE, PROD_YEAR) VALUES (?,?)";

            // if lost connection
            if (this.connection == null){
                createConnection();
            }

            PreparedStatement preparedStatement = this.connection.prepareStatement(insetQuery);
            // iterating through the records and insert every record
            for (List record: records){
                String title = (String)record.get(0);
                Integer prodYear = Integer.parseInt((String)record.get(1));
                preparedStatement.setString(1, title);
                preparedStatement.setInt(2, prodYear);

                preparedStatement.executeUpdate();
            }

            this.connection.commit();
            // closing resources
            preparedStatement.close();
        } catch (SQLException e) {
            try{
                e.printStackTrace();
                this.connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void createConnection(){
        // Registering the oracle driver
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            this.connection = DriverManager.getConnection(this.connectionString, this.DBUsername, this.DBPassword);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void calculateSimilarity(){
        // creating a connection
        try {

            // if lost connection
            if (this.connection==null)
                createConnection();

            // retrieving Maximal Distance in MediaItems table
            CallableStatement callableStatement = this.connection.prepareCall("{? = call MaximalDistance}");
            callableStatement.registerOutParameter(1, OracleTypes.NUMBER);
            callableStatement.execute();
            int maximalDistance = callableStatement.getInt(1);

            // if lost connection
            if (this.connection==null)
                createConnection();

            // retrieving all of the MIDs from MediaItems table
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT MID FROM MediaITEMS");
            List<Long> mids = new ArrayList<>();
            while (resultSet.next()){
                mids.add(resultSet.getLong("MID"));
            }

            // inserting or updating the similarity between the pairs
            for(int i=0; i<mids.size(); i++){
                for (int j=0; j<mids.size(); j++){
                    if (i != j){
                        Long MID1 = mids.get(i);
                        Long MID2 = mids.get(j);

                        // calculating the MID_i and MID_j similarity with the DB function
                        float pairSimilarity = getPairSimilarity(maximalDistance, MID1, MID2);
                        setSimilarity(MID1, MID2, pairSimilarity);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void setSimilarity(Long MID1, Long MID2, float pairSimilarity) {
        // inserting or updating the similarity between MID1 and MID2
        if (isPairExist(MID1, MID2)){
            updateSimilarity(MID1, MID2, pairSimilarity);
        }
        else{
            insertSimilarity(MID1, MID2, pairSimilarity);
        }
    }

    private void insertSimilarity(Long MID1, Long MID2, float pairSimilarity){
        // if lost connection
        if (this.connection==null)
            createConnection();

        try{
            String insert_query = "INSERT INTO Similarity (MID1, MID2, SIMILARITY) VALUES (?,?,?) ";
            PreparedStatement preparedStatement = this.connection.prepareStatement(insert_query);
            preparedStatement.setLong(1, MID1);
            preparedStatement.setLong(2, MID2);
            preparedStatement.setFloat(3, pairSimilarity);
            preparedStatement.executeUpdate();

            this.connection.commit();
            // closing resources
            if (preparedStatement!=null)
                preparedStatement.close();

        } catch (SQLException e) {
            try{
                e.printStackTrace();
                this.connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void updateSimilarity(Long MID1, Long MID2, float pairSimilarity){
        // if lost connection
        if (this.connection==null)
            createConnection();

        try{
            String update_query = "UPDATE Similarity SET SIMILARITY = ? WHERE MID1=? AND MID2=? ";
            PreparedStatement preparedStatement = this.connection.prepareStatement(update_query);
            preparedStatement.setFloat(1, pairSimilarity);
            preparedStatement.setLong(2, MID1);
            preparedStatement.setLong(3, MID2);
            preparedStatement.executeUpdate();

            this.connection.commit();
            // closing resources
            if (preparedStatement!=null)
                preparedStatement.close();

        } catch (SQLException e) {
            try{
                e.printStackTrace();
                this.connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    private float getPairSimilarity(int maximalDistance, Long MID1, Long MID2) {
        // if lost connection
        if (this.connection==null)
            createConnection();
        float pairSimilarity = 0;
        try{
            // retrieving MID_i and MID_j similarity
            CallableStatement callableStatement = connection.prepareCall("{? = call SimCalculation(?,?,?)}");
            callableStatement.registerOutParameter(1, OracleTypes.FLOAT);
            callableStatement.setLong(2, MID1);
            callableStatement.setLong(3, MID2);
            callableStatement.setInt(4, maximalDistance);
            callableStatement.execute();
            pairSimilarity =  callableStatement.getFloat(1);

            // closing resources
            if (callableStatement!=null)
                callableStatement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return pairSimilarity;
    }

    private boolean isPairExist(Long MID1, Long MID2){
        // if lost connection
        if (this.connection==null)
            createConnection();

        boolean isExist = false;
        try{
            PreparedStatement statement = connection.prepareStatement("SELECT MID1, MID2 FROM Similarity WHERE MID1=? AND MID2=? ");
            statement.setLong(1, MID1);
            statement.setLong(2, MID2);
            ResultSet resultSet = statement.executeQuery();
            isExist = resultSet.next();

            // closing resources
            if (statement!=null)
                statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return isExist;
    }

    public void printSimilarItems(long MID){
        try {
            Connection connection = DriverManager.getConnection(this.connectionString, this.DBUsername, this.DBPassword);
            String query = "SELECT mi2.TITLE, Similarity.SIMILARITY " +
                    "FROM MediaItems mi1 INNER JOIN Similarity ON mi1.MID = Similarity.MID1 INNER JOIN MediaItems mi2 ON mi2.MID = SIMILARITY.MID2 " +
                    "WHERE mi1.MID = ? AND SIMILARITY >= 0.3 " +
                    "ORDER BY SIMILARITY ASC ";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setLong(1, MID);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                System.out.println("("+resultSet.getString("TITLE")+","+resultSet.getString("SIMILARITY")+")");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
