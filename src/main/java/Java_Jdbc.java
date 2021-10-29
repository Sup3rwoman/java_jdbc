import java.sql.*;

public class Java_Jdbc {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        final String DRIVER = "com.mysql.cj.jdbc.Driver";
        final String URL = "jdbc:mysql://localhost/quest_jdbc";
        final String USER = "***";
        final String PASSWORD ="***";
        final String INSERT_PERSON = "INSERT INTO persons (firstname, lastname, age) VALUES (?,?,?)";
        final String UPDATE_LASTNAME = "UPDATE persons SET lastname=? WHERE lastname=?";
        final String DELETE_PERSON = "DELETE FROM persons WHERE lastname=?";

        try{
            //load Driver
            Class.forName(DRIVER);

            //Connecting to JDBC Database
            Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("Connection established......");

            //Display MetaData of table persons
            displayMetaData(connection);

            //Display records of table persons
            System.out.println("\nRecords of table persons:");
            displayPersons(connection);

            //Insert new records into table persons
            insertPerson(INSERT_PERSON, connection, "Christiane", "Bock", 25);
            insertPerson(INSERT_PERSON, connection, "Paul", "Maus", 45);

            //Display records after inserting new records
            System.out.println("\nRecords after inserting new persons:");
            displayPersons(connection);

            //Update record in table persons
            updateLastname(UPDATE_LASTNAME, connection, "Bock", "Langhans");

            //Display records after updating record
            System.out.println("\nRecords after updating lastname of person:");
            displayPersons(connection);

            //Delete records from table persons
            deletePerson(DELETE_PERSON, connection, "Langhans");
            deletePerson(DELETE_PERSON, connection, "Maus");

            //Display records after deleting records
            System.out.println("\nRecords after deleting persons:");
            displayPersons(connection);

            //Close Connection
            connection.close();
            System.out.println("\nConnection closed......");
        }catch(ClassNotFoundException e){
            System.err.println(e.getMessage());
        }catch(SQLSyntaxErrorException e){
            System.err.println(e.getMessage());
        }catch(SQLException e){
            System.err.println(e.getMessage());
        }
    }

    private static void displayMetaData(Connection connection){
        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery("select * from persons");
            ResultSetMetaData rsMetaData = rs.getMetaData();

            System.out.println("\nMetaData:");
            System.out.println("Table Name: "+ rsMetaData.getTableName(1));
            System.out.println("Number of columns: "+ rsMetaData.getColumnCount());
            for(int i=1; i <= rsMetaData.getColumnCount();i++){
                System.out.println("Column " + i + ": [NAME: " + rsMetaData.getColumnLabel(i) + "; TYPE: " + rsMetaData.getColumnTypeName(i) + "]");
            }
        }catch(SQLException e){
            System.err.println(e.getMessage());
        }

    }

    private static void deletePerson(String DELETE_PERSON, Connection connection, String lastname) throws SQLException {
        //Check if record exists in table
        PreparedStatement ps = connection.prepareStatement("select * from persons where lastname=?");
        ps.setString(1, lastname);
        ResultSet rs = ps.executeQuery();

        if(rs.next()){
            ps = connection.prepareStatement(DELETE_PERSON);
            ps.setString(1,lastname);
            ps.executeUpdate();

        }else{
            System.err.println("Could not DELETE record. Record does not exist in table.");
        }

        ps.close();
        rs.close();
    }

    private static void updateLastname(String UPDATE_LASTNAME, Connection connection, String oldLastname, String newLastname ) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("select * from persons where lastname=?");
        ps.setString(1, oldLastname);
        ResultSet rs = ps.executeQuery();

        if(rs.next()){
            ps = connection.prepareStatement(UPDATE_LASTNAME);
            ps.setString(1, newLastname);
            ps.setString(2, oldLastname);
            ps.executeUpdate();
        }
        else{
            System.err.println("Could not UPDATE record. Record does not exist in table.");
        }
        rs.close();
        ps.close();
    }

    private static void insertPerson(String PREP_INSERT, Connection connection, String firstname, String lastname, Integer age){
        try{
            PreparedStatement ps = connection.prepareStatement(PREP_INSERT);
            ps.setString(1, firstname);
            ps.setString(2, lastname);
            ps.setInt(3, age);
            ps.execute();
            ps.close();
        }catch(SQLException e){
            System.err.println("Could not insert new Person. " + e.getMessage());
        }
    }

    //Display records in table persons
    private static void displayPersons(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from persons");

        while(rs.next()){
            System.out.println(rs.getString(1) + " " + rs.getString(2) + ", " + rs.getString(3) );
        }

        rs.close();
        statement.close();
    }
}
