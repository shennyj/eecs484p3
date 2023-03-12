import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();
        
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            // Your implementation goes here....
            
            ResultSet rst = stmt.executeQuery(
                "SELECT * FROM "+userTableName
            );
            while(rst.next()){
                JSONObject new_user = new JSONObject();
                int user_id = rst.getInt(1);
                String first_name = rst.getString(2);
                String last_name = rst.getString(3);
                String gender = rst.getString(4);
                int YOB = rst.getInt(5);
                int MOB = rst.getInt(6);
                int DOB = rst.getInt(7);
                JSONArray friends = new JSONArray();
                JSONObject current = new JSONObject();
                JSONObject hometown = new JSONObject();

                
                
                try (Statement friendStmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    ResultSet frst=friendStmt.executeQuery(
                        "SELECT user2_ID FROM "+friendsTableName+" "+
                        "WHERE user1_ID = "+user_id
                    );
                    while(frst.next())
                    {
                        friends.put(frst.getInt(1));
                    }   
                } catch (SQLException e) {
                System.err.println(e.getMessage());
                }
            
            try (Statement currentStmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    ResultSet curr=currentStmt.executeQuery(
                        "SELECT C.city_name, C.state_name, C.country_name FROM "+currentCityTableName +" CC, "+cityTableName+" C "+
                        "WHERE CC.current_city_id = C.city_id AND CC.user_id = "+user_id
                );
                while(curr.next())
                {
                    String city = curr.getString(1);
                    String state = curr.getString(2);
                    String country = curr.getString(3);
                    current.put("city",city);
                    current.put("state",state);
                    current.put("country",country);
                }
                
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }

            try (Statement hometownStmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    ResultSet curr=currentStmt.executeQuery(
                        "SELECT C.city_name, C.state_name, C.country_name FROM "+hometownCityTableName +" HC, "+cityTableName+" C "+
                        "WHERE HC.hometown_city_id = C.city_id AND CC.user_id = "+user_id 
                );
                while(curr.next())
                {
                    String city = curr.getString(1);
                    String state = curr.getString(2);
                    String country = curr.getString(3);
                    hometown.put("city",city);
                    hometown.put("state",state);
                    hometown.put("country",country);
                }
                
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }

                new_user.put("user_id",user_id);
                new_user.put("first_name",first_name);
                new_user.put("last_name",last_name);
                new_user.put("gender",gender);
                new_user.put("YOB",YOB);
                new_user.put("MOB",MOB);
                new_user.put("DOB",DOB);
                new_user.put("current",current);
                new_user.put("hometown",hometown);
                new_user.put("friends",friends);

                users_info.put(new_user);

            }
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
