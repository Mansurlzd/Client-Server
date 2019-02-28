
import java.io.*;
import java.sql.*;
import java.util.*;
import java.net.*;


public class Server
{
    public static void main(String[] args) throws IOException
    {
        // server is listening on port 5056
        ServerSocket ss = new ServerSocket(1408);


        // running infinite loop for getting
        // client request


        while (true)
        {
            Socket s = null;

            try
            {
                // socket object to receive incoming client requests
                s = ss.accept();

                System.out.println("A new client is connected : " + s);

                // obtaining input and out streams
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());

                System.out.println("Assigning new thread for this client");


                // create a new thread object
                Thread t = new ClientHandler(s, dis, dos);

                // Invoking the start() method
                t.start();

            }
            catch (Exception e){
                s.close();
                e.printStackTrace();
            }
        }
    }




}

// ClientHandler class
class ClientHandler extends Thread
{

    final DataInputStream dis;
    final DataOutputStream dos;
    final Socket s;

    final List<String>blacklistedUsers=Arrays.asList("john","doe","someone","test","admin");



    public ClientHandler(Socket s, DataInputStream dis, DataOutputStream dos)
    {
        this.s = s;
        this.dis = dis;
        this.dos = dos;
    }

    public enum Error {
        NOBALANCE(0, "You don't have enough balance to play :( ."),
        HIGHCHANGE(1, "Balance change is bigger than configured limit :( ."),
        BLACKLIST(2, "We figured out that you are in the blacklist :( , get a new username! :) ."),
        TRANSACTIONEXIST(3,"We found out that this transaction id was used before");


        private final int code;
        private final String description;

        private Error(int code, String description) {
            this.code = code;
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        public int getCode() {
            return code;
        }

        @Override
        public String toString() {
            return  description;
        }
    }

    @Override
    public void run()
    {

        String[] received=new String[3];
        String errorCode="";
        int defaultBalance;
        int finalBalance;
        //storing transactionId and username,balance
        HashMap<String,String>userData=new HashMap<String,String>();
        HashMap<String,Integer>userBalance=new HashMap<String, Integer>();


        while (true)
        {
            try {

                // Ask user what he/she wants

                dos.writeUTF("Enter username and transaction_id(comma seperated)[username,transaction_id]:  (For quit type Exit)");


                // receive the answer from client

                received = dis.readUTF().split(",");
                connect();
                String error="";
                String username=received[0];
                String transactionId=received[1];
                int balanceChange=Integer.parseInt(received[2]);
                List<String> result=getUsername(username);

                //if user is in the db version is 1 if not 0,after insert version has autoincrement
                int balanceVersion;


                if(received.equals("Exit"))
                {
                    System.out.println("Client " + this.s + " sends exit...");
                    System.out.println("Closing this connection.");
                    this.s.close();
                    System.out.println("Connection closed");
                    break;
                }
                if(result.size()>0) {

                    balanceVersion = Integer.parseInt(result.get(0));
                    defaultBalance = Integer.parseInt(result.get(2));
                }
                else{
                    defaultBalance=50;
                    balanceVersion=0;
                }
                    //checking for define error information
                    if(checkBalance(defaultBalance)!=true){
                        error+=Error.NOBALANCE.toString();}
                    if(checkBalanceLimit(balanceChange)!=true){
                        error+=Error.HIGHCHANGE.toString();}
                    if (checkBlackList(username)!=true){
                        error+=Error.BLACKLIST.toString();}

                    if (checkAllConditions(defaultBalance, balanceChange, username) == true) {

                        //logic could be changed here,i check only for transaction id ,balance isn't changed.
                        if(userData.containsKey(transactionId)){

                            String key=userData.get(transactionId);
                            errorCode=Error.TRANSACTIONEXIST.toString();
                            username=key.split(",")[0];
                            finalBalance=Integer.parseInt(key.split(",")[1]);
                            dos.writeUTF("Transaction id :" + transactionId + " exists, error code is: " + errorCode + " by : "+username+ " with balance : "+finalBalance+" use different transaction id");
                        }

                        else {
                            if(userBalance.containsKey(username)){

                                finalBalance=userBalance.get(username)+balanceChange;

                            }
                            else{
                            finalBalance=defaultBalance+balanceChange;}
                            userData.put(transactionId, username + "," + finalBalance);
                            userBalance.put(username,finalBalance);
                            dos.writeUTF("Hi " +username + ", your  transaction id is : " + transactionId + " ,balance change is :" +balanceChange+" ,balanceVersion: "+ balanceVersion + ",balance after change is : " + finalBalance + " ,error didn't occur :)" );
                            if(result.size()>0) {
                                update(finalBalance,username);
                            }
                            if(result.size()==0 && userBalance.size()>1) {
                                insert(finalBalance,username);
                            }
                        }
                    }
                    else
                    {
                        dos.writeUTF("Dear " + username +  ", you can't continue due to : error code: " + error);
                    }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try
        {
            // closing resources
            this.dis.close();
            this.dos.close();

        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public boolean checkAllConditions(int balance,int balanceChange,String username){


        if(checkBalance(balance)==true && checkBalanceLimit(balanceChange)==true && checkBlackList(username)==true) {
            return true;
        }
        return false;

    }

    public  boolean checkBalance(int balance){

        if(balance<0) {
            return false;
        }
        return true;
    }
    public  boolean  checkBalanceLimit(int balanceChange){

        if (balanceChange>100 || balanceChange < -100) {
            return false;
        }
        return true;
    }


    public  boolean checkBlackList(String username){

      if(blacklistedUsers.contains(username.toLowerCase())==true) {
          return false;
      }
      return true;
    }



    private Connection connect() {

        // SQLite connection string
        String url = "jdbc:sqlite:C:/Users/mansur.alizada/Desktop/sqlite/testdb.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public List<String> getUsername(String username){
        String sql = "SELECT balance_version, username, balance "
                + "FROM YOURTABLE WHERE username = ?";

        List<String> result =new ArrayList<String>();
        try (Connection conn = this.connect();
             PreparedStatement pstmt  = conn.prepareStatement(sql)){

            // set the value
            pstmt.setString(1,username);
            //
            ResultSet rs  = pstmt.executeQuery();

            // loop through the result set
            while (rs.next()) {

                result.add(rs.getString("balance_version"));
                result.add(rs.getString("username"));
                result.add(rs.getString("balance"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return result;
    }
    public void insert(int balance, String username) {
        String sql = "INSERT INTO YOURTABLE(balance,username) VALUES(?,?)";
        PreStatement(sql,username,balance);

    }
    public void update(int balance, String username) {
        String sql = "UPDATE YOURTABLE SET balance = ?  "
                + "WHERE username = ?";
        PreStatement(sql,username,balance);

    }
    public void PreStatement(String sql,String username,int balance){

        try (Connection conn = this.connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setInt(1, balance);
            pstmt.setString(2, username);

            // update
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }



}
