package org.dstu.db;

import org.dstu.util.CsvReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DbWorker {
    public static void populateFromFile(String fileName) {
        List<String[]> strings = CsvReader.readCsvFile(fileName, ";");
        Connection conn = DbConnection.getConnection();
        try {
            Statement cleaner = conn.createStatement();
            System.out.println(cleaner.executeUpdate("DELETE FROM mammal"));
            System.out.println(cleaner.executeUpdate("DELETE FROM bird"));
            PreparedStatement mammalSt = conn.prepareStatement(
                    "INSERT INTO mammal (genus, name, age, is_male, is_up_right, is_season_hibernation) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");
            PreparedStatement birdSt = conn.prepareStatement(
                    "INSERT INTO bird (genus, name, age, is_male, is_flying, is_migrating) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");

            for (String[] line: strings) {
                if (line[0].equals("0")) {
                    mammalSt.setString(1, line[1]);
                    mammalSt.setString(2, line[2]);
                    mammalSt.setInt(3, Integer.parseInt(line[3]));
                    mammalSt.setBoolean(4, Boolean.parseBoolean(line[4]));
                    mammalSt.setBoolean(5, Boolean.parseBoolean(line[5]));
                    mammalSt.setBoolean(6, Boolean.parseBoolean(line[6]));
                    mammalSt.addBatch();
                } else {
                    birdSt.setString(1, line[1]);
                    birdSt.setString(2, line[2]);
                    birdSt.setInt(3, Integer.parseInt(line[3]));
                    birdSt.setBoolean(4, Boolean.parseBoolean(line[4]));
                    birdSt.setBoolean(5, Boolean.parseBoolean(line[5]));
                    birdSt.setBoolean(6, Boolean.parseBoolean(line[6]));
                    birdSt.addBatch();
                }
            }
            int[] stRes = mammalSt.executeBatch();
            int[] birdRes = birdSt.executeBatch();
            for (int num: stRes) {
                System.out.println(num);
            }

            for (int num: birdRes) {
                System.out.println(num);
            }
            cleaner.close();
            mammalSt.close();
            birdSt.close();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
        }
    }

    public static void demoQuery() {
        Connection conn = DbConnection.getConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM bird WHERE age > 1");
            while (rs.next()) {
                System.out.print(rs.getString("name"));
                System.out.print(" ");
                System.out.print(rs.getString("genus"));
                System.out.print(" ");
                System.out.println(rs.getInt("age"));
            }
            rs.close();
            st.close();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
        }
    }

    public static void dirtyReadDemo() {
        Runnable first = () -> {
            Connection conn1 = DbConnection.getNewConnection();
            if (conn1 != null) {
                try {
                    conn1.setAutoCommit(false);
                    conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement upd = conn1.createStatement();
                    upd.executeUpdate("UPDATE mammal SET is_male=false WHERE is_male=true");
                    Thread.sleep(2000);
                    conn1.rollback();
                    upd.close();
                    Statement st = conn1.createStatement();
                    System.out.println("In the first thread:");
                    ResultSet rs = st.executeQuery("SELECT * FROM mammal");
                    while (rs.next()) {
                        System.out.println(rs.getBoolean("is_male"));
                    }
                    st.close();
                    rs.close();
                    conn1.close();
                } catch (SQLException | InterruptedException throwable) {
                    throwable.printStackTrace();
                }
            }
        };

        Runnable second = () -> {
            Connection conn2 = DbConnection.getNewConnection();
            if (conn2 != null) {
                try {
                    Thread.sleep(500);
                    conn2.setAutoCommit(false);
                    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement st = conn2.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM mammal");
                    while (rs.next()) {
                        System.out.println(rs.getBoolean("is_male"));
                    }
                    rs.close();
                    st.close();
                    conn2.close();
                } catch (SQLException | InterruptedException throwable) {
                    throwable.printStackTrace();
                }
            }
        };
        Thread th1 = new Thread(first);
        Thread th2 = new Thread(second);
        th1.start();
        th2.start();
    }
}
