package com.dp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class Processor {

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/employees";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "password";

    public static void main(String[] args) {
        System.out.println("Invoked Spark");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Data Processing");
        JavaSparkContext sc = new JavaSparkContext(conf);

        DbConnection dbConnection = new DbConnection(MYSQL_DRIVER, MYSQL_CONNECTION_URL, MYSQL_USERNAME, MYSQL_PWD);

        JdbcRDD<Object[]> jdbcRDDOne =
                new JdbcRDD<>(sc.sc(), dbConnection,
                        "select * from personsOne where id >= ? and id <= ?",
                        0,
                        2,
                        1, new MapResult(), ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        JdbcRDD<Object[]> jdbcRDDTwo =
                new JdbcRDD<>(sc.sc(), dbConnection,
                        "select * from personsTwo where id >= ? and id <= ?",
                        0,
                        2,
                        1, new MapResult(), ClassManifestFactory$.MODULE$.fromClass(Object[].class));


        JavaRDD<Object[]> javaRDDOne = JavaRDD.fromRDD(jdbcRDDOne, ClassManifestFactory$.MODULE$.fromClass(Object[].class));
        JavaRDD<Object[]> javaRDDTwo = JavaRDD.fromRDD(jdbcRDDTwo, ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        System.out.println("javaRDDOne: =================" + javaRDDOne.collect().size());
        System.out.println("javaRDDTwo: =================" + javaRDDTwo.collect().size());

        ;
        System.out.println("Union Result: =================" + javaRDDOne.union(javaRDDTwo).collect().size());

        JavaRDD<Object[]> subtract = javaRDDOne.subtract(javaRDDTwo);
        System.out.println("Subtract Result: =================" + subtract.collect().size());


    }

    static class DbConnection extends AbstractFunction0<Connection> implements Serializable {

        private String driverClassName;
        private String connectionUrl;
        private String userName;
        private String password;

        DbConnection(String driverClassName, String connectionUrl, String userName, String password) {
            this.driverClassName = driverClassName;
            this.connectionUrl = connectionUrl;
            this.userName = userName;
            this.password = password;
        }

        @Override
        public Connection apply() {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            Properties properties = new Properties();
            properties.setProperty("user", userName);
            properties.setProperty("password", password);

            Connection connection = null;
            try {
                connection = DriverManager.getConnection(connectionUrl, properties);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return connection;
        }
    }

    static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {

        public Object[] apply(ResultSet row) {
            return JdbcRDD.resultSetToObjectArray(row);
        }
    }

}