package org.yolin.moviesRecommand;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import sun.nio.ch.IOUtil;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class step5 {
    public static Connection connection() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://localhost:3306/moviesRecommand?CharacterEncoding=UTF8";
        String user = "root";
        String password = "root";
        return DriverManager.getConnection(url, user, password);
    }

    public static void releaseConnection(Connection connection) throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    public static void insertData(PreparedStatement pstm, String out) throws SQLException {

        int row = 0;
        String[] line = out.split("\n");
        for (int j = 0; j < line.length; j++) {
            String[] predict = Recommand.DELIMITER.split(line[j]);
            for (int i = 0; i < 3; i++) {
                pstm.setString(i + 1, predict[i]);
            }
            row += pstm.executeUpdate();
        }
        System.out.println("共插入" + row + "条数据");
    }

    public static void run() throws SQLException, ClassNotFoundException, IOException {
        Connection conn = connection();
        String sql = "insert into result(userid,itemid,predictScore) value(?,?,?)";
        PreparedStatement pstm = conn.prepareStatement(sql);

        String input = "hdfs://127.0.0.1:9000/recommand/step4/part-r-00000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(input), conf);

        InputStream in = fs.open(new Path(input));

//        File tmp = new File("tmp");
//        PrintWriter pw = new PrintWriter(tmp);
        String out = IOUtils.toString(in);
        //System.out.println(out);
//        pw.write(out);
//        pw.flush();
        insertData(pstm,out);
        releaseConnection(conn);
    }
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        run();
    }
}
