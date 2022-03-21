import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;
public class HW2_HBase{
    public static void insertData(Connection connection, TableName tableName, String row, String colFamily, String col, String cell) throws IOException{
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(cell));

        connection.getTable(tableName).put(put);
    }
    public static boolean findNamespace(Admin admin, String ns) throws IOException {
        NamespaceDescriptor[] nsDescriptors = admin.listNamespaceDescriptors();
        boolean find_ns = false;
        for (NamespaceDescriptor nsDescriptor : nsDescriptors) {
            if (nsDescriptor.getName().equals(ns)) {
                return true;
            }
        }
        return false;
    }
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        String ns = "zhangj";
        String tb = "student";

        if (!findNamespace(admin, ns)) {
            NamespaceDescriptor nsDescriptor = NamespaceDescriptor.create(ns).build();
            admin.createNamespace(nsDescriptor);
            System.out.println("Namespace create successful");
        } else {
            System.out.println("Namespace " + ns + " already exists");
        }

        TableName tableName = TableName.valueOf(ns + ":" + tb);
        String[] colFamilies = {"info", "score"};
        if (admin.tableExists(tableName)) {
            System.out.println("Table " + tb + " already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String colFamily : colFamilies) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        insertData(conn, tableName, "Tom", "info", "student_id", "20210000000001");
        insertData(conn, tableName, "Tom", "info", "class", "1");
        insertData(conn, tableName, "Tom", "score", "understanding", "75");
        insertData(conn, tableName, "Tom", "score", "programming", "82");

        insertData(conn, tableName, "Jerry", "info", "student_id", "20210000000002");
        insertData(conn, tableName, "Jerry", "info", "class", "1");
        insertData(conn, tableName, "Jerry", "score", "understanding", "85");
        insertData(conn, tableName, "Jerry", "score", "programming", "67");

        insertData(conn, tableName, "Jack", "info", "student_id", "20210000000003");
        insertData(conn, tableName, "Jack", "info", "class", "2");
        insertData(conn, tableName, "Jack", "score", "understanding", "80");
        insertData(conn, tableName, "Jack", "score", "programming", "80");

        insertData(conn, tableName, "Rose", "info", "student_id", "20210000000004");
        insertData(conn, tableName, "Rose", "info", "class", "2");
        insertData(conn, tableName, "Rose", "score", "understanding", "60");
        insertData(conn, tableName, "Rose", "score", "programming", "61");

        insertData(conn, tableName, "zjx", "info", "student_id", "G20200343130026");
        insertData(conn, tableName, "zjx", "info", "class", "1");
        System.out.println("Data insertion success");

        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        while ((result = scanner.next()) != null) {
            List<Cell> cells = result.listCells();
            System.out.print(Bytes.toString(CellUtil.cloneRow(cells.get(0))) + " : ");
            for (Cell cell : cells) {
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + " <" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "> ");
            }
            System.out.println();
        }

        Delete delete = new Delete(Bytes.toBytes("Jerry"));
        conn.getTable(tableName).delete(delete);
        System.out.println("Jerry deletion Success");
        
        Get get = new Get(Bytes.toBytes("Rose"));
        if (!get.isCheckExistenceOnly()) {
            Result result1 = conn.getTable(tableName).get(get);
            System.out.print("FIND Rose : ");
            for (Cell cell : result1.rawCells()) {
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + " <" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "> ");
            }
            System.out.println("Data get success");
        }

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }

        if (findNamespace(admin, ns)) {
            admin.deleteNamespace(ns);
            System.out.println("Namespace Delete Successful");
        } else {
            System.out.println("Namespace does not exist!");
        }
    }
}
