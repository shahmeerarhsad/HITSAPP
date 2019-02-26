import java.sql.*;



public class HITConn {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String server = "10.173.1.46";
        String port = "1433";
        String user = "HITS_IDM";
        String password = "HITS_IDM";
        String database = "Mobilink";
        String jdbcurl="jdbc:sqlserver://10.173.1.46:1433;DatabaseName=new";
        Connection con = null;
        Connection conn = null;
        jdbcurl = "jdbc:sqlserver://"+ server + ":" +port + ";user=" + user +
                ";password=" +password + ";databasename=" + database + "";
        String db2url = "jdbc:db2://10.231.105.176:50005/maxdb71:" +
      		  "user=db2cmdb1;password=mobilink;";     
        int count = 1;
        String insertsql = "insert into maximo.hits (HITSID,HASLD,EMPLOYEEID,LOGON_NAME,DESIGNATION,DEPARTMENT,MANAGERNUMBER,MANAGERNAME,LOCATION,LOGON_NAME2,DESIGNATION2,DEPARTMENT2,MANAGERNUMBER2,MANAGERNAME2,LOCATION2) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
        String updatesql = "update maximo.hits SET LOGON_NAME2 = ? , DESIGNATION2 = ? , DEPARTMENT2 = ? , MANAGERNUMBER2 = ? , MANAGERNAME2 = ? , LOCATION2 = ? where EMPLOYEEID = ? ";
        try {
        	  // Load the IBM Data Server Driver for JDBC and SQLJ with DriverManager
        	  Class.forName("com.ibm.db2.jcc.DB2Driver");
        	} catch (ClassNotFoundException e) {
        	     e.printStackTrace();
        	}
     
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try{
        		
        		conn = DriverManager.getConnection(db2url); 
            con = DriverManager.getConnection(jdbcurl,user,password);
            Statement state = con.createStatement();
			PreparedStatement ps = conn.prepareStatement(insertsql);
			PreparedStatement up = conn.prepareStatement(updatesql);
            ResultSet srt = state.executeQuery("select * from dbo.v_IDM");
            	while(srt.next())
            	{
            		try {
            			ps.setString(1, srt.getString("EMPLOYEE_NUMBER"));
            			ps.setInt(2, 0 );
            			ps.setString(3,srt.getString("EMPLOYEE_NUMBER") );
            			ps.setString(4, srt.getString("LOGON_NAME"));
            			ps.setString(5,srt.getString("DESIGNATION"));
            			ps.setString(6, srt.getString("DEPARTMENT"));
            			ps.setString(7,srt.getString("MANAGERNUMBER"));
            			ps.setString(8,srt.getString("MANAGERNAME"));
            			ps.setString(9,srt.getString("LOCATION"));
            			ps.setString(10,srt.getString("LOGON_NAME"));
            			ps.setString(11,srt.getString("DESIGNATION"));
            			ps.setString(12,srt.getString("DEPARTMENT"));
            			ps.setString(13,srt.getString("MANAGERNUMBER"));
            			ps.setString(14,srt.getString("MANAGERNAME"));
            			ps.setString(15,srt.getString("LOCATION"));
            			ps.executeUpdate();
            			count++;
            		}
            		catch(SQLException e) {
            			if (e.getMessage().contains("SQLCODE=-803")){
            			up.setString(1, srt.getString("LOGON_NAME"));
            			up.setString(2, srt.getString("DESIGNATION"));
            			up.setString(3, srt.getString("DEPARTMENT"));
            			up.setString(4, srt.getString("MANAGERNUMBER"));
            			up.setString(5, srt.getString("MANAGERNAME"));
            			up.setString(6, srt.getString("LOCATION"));
            			up.setString(7, srt.getString("EMPLOYEE_NUMBER"));
            			up.executeUpdate();
    					}
            			}
            	}
            	ps.close();
            	up.close();
        		System.out.print(count);

        }catch(SQLException e){
            e.printStackTrace();
        }
        
	}

}
