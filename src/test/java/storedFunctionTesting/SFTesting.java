package storedFunctionTesting;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import storedProcedureTesting.SPTesting;

public class SFTesting {

	Connection con = null;
	Statement stmt = null;
	ResultSet rs, rs1, rs2;
	CallableStatement cStmt;
	String getCustomerLevelSQL = "SELECT customerName, CASE WHEN creditLimit > 50000 THEN 'PLATINUM' "
			+ "WHEN creditLimit >= 10000 AND creditLimit <= 50000 THEN 'GOLD' "
			+ "WHEN creditLimit < 10000 THEN 'SILVER' END as customerLevel FROM customers";

	@BeforeClass
	void setUp() throws SQLException {
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", "MillionYearPicnic!0");

	}

	@AfterClass
	void tearDown() throws SQLException {
		con.close();
	}

	@Test(priority = 1)
	void test_storedFunctionExits() throws SQLException {
		stmt = con.createStatement();
		rs = stmt.executeQuery("SHOW FUNCTION STATUS WHERE db = 'classicmodels'");
		rs.next();

		Assert.assertEquals(rs.getString("Name"), "CustomerLevel");

	}

	@Test(priority = 2)
	void test_CustomerLevel_with_SQLStatement() throws SQLException {
		stmt = con.createStatement();
		rs1 = stmt.executeQuery("SELECT customerName, CustomerLevel(creditLimit) FROM customers;");

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery(getCustomerLevelSQL + ";");

		Assert.assertEquals(SPTesting.compareResultSets(rs1, rs2), true);

	}

	@Test(priority = 3)
	void test_GetCustomerLevelProcedure() throws SQLException {
		cStmt = con.prepareCall("{CALL GetCustomerLevel(?,?)}");
		cStmt.setInt(1, 131);
		cStmt.registerOutParameter(2, Types.VARCHAR);
		cStmt.executeQuery();
		
		String custLevel = cStmt.getString(2);

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery(getCustomerLevelSQL + " WHERE customerNumber = 131;");
		rs2.next();
		
		String exp_custLevel = rs2.getString("customerLevel");
		
		Assert.assertEquals(custLevel, exp_custLevel);

	}

}
