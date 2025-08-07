package storedProcedureTesting;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.Strings;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SPTesting {

	Connection con = null;
	Statement stmt = null;
	ResultSet rs, rs1, rs2;
	CallableStatement cStmt;

	@BeforeClass
	void setUp() throws SQLException {
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", "MillionYearPicnic!0");

	}

	@AfterClass
	void tearDown() throws SQLException {
		con.close();
	}

	@Test(priority = 1)
	void test_storedProceduresExists() throws SQLException {
		stmt = con.createStatement();
		rs = stmt.executeQuery("SHOW PROCEDURE STATUS WHERE Name = 'SelectAllCustomers'");
		rs.next();

		Assert.assertEquals(rs.getString("Name"), "SelectAllCustomers");

	}

	@Test(priority = 2)
	void test_SelectAllCustomers() throws SQLException {
		cStmt = con.prepareCall("{CALL SelectAllCustomers()}");
		rs1 = cStmt.executeQuery();

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery("SELECT * from Customers;");

		Assert.assertEquals(compareResultSets(rs1, rs2), true);

	}

	@Test(priority = 3)
	void test_SelectAllCustomersByCity() throws SQLException {
		cStmt = con.prepareCall("{CALL SelectAllCustomersByCity(?)}");
		cStmt.setString(1, "Singapore");
		rs1 = cStmt.executeQuery();

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery("SELECT * from Customers WHERE city = 'Singapore';");

		Assert.assertEquals(compareResultSets(rs1, rs2), true);

	}

	@Test(priority = 4)
	void test_SelectAllCustomersByCityAndPinCode() throws SQLException {
		cStmt = con.prepareCall("{CALL SelectAllCustomersByCityAndPin(?,?)}");
		cStmt.setString(1, "Singapore");
		cStmt.setString(2, "079903");
		rs1 = cStmt.executeQuery();

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery("SELECT * from Customers WHERE city = 'Singapore' AND postalCode = '079903';");

		Assert.assertEquals(compareResultSets(rs1, rs2), true);

	}

	@Test(priority = 5)
	void test_GetOrderByCustomer() throws SQLException {
		cStmt = con.prepareCall("{CALL GetOrderByCustomer(?,?,?,?,?)}");
		cStmt.setInt(1, 141);
		cStmt.registerOutParameter(2, Types.INTEGER);
		cStmt.registerOutParameter(3, Types.INTEGER);
		cStmt.registerOutParameter(4, Types.INTEGER);
		cStmt.registerOutParameter(5, Types.INTEGER);
		cStmt.executeQuery();

		int shipped = cStmt.getInt(2);
		int canceled = cStmt.getInt(3);
		int resolved = cStmt.getInt(4);
		int disputed = cStmt.getInt(5);

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery("SELECT "
				+ "(SELECT count(*) as 'shipped' FROM orders WHERE customerNumber = 141 AND status = 'Shipped') AS Shipped,"
				+ "(SELECT count(*) as 'canceled' FROM orders WHERE customerNumber = 141 AND status = 'Canceled') AS Canceled,"
				+ "(SELECT count(*) as 'resolved' FROM orders WHERE customerNumber = 141 AND status = 'Resolved') AS Resolved,"
				+ "(SELECT count(*) as 'disputed' FROM orders WHERE customerNumber = 141 AND status = 'Disputed') AS Disputed;");

		rs2.next();

		int exp_shipped = rs2.getInt("shipped");
		int exp_canceled = rs2.getInt("canceled");
		int exp_resolved = rs2.getInt("resolved");
		int exp_disputed = rs2.getInt("disputed");

		if (shipped == exp_shipped && canceled == exp_canceled && resolved == exp_resolved
				&& disputed == exp_disputed) {
			Assert.assertTrue(true);
		} else {
			Assert.assertTrue(false);
		}

	}

	@Test(priority = 6, dataProvider = "customerNumbers")
	void test_GetCustomerShipping(int customerNumber) throws SQLException {
		cStmt = con.prepareCall("{CALL GetCustomerShipping(?,?)}");
		cStmt.setInt(1, customerNumber);
		cStmt.registerOutParameter(2, Types.VARCHAR);
		cStmt.executeQuery();

		String shippingTime = cStmt.getString(2);

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery("SELECT country," + "CASE " + "WHEN country = 'USA' THEN '2-day Shipping'"
				+ "WHEN country = 'Canada' THEN '3-day Shipping'" + "ELSE '5-day Shipping'" + "END as ShippingTime "
				+ "FROM customers WHERE customerNumber = " + customerNumber + ";");

		rs2.next();

		String exp_ShippingTime = rs2.getString("ShippingTime");

		Assert.assertEquals(shippingTime, exp_ShippingTime);

	}

	@DataProvider(name = "customerNumbers")
	public Object[][] provideTestData() {
		return new Object[][] { { 112 }, { 260 }, { 353 } };
	}

	public static boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException {
		while (resultSet1.next()) {
			resultSet2.next();
			int count = resultSet1.getMetaData().getColumnCount();
			for (int i = 1; i <= count; i++) {
				if (!Strings.CS.equals(resultSet1.getString(i), resultSet2.getString(i))) {
					return false;
				}

			}
		}
		return true;

	}
}
