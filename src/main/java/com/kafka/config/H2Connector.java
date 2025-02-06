package com.kafka.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2Connector {
	private static final Logger LOG = LoggerFactory.getLogger(H2Connector.class);
	private static final String JDBC_URL = "jdbc:h2:~/data/demo3";
	private static final String USER = "sa";
	private static final String PASSWORD = "";
	private static Connection connection; // ToDO: implement pool of connections or per thread

	public static Connection connect() {
		try {
			connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			if (connection != null && !connection.isClosed()) {
				LOG.info("Connected to H2 Database...");
				// createTable(connection);
				// insertData(connection);
				// fetchData(connection);
				return connection;
			} else {
				LOG.error("Failed to establish a connection to H2 Database...");
				throw new SQLException("Failed to establish a connection to H2 Database...");
			}
		} catch (SQLException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public static void closeConnection() {
		if (connection != null) {
			try {
				connection.close();
				LOG.info("H2 Connection closed.");
			} catch (SQLException e) {
				LOG.error("Error closing connection: " + e.getMessage());
			}
		}
	}

	private static void createTable() throws SQLException {
		String dropTable = "DROP TABLE IF EXISTS storeData";
		String createTableSQL = """
				     CREATE TABLE storeData (
				         id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
				         companyName VARCHAR(100) NOT NULL,
				         productName VARCHAR(100) NOT NULL
				     );
				""";
		try (Statement statement = connection.createStatement()) {
			statement.execute(dropTable);
			statement.execute(createTableSQL);
			LOG.info("Table 'storeData' created...");
		}
	}

	private static void insertData() throws SQLException {
		String insertSQL = "INSERT INTO storeData (companyName, productName) VALUES (?, ?)";

		try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
			String[][] data = { { "Microsoft", "Windows 11" }, { "Apple", "MacBook Pro" }, { "Google", "Pixel 8" },
					{ "Amazon", "Echo Dot" }, { "Facebook", "Meta Quest 3" }, { "Tesla", "Model S" },
					{ "IBM", "Watson AI" }, { "Intel", "Core i9" }, { "AMD", "Ryzen 9" }, { "NVIDIA", "RTX 4090" },
					{ "Samsung", "Galaxy S23" }, { "Sony", "PlayStation 5" }, { "LG", "OLED TV" }, { "Dell", "XPS 15" },
					{ "HP", "Spectre x360" }, { "Lenovo", "ThinkPad X1" }, { "Cisco", "Webex" },
					{ "Oracle", "Database 21c" }, { "Salesforce", "CRM Cloud" }, { "SAP", "S/4HANA" },
					{ "Adobe", "Photoshop" }, { "Dropbox", "Cloud Storage" }, { "Spotify", "Premium Subscription" },
					{ "Netflix", "Streaming Service" }, { "Zoom", "Video Conferencing" }, { "Uber", "Ride Sharing" },
					{ "Airbnb", "Vacation Rentals" }, { "Twitter", "X Premium" }, { "Snapchat", "Spectacles" },
					{ "Red Hat", "Enterprise Linux" }, { "VMware", "vSphere" }, { "Palo Alto Networks", "Firewalls" },
					{ "McAfee", "Antivirus" }, { "Bitdefender", "Total Security" }, { "Fortinet", "FortiGate" },
					{ "Zscaler", "Cloud Security" }, { "Atlassian", "Jira Software" }, { "Slack", "Messaging App" },
					{ "Trello", "Task Management" }, { "GitHub", "Enterprise Cloud" }, { "GitLab", "DevOps Platform" },
					{ "Docker", "Container Engine" }, { "Kubernetes", "Cluster Orchestration" },
					{ "OpenAI", "ChatGPT" }, { "SpaceX", "Starlink" }, { "Boeing", "787 Dreamliner" },
					{ "Ford", "Mustang Mach-E" }, { "General Motors", "Chevrolet Bolt" }, { "Hyundai", "Ioniq 5" },
					{ "Nissan", "Leaf EV" }, { "Rivian", "R1T Truck" } };
			connection.setAutoCommit(false);
			for (String[] entry : data) {
				preparedStatement.setString(1, entry[0]);
				preparedStatement.setString(2, entry[1]);
				preparedStatement.executeUpdate();
				LOG.info("entry[0]" + entry[0] + "entry[1]" + entry[1]);
				connection.commit();
			}
			LOG.info(" 50 rows inserted successfully...");
		}
	}

	public static ResultSet fetchData() {
		String querySQL = "SELECT * FROM storeData";
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(querySQL);
			return resultSet;
//        	LOG.info("Fetching Data from H2 Database...");
//            while (resultSet.next()) {
//            	int id = resultSet.getInt("id");
//                String companyName = resultSet.getString("companyName");
//                String productName = resultSet.getString("productName");
//                LOG.info(" ID: {} | Company: {} | Product: {}: ", id, companyName, productName);
//            }
		} catch (SQLException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

}
