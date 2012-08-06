package simpledb;

/**
 * Base class for all SimpleDb test classes. 
 * @author nizam
 *
 */
public class SimpleDbTestBase {
	
	public SimpleDbTestBase() {
		try {
			setUp();
		} catch (Exception e) {
			System.out.println("Error setting up test base");
			System.exit(1);
		}
	}
	
	/**
	 * Reset the database before each test is run.
	 */
	public void setUp() throws Exception {					
		Database.reset();
	}
	
}
