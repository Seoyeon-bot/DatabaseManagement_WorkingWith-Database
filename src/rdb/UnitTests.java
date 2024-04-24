package rdb;

import static org.junit.Assert.*;

import java.util.stream.Stream;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import bank.rdb.Bank;
import rdb.Relation.DuplicateKeyException;
import rdb.RelationSchema.DuplicateAttributeNameException;
import rdb.Tuple.TypeException;

/**
 * {@code UnitTests} tests the implementations in the {@code bank} package.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnitTests {

	/**
	 * A {@code RelationalDatabase} used for unit tests.
	 */
	RelationalDatabase database;

	/**
	 * A {@code RelationSchema} used for unit tests.
	 */
	RelationSchema schema;

	{
		database = new RelationalDatabase("Sample Bank");
		schema = database.createTable("customers").attribute("customerNumber", String.class)
				.attribute("zipCode", Integer.class).primaryKey("customerNumber");
		database.createTable("accounts").attribute("accountNumber", String.class)
				.attribute("customerNumber", String.class).attribute("balance", Double.class)
				.primaryKey("accountNumber");
		try {
			Bank.addData(database, 10);
		} catch (Exception e) {
		}
	}

	/**
	 * Tests the Task 1 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task1() throws Exception {
		var r = new RelationSchema();
		r.attribute("customerNumber", String.class);
		assertEquals("{customerNumber=java.lang.String}", r.toString());
		r.attribute("zipCode", Integer.class);
		assertEquals("{customerNumber=java.lang.String, zipCode=java.lang.Integer}", r.toString());
		assertThrows(DuplicateAttributeNameException.class, () -> r.attribute("zipCode", Integer.class));
	}

	/**
	 * Tests the Task 2 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task2() throws Exception {
		Tuple t = new Tuple(schema, "C10", 12224);
		assertEquals("{customerNumber=C10, zipCode=12224}", "" + t);
		t = new Tuple(schema, "C11", 12225);
		assertEquals("{customerNumber=C11, zipCode=12225}", "" + t);
		assertThrows(TypeException.class, () -> new Tuple(schema, "C11", "12225"));
	}

	/**
	 * Tests the Task 3 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task3() throws Exception {
		var database = new RelationalDatabase("Sample Bank");
		database.createTable("customers").attribute("customerNumber", String.class).attribute("zipCode", Integer.class)
				.primaryKey("customerNumber");
		Tuple t = database.addTuple("customers", "C10", 12224);
		assertEquals("{customerNumber=C10, zipCode=12224}", "" + t);
		assertThrows(DuplicateKeyException.class, () -> database.addTuple("customers", "C10", 12224));
		assertEquals("Sample Bank{customers={customerNumber=java.lang.String, zipCode=java.lang.Integer}:1}",
				"" + database);
	}

	/**
	 * Tests the Task 4 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task4() throws Exception {
		Stream<Tuple> result = database.select("*", "customers");
		var l = result.toList();
		assertEquals(10, l.size());
		assertEquals("{customerNumber=C00, zipCode=12222}", "" + l.get(0));
		assertEquals("{customerNumber=C09, zipCode=12223}", "" + l.get(9));
		result = database.select("*", "accounts");
		l = result.toList();
		assertEquals(21, l.size());
		assertEquals("{accountNumber=A00, customerNumber=C00, balance=1000.0}", "" + l.get(0));
		assertEquals("{accountNumber=A09, customerNumber=C04, balance=1000.0}", "" + l.get(9));
	}

	/**
	 * Tests the Task 5 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task5() throws Exception {
		Stream<Tuple> result = database.select("*", "accounts", "balance > 10000");
		var l = result.toList();
		assertEquals(7, l.size());
		assertEquals("{accountNumber=A02, customerNumber=C01, balance=100000.0}", "" + l.get(0));
		assertEquals("{accountNumber=A20, customerNumber=C09, balance=100000.0}", "" + l.get(6));
	}

	/**
	 * Tests the Task 6 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task6() throws Exception {
		Stream<Tuple> result = database.select("accountNumber, zipCode", "accounts natural join customers");
		var l = result.toList();
		assertEquals(21, l.size());
		assertEquals("{accountNumber=A00, zipCode=12222}", "" + l.get(0));
		assertEquals("{accountNumber=A20, zipCode=12223}", "" + l.get(20));

		result = database.select("zipCode", "accounts natural join customers", "accountNumber = \"A10\"");
		assertEquals("[{zipCode=12223}]", "" + result.toList());
		result = database.select("zipCode", "accounts natural join customers", "accountNumber = \"A11\"");
		assertEquals("[{zipCode=12223}]", "" + result.toList());
		result = database.select("zipCode", "accounts natural join customers", "accountNumber = \"A15\"");
		assertEquals("[{zipCode=12225}]", "" + result.toList());
	}

	/**
	 * Tests the Task 7 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task7a() throws Exception {
		var c = new Aggregate.Count("accountNumber", Integer.class);
		c.update(1);
		assertEquals("Count(accountNumber):1", "" + c);
		c.update(1);
		assertEquals("Count(accountNumber):2", "" + c);
	}

	/**
	 * Tests the Task 7 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task7b() throws Exception {
		var m = new Aggregate.Minimum("balance", Integer.class);
		m.update(1);
		assertEquals("Minimum(balance):1", "" + m);
		m.update(2);
		assertEquals("Minimum(balance):1", "" + m);
		m.update(0);
		assertEquals("Minimum(balance):0", "" + m);
	}

	/**
	 * Tests the Task 7 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task7c() throws Exception {
		Stream<Tuple> result = database.select("count(accountNumber) as count", "accounts");
		assertEquals("[{count=21}]", "" + result.toList());
		result = database.select("max(balance) as maxBalance", "accounts");
		assertEquals("[{maxBalance=100000.0}]", "" + result.toList());
	

		result = database.selectGroupBy("zipCode, count(customerNumber) as customerCount", "customers", "zipCode");
		var l = result.toList();
		assertEquals(4, l.size());
		assertEquals("{zipCode=12222, customerCount=3}", "" + l.get(0));
		assertEquals("{zipCode=12225, customerCount=2}", "" + l.get(3));

		result = database.selectGroupBy("zipCode, count(accountNumber) as accountCount",
				"accounts natural join customers", "zipCode");
		l = result.toList();
		assertEquals(4, l.size());
		assertEquals("{zipCode=12222, accountCount=6}", "" + l.get(0));
		assertEquals("{zipCode=12225, accountCount=4}", "" + l.get(3));

		result = database.with("t", "min(balance) as balance", "accounts").select("accountNumber, balance",
				"accounts natural join t");
		l = result.toList();
		assertEquals(7, l.size());
		assertEquals("{accountNumber=A00, balance=1000.0}", "" + l.get(0));
		assertEquals("{accountNumber=A09, balance=1000.0}", "" + l.get(3));
	}

}
