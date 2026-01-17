package rs.raf.simpledb;

import net.sf.jsqlparser.JSQLParserException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class InitKolokvijumDB {

	public static boolean initDB(String dbName) {
		// Kreiranje baze podataka
		return SimpleDBEngine.init(dbName);
	}

	public static void createDBTables() throws JSQLParserException {

		String createTableSQL =
				"create table student(" +
						"sid int, studname varchar(35), smerid int, godstud int, godupis int, goddipl int)";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table student created.");

		createTableSQL =
				"create table smer(" +
						"smid int, smername varchar(25))";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table smer created.");

		createTableSQL =
				"create table predmet(" +
						"pid int, prednaziv varchar(25), predsmerid int, predgod int)";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table predmet created.");

		createTableSQL =
				"create table ispitnirok(" +
						"rokid int, isproknaziv varchar(25))";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table ispitnirok created.");

		createTableSQL =
				"create table ispit(" +
						"ispid int, predmetid int, ispitnirokid int, ispugod int, ispdatum varchar(25))";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table ispit created.");

		createTableSQL =
				"create table polaganje(" +
						"polagstudid int, ispitid int, ocena int)";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table polaganje created.");
	}

	public static void genericInsertDBData() {

		try {

			insertTableDataGeneric(
					"raf-simpledb-kolokvijumA/raf-simpledb-kolokvijumA/raf-simpledb/data/studenti.csv",
					"student",
					"insert into student(sid, studname, smerid, godstud, godupis, goddipl) values ",
					"(%s,'%s',%s,%s,%s,%s)",
					6
			);

			insertTableDataGeneric(
					"raf-simpledb-kolokvijumA/raf-simpledb-kolokvijumA/raf-simpledb/data/smer.csv",
					"smer",
					"insert into smer(smid, smername) values ",
					"(%s,'%s')",
					2
			);

			insertTableDataGeneric(
					"raf-simpledb-kolokvijumA/raf-simpledb-kolokvijumA/raf-simpledb/data/raf_predmeti.csv",
					"predmet",
					"insert into predmet(pid, prednaziv, predsmerid, predgod) values ",
					"(%s,'%s',%s,%s)",
					4
			);

			insertTableDataGeneric(
					"raf-simpledb-kolokvijumA/raf-simpledb-kolokvijumA/raf-simpledb/data/ispitni_rok.csv",
					"ispitnirok",
					"insert into ispitnirok(rokid, isproknaziv) values ",
					"(%s,'%s')",
					2
			);

			insertTableDataGeneric(
					"raf-simpledb-kolokvijumA/raf-simpledb-kolokvijumA/raf-simpledb/data/ispiti.csv",
					"ispit",
					"insert into ispit(ispid, predmetid, ispitnirokid, ispugod, ispdatum) values ",
					"(%s,%s,%s,%s,'%s')",
					5
			);

			insertTableDataGeneric(
					"raf-simpledb-kolokvijumA/raf-simpledb-kolokvijumA/raf-simpledb/data/polaganja.csv",
					"polaganje",
					"insert into polaganje(polagstudid, ispitid, ocena) values ",
					"(%s,%s,%s)",
					3
			);

		} catch (IOException | JSQLParserException e) {
			e.printStackTrace();
		}
	}

	private static void insertTableDataGeneric(
			String fileName,
			String tableName,
			String insertSQLString,
			String formatString,
			int argNums
	) throws IOException, JSQLParserException {

		File dataFile = new File(fileName);
		BufferedReader reader =
				new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));

		reader.readLine(); // preskace header

		while (true) {
			String dataLine = reader.readLine();
			if (dataLine == null)
				break;

			String[] dataFields = dataLine.split(",");
			String values = String.format(formatString, (Object[]) dataFields);

			MainQueryRunner.executeSQLUpdate(insertSQLString + values);
		}

		reader.close();
		System.out.println("Records for table " + tableName + " have been inserted.");
	}
}
