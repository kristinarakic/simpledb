package rs.raf.simpledb;

import net.sf.jsqlparser.JSQLParserException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class InitKolokvijumDB {
	
	public static boolean initDB(String dbName) {
		// Kreiranje baze podataka, sto podrazumeva fajl sa podacima i podatke u sistemskom katalogu
		return SimpleDBEngine.init(dbName);
	}
	
	public static void createDBTables() throws JSQLParserException {
		String createTableSQL = "create table STUDENT(sid int, studname varchar(35), smerId int, godStud int, godUpis int, godDipl int)";
		
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table STUDENT created.");
		
		
		createTableSQL = "create table SMER(smid int, smerName varchar(25))";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table SMER created.");
		
		
		createTableSQL = "create table PREDMET(pid int, predNaziv varchar(25), predSmerId int, predGod int)";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table PREDMET created.");
		
		
		createTableSQL = "create table ISPITNIROK(rokId int, ispRokNaziv varchar(25))";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table ISPITNI ROK created.");
		
		
		createTableSQL = "create table ISPIT(ispid int, predmetid int, ispitniRokId int, ispUGod int, ispDatum varchar(25))";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table ISPIT created.");
		
		
		createTableSQL = "create table POLAGANJE(polagStudId int, ispitId int, ocena int)";
		MainQueryRunner.executeSQLUpdate(createTableSQL);
		System.out.println("Table POLAGANJE created.");
		
				
	}
	
	public static void genericInsertDBData() {
		
		try {
			
			insertTableDataGeneric("raf-simpledb/data/studenti.csv", "STUDENT",
					"insert into STUDENT(sid, studname, smerId, godStud, godUpis, godDipl) values ", 
					"(%s,'%s',%s,%s,%s,%s)", 6);
		
			
			insertTableDataGeneric("raf-simpledb/data/smer.csv", "SMER",
					"insert into SMER(smid, smername) values ", 
					"(%s,'%s')", 2);
			
			insertTableDataGeneric("raf-simpledb/data/raf_predmeti.csv", "PREDMET",
					"insert into PREDMET(pid, predNaziv, predSmerId, predGod) values ", 
					"(%s,'%s',%s,%s)", 4);
			
			
			insertTableDataGeneric("raf-simpledb/data/ispitni_rok.csv", "ISPITNIROK",
					"insert into ISPITNIROK(rokId, ispRokNaziv) values ", 
					"(%s,'%s')", 2);	
			
			
			insertTableDataGeneric("raf-simpledb/data/ispiti.csv", "ISPIT",
					"insert into ISPIT(ispid, predmetid, ispitniRokId, ispUGod, ispDatum) values ", 
					"(%s,%s,%s,%s,'%s')", 5);			
			
			
			insertTableDataGeneric("raf-simpledb/data/polaganja.csv", "POLAGANJE",
					"insert into POLAGANJE(polagStudId, ispitId, ocena) values ", 
					"(%s,%s,%s)", 3);
			

		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSQLParserException e) {
            throw new RuntimeException(e);
        }


    }
	
	private static void insertTableDataGeneric(String fileName, String tableName, String insertSQLString, String formatString, int argNums) throws IOException, JSQLParserException {
		
		File dataFile = new File(fileName);
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));
		reader.readLine(); // preskace se prva linija sa nazivima kolona
		while(true) {
			String dataLine = reader.readLine();
			if (dataLine == null)
				break;
			String[] dataFields = dataLine.split(",");
			String values = String.format(formatString, (Object[]) dataFields);
			
			
			MainQueryRunner.executeSQLUpdate(insertSQLString + values);
		}
		reader.close();
		System.out.println("Records for table "+tableName+" has been inserted.");
	}
}
