package rs.raf.simpledb;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import rs.raf.simpledb.parse.QueryData;
import rs.raf.simpledb.query.*;
import rs.raf.simpledb.query.aggregation.AggregationFn;
import rs.raf.simpledb.query.aggregation.CountFn;
import rs.raf.simpledb.query.aggregation.GroupByPlan;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.record.Schema;
import rs.raf.simpledb.tx.Transaction;


import static rs.raf.simpledb.InitKolokvijumDB.*;


public class MainQueryRunner {

	
	public static void main(String[] args) {
		try {
			
			boolean isnew = initDB("fakultet10");
			
			if (isnew)
            {
                createDBTables();
                genericInsertDBData();
            }
			else
				System.out.println("Baza fakultet10 is already created!");
			
			/*
			Šema baze je sledeca:
			
			STUDENT(sid int, sname varchar(25), smerid int, goddipl int)
			SMER(smid int, smerName varchar(25))
			PREDMET(pid int, naziv varchar(25), smerid int)
			*/
			
//			queryManualPlan1();
//
//			queryManualPlan2();
//
//			querySQL1();

//
   		    queryOptimizedManualPlan();
//
 			queryOptimizedManualPlanSMJ();
//
// 			queryBasePlan();

            queryBasePlanByParser();
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void queryManualPlan1() {
		
		/*
		 * Ovaj primer pokazuje manuelno kreiranje plana izvrsavanja upita.
		 * Zelimo da prikazemo sve studente koji su diplomirali 2025. godine.
		 * Ekvivalentan SQL upit bi bio:
		 * 	SELECT sname, goddipl 
	       	FROM STUDENT
	        WHERE goddipl=2025
	        	        
	        Obratiti paznju kako se kreiraju operatori Selekcije i Projekcije
	  	 * 
		 */
		Transaction tx = new Transaction();
		Plan p1 = new TablePlan("student", tx);
				
		
		// Kreiranje predikatskog uslova 'goddipl=2025'
		Expression lhs1 = new FieldNameExpression("goddipl");
		Constant c = new IntConstant(2025);
		Expression rhs1 = new ConstantExpression(c);
		Term t1 = new Term(lhs1, rhs1);
		Predicate pred1 = new Predicate(t1); //goddipl=2025  (SI)
				
		Plan p3 = new SelectionPlan(p1, pred1);
		
					
		List<String> fields = Arrays.asList("sname", "goddipl");
		Plan p4 = new ProjectionPlan(p3, fields);
		
		Scan s = p4.open();
		
		System.out.println("\nStudenti koji su diplomiali 2025:");
		System.out.println("\nStudent\t\t\tGodina diplomiranja");
		System.out.println("-----------------------------------------");
		while (s.next()) {
			String sname = s.getString("sname"); 	//SimpleDB cuva naziv kolona 
			int god = s.getInt("goddipl"); 			//sa malim slovima (lower case)
			System.out.println(sname + "\t\t" + god);
		}
		System.out.println();
		s.close();
		tx.commit();
		
	}

	private static void queryManualPlan2() {
		
		/*
		 * Ovaj primer pokazuje manuelno kreiranje plana izvrsavanja upita.
		 * Zelimo da prikazemo sve studente sa smera Softversko inzenjerstvo.
		 * Ekvivalentan SQL upit bi bio:
		 * 	SELECT sname, smerName 
	       	FROM STUDENT, SMER
	        WHERE smerId = SMId
	        AND	smerId = 1
	        
	        Obratiti paznju da se equi-join izvodi kao:
	        SELEKCIJAuslov_spajanja(DEKARTOV_PROIZVOD(tabela1, tabela2))
	  	 * 
		 */
		Transaction tx = new Transaction();
		Plan p1 = new TablePlan("student", tx);
		Plan p2 = new TablePlan("smer", tx);

		// Kreiranje predikatskog uslova 'smerid=1'
		Expression lhs1 = new FieldNameExpression("smerid");
		Constant c = new IntConstant(1);
		Expression rhs1 = new ConstantExpression(c);
		Term t1 = new Term(lhs1, rhs1);
		Predicate pred1 = new Predicate(t1); //smerid=1  (SI)
				
		Plan p3 = new SelectionPlan(p1, pred1);
		
		Plan p4 = new CrossProductPlan(p3, p2);
		
		// Kreiranje predikatskog uslova 'smerid=smid', sto je uslov spajanja/join-a
		Expression lhs2 = new FieldNameExpression("smerid");
		Expression rhs2 = new FieldNameExpression("smid");
		Term t2 = new Term(lhs2, rhs2);
		Predicate pred2 = new Predicate(t2); //smerid=smid  - Join uslov
		
		Plan p5 = new SelectionPlan(p4, pred2);

		List<String> fields = Arrays.asList("sname", "smername");
		Plan p6 = new ProjectionPlan(p5, fields);
		
		Scan s = p6.open();
		
		System.out.println("\nStudenti sa smera Softversko inzenjerstvo:");
		System.out.println("\nStudent\t\t\tSmer");
		System.out.println("-----------------------------------------");
		while (s.next()) {
			String sname = s.getString("sname"); 	//SimpleDB cuva naziv kolona 
			String dname = s.getString("smername"); //sa malim slovima (lower case)
			System.out.println(sname + "\t\t" + dname);
		}
		System.out.println();
		s.close();
		tx.commit();

	}

	// optimizovani plan
	private static void queryOptimizedManualPlan() {

		System.out.println("OPTIMIZOVANI PLAN");

		Transaction tx = new Transaction();

		// ============================================================
		// 1) BASE TABLE PLANS
		// ============================================================
		Plan student   = new TablePlan("student", tx);
		Plan predmet   = new TablePlan("predmet", tx);
		Plan ispit     = new TablePlan("ispit", tx);
		Plan polaganje = new TablePlan("polaganje", tx);

		// (opcioni debug)
		System.out.println("predmeta po godini: " + predmet.distinctValues("predgod"));
		System.out.println("polaganje ocena:     " + polaganje.distinctValues("ocena"));
		System.out.println("ispit predmetid:     " + ispit.distinctValues("predmetid"));
		System.out.println("polaganje ispitid:   " + polaganje.distinctValues("ispitid"));

		// ============================================================
		// 2) σ PUSH-DOWN FILTERS
		// ============================================================
		Plan predmet_predgod1 = new SelectionPlan(
				predmet,
				new Predicate(new Term(
						new FieldNameExpression("predgod"),
						new ConstantExpression(new IntConstant(1))
				))
		);

		Plan polaganje_ocena10 = new SelectionPlan(
				polaganje,
				new Predicate(new Term(
						new FieldNameExpression("ocena"),
						new ConstantExpression(new IntConstant(10))
				))
		);

		// ============================================================
		// 3) π PUSH-DOWN PROJECTIONS (SUŽAVANJE)
		// ============================================================
		Plan pred1 = new ProjectionPlan(predmet_predgod1, Arrays.asList("pid"));
		Plan isp   = new ProjectionPlan(ispit, Arrays.asList("ispid", "predmetid"));
		Plan pol10 = new ProjectionPlan(polaganje_ocena10, Arrays.asList("polagstudid", "ispitid", "ocena"));

		// ============================================================
		// 4) LEFT-DEEP JOIN #1:  J1 = σ(predmetid=pid)(PRED1 × ISP)
		// ============================================================
		Plan j1 = new SelectionPlan(
				new CrossProductPlan(pred1, isp),
				new Predicate(new Term(
						new FieldNameExpression("predmetid"),
						new FieldNameExpression("pid")
				))
		);

		// ============================================================
		// 5) LEFT-DEEP JOIN #2:  J2 = σ(ispitid=ispid)(J1 × POL10)
		// ============================================================
		Plan j2 = new SelectionPlan(
				new CrossProductPlan(j1, pol10),
				new Predicate(new Term(
						new FieldNameExpression("ispitid"),
						new FieldNameExpression("ispid")
				))
		);

		// ============================================================
		// 6) π PRE GROUP BY (KRITIČNO: nosi minimum)
		// ============================================================
		Plan beforeGroup = new ProjectionPlan(j2, Arrays.asList("polagstudid", "ocena"));

		// ============================================================
		// 7) GROUP BY polagstudid + COUNT(ocena)
		// ============================================================
		Collection<String> groupBy = new ArrayList<String>();
		groupBy.add("polagstudid");

		Collection<AggregationFn> aggs = new ArrayList<AggregationFn>();
		AggregationFn cntOcena = new CountFn("ocena");
		aggs.add(cntOcena);

		Plan grouped = new GroupByPlan(beforeGroup, groupBy, aggs, tx);
		String countField = cntOcena.fieldName(); // npr "countofocena"

		// ============================================================
		// 8) LATE JOIN sa STUDENT:
		//    σ(polagstudid=sid)(G × π(sid,studname)(STUDENT))
		// ============================================================
		Plan studentNarrow = new ProjectionPlan(student, Arrays.asList("sid", "studname"));

		Plan withStudent = new SelectionPlan(
				new CrossProductPlan(grouped, studentNarrow),
				new Predicate(new Term(
						new FieldNameExpression("polagstudid"),
						new FieldNameExpression("sid")
				))
		);

		// ============================================================
		// 9) π PRE SORT (sort ne nosi višak)
		// ============================================================
		Plan beforeSort = new ProjectionPlan(withStudent, Arrays.asList("studname", countField));

		// ============================================================
		// 10) ORDER BY countField (ASC u SortPlan)
		// ============================================================
		Plan sorted = new SortPlan(beforeSort, Arrays.asList(countField), tx);

		// ============================================================
		// 11) FINAL PROJECTION
		// ============================================================
		Plan finalPlan = new ProjectionPlan(sorted, Arrays.asList(countField, "studname"));

		// ============================================================
		// 12) EXECUTE + TIMING
		// ============================================================
		System.out.println("\nLOGIČKI PLAN UPITA (LEFT-DEEP, optimizovan):");
		finalPlan.printPlan(0);

		long t0 = System.currentTimeMillis();

		Scan s = finalPlan.open();
		int rows = 0;

		System.out.println("\nStudent\t\t\tDesetke");
		System.out.println("-----------------------------------------");
		while (s.next()) {
			rows++;
			String name = s.getString("studname");
			int desetke = s.getInt(countField);
			System.out.println(name + "\t\t" + desetke);
		}
		s.close();

		long t1 = System.currentTimeMillis();
		System.out.println("\nRows=" + rows + ", time(ms)=" + (t1 - t0));
		System.out.println("Blocks accessed estimate: " + finalPlan.blocksAccessed());
		System.out.println("Records output estimate:  \n" + finalPlan.recordsOutput());

		tx.commit();
	}

	// zamenjen cross join sa sort merge-join-om
	private static void queryOptimizedManualPlanSMJ() {
		System.out.println("OPTIMIZOVANI PLAN SORT-MERGE JOIN");

		Transaction tx = null;

		try {
			tx = new Transaction();

			// ============================================================
			// 1) TABLE PLAN-ovi
			// ============================================================
			Plan pStudent   = new TablePlan("student", tx);
			Plan pPredmet   = new TablePlan("predmet", tx);
			Plan pIspit     = new TablePlan("ispit", tx);
			Plan pPolaganje = new TablePlan("polaganje", tx);

			// ============================================================
			// 2) σ push-down filteri
			// ============================================================
			// PREDMET.predgod = 1
			String predgod = "predgod";
			Predicate predPredgod1 = new Predicate(
					new Term(new FieldNameExpression(predgod),
							new ConstantExpression(new IntConstant(1)))
			);
			Plan predmet_predgod1 = new SelectionPlan(pPredmet, predPredgod1);

			// POLAGANJE.ocena = 10
			String ocena = "ocena";
			Predicate predOcena10 = new Predicate(
					new Term(new FieldNameExpression(ocena),
							new ConstantExpression(new IntConstant(10)))
			);
			Plan polaganje_ocena10 = new SelectionPlan(pPolaganje, predOcena10);

			// ============================================================
			// 3) π push-down projekcije
			// ============================================================
			String pid = "pid";
			Plan pred1_pid = new ProjectionPlan(predmet_predgod1, java.util.List.of(pid));

			String ispid = "ispid";
			String predmetid = "predmetid";
			Plan ispit_narrow = new ProjectionPlan(pIspit, java.util.List.of(ispid, predmetid));

			String polagstudid = "polagstudid";
			String ispitid = "ispitid";
			Plan pol10_narrow = new ProjectionPlan(polaganje_ocena10, java.util.List.of(polagstudid, ispitid, ocena));

			// ============================================================
			// 4) JOIN #1: PRED1(pid) ⋈ ISPIT(ispid,predmetid)  ON pid = predmetid
			// ============================================================
			Plan j1 = new SortMergeJoinPlan(pred1_pid, ispit_narrow, pid, predmetid, tx);

			// ============================================================
			// 5) JOIN #2: J1 ⋈ POL10  ON ispid = ispitid
			// ============================================================
			Plan j2 = new SortMergeJoinPlan(j1, pol10_narrow, ispid, ispitid, tx);

			// ============================================================
			// 6) π PRE GROUP BY  (sid + ocena)
			// ============================================================
			Plan beforeGroup = new ProjectionPlan(j2, java.util.List.of(polagstudid, ocena));

			// ============================================================
			// 7) GROUP BY polagstudid + COUNT(ocena)
			// ============================================================
			java.util.Collection<String> groupFields = new java.util.ArrayList<>();
			groupFields.add(polagstudid);

			java.util.Collection<AggregationFn> aggFns = new java.util.ArrayList<>();
			AggregationFn cntOcena = new CountFn(ocena);
			aggFns.add(cntOcena);

			Plan grouped = new GroupByPlan(beforeGroup, groupFields, aggFns, tx);
			String countField = cntOcena.fieldName(); // npr. "countofocena"

			// ============================================================
			// 8) Late join sa STUDENT: G ⋈ π(sid,studname)(STUDENT) ON polagstudid = sid
			// ============================================================
			String sid = "sid";
			String studname = "studname";
			Plan student_narrow = new ProjectionPlan(pStudent, java.util.List.of(sid, studname));

			Plan joinedStudent = new SortMergeJoinPlan(grouped, student_narrow, polagstudid, sid, tx);

			// ============================================================
			// 9) π PRE SORT
			// ============================================================
			Plan beforeSort = new ProjectionPlan(joinedStudent, java.util.List.of(studname, countField));

			// ============================================================
			// 10) ORDER BY countField
			// NOTE: vaš SortPlan sortira ASC. Zahtev je "najviše desetki prvo" => DESC.
			// Najlakši test: sortiraj ASC pa ispiši obrnuto (materijalizuj rezultate u listu).
			// ============================================================
			Plan sortedAsc = new SortPlan(beforeSort, java.util.List.of(countField), tx);

			// ============================================================
			// 11) FINAL PROJECTION (čisto)
			// ============================================================
			Plan finalPlan = new ProjectionPlan(sortedAsc, java.util.List.of(studname, countField));

			System.out.println("\nLOGICKI PLAN UPITA (SMJ, optimizovan):");
			finalPlan.printPlan(0);

			long t0 = System.currentTimeMillis();

			Scan s = finalPlan.open();

			// Pošto SortPlan radi ASC, ovde materijalizujemo i štampamo obrnutim redom (DESC)
			java.util.List<String> out = new java.util.ArrayList<>();

			while (s.next()) {
				String name = s.getString(studname);
				int desetke = s.getInt(countField);
				out.add(name + "\t\t" + desetke);
			}
			s.close();

			System.out.println("\nStudent\t\t\tDesetke");
			System.out.println("-----------------------------------------");

			for (int i = out.size() - 1; i >= 0; i--) {
				System.out.println(out.get(i));
			}
			System.out.println();

			long t1 = System.currentTimeMillis();
			System.out.println("Rows=" + out.size() + ", time(ms)=" + (t1 - t0));

			System.out.println("Blocks accessed estimate: " + finalPlan.blocksAccessed());
			System.out.println("Records output estimate: " + finalPlan.recordsOutput());

			tx.commit();
		} catch (RuntimeException e) {
			if (tx != null) tx.rollback();
			throw e;
		}
	}

	// parser vraca logicki plan upita
	private static void queryBasePlanByParser() throws JSQLParserException {
		System.out.println("\nBAZNI PLAN - JSQLPARSER\n");
		Transaction tx = new Transaction();

		String sql =
				"select studname, ocena " +
						"from student, predmet, ispit, polaganje " +
						"where ispid=ispitid " +
						"and pid=predmetid " +
						"and sid=polagstudid " +
						"and predgod=1 " +
						"and ocena=10 " +
                "GROUP by studname, ocena " +
                "ORDER BY desetke ";

		QueryData qd = SimpleDBEngine.planner().getParsingResult(sql);
		System.out.println("Rezultat parsiranja:\n" + qd);

		Plan p = SimpleDBEngine.planner().createQueryPlanFromParsingResult(qd, tx);

        System.out.println("GROUP BY: " + qd.groupBy());
        System.out.println("ORDER BY: " + qd.orderBy());


        System.out.println("\nBAZNI (parser) LOGICKI PLAN:");
		p.printPlan(0);

		tx.commit();
	}

	// neoptimizovan plan (onaj koji je parser dao) bez group by i sort by
	private static void queryBasePlan() {

		Transaction tx = new Transaction();

		// 1) TABLE SCAN plans
		Plan pStudent   = new TablePlan("student", tx);
		Plan pPredmet   = new TablePlan("predmet", tx);
		Plan pIspit     = new TablePlan("ispit", tx);
		Plan pPolaganje = new TablePlan("polaganje", tx);

		// 2) CROSS PRODUCTS (same order as parser output)
		Plan cp1 = new CrossProductPlan(pStudent, pPredmet);
		Plan cp2 = new CrossProductPlan(cp1, pIspit);
		Plan cp3 = new CrossProductPlan(cp2, pPolaganje);

		// 3) SELECTION predicate (all conditions at the top)
		Predicate pred = new Predicate(new Term(
				new FieldNameExpression("ispid"),
				new FieldNameExpression("ispitid")
		));

		pred.conjoinWith(new Predicate(new Term(
				new FieldNameExpression("pid"),
				new FieldNameExpression("predmetid")
		)));

		pred.conjoinWith(new Predicate(new Term(
				new FieldNameExpression("sid"),
				new FieldNameExpression("polagstudid")
		)));

		pred.conjoinWith(new Predicate(new Term(
				new FieldNameExpression("predgod"),
				new ConstantExpression(new IntConstant(1))
		)));

		pred.conjoinWith(new Predicate(new Term(
				new FieldNameExpression("ocena"),
				new ConstantExpression(new IntConstant(10))
		)));

		Plan sel = new SelectionPlan(cp3, pred);

		// 4) PROJECTION ([studname, ocena])
		Plan proj = new ProjectionPlan(sel, java.util.List.of("studname", "ocena"));

		System.out.println("\nRUČNO KREIRAN (bazni) LOGIČKI PLAN:");
		proj.printPlan(0);

		// 5) Execute + timing
		long t0 = System.currentTimeMillis();

		Scan s = proj.open();
		int rows = 0;

		while (s.next()) {
			// samo “prođi” kroz rezultate (bez štampe) da ne meriš System.out trošak
			s.getString("studname");
			s.getInt("ocena");
			rows++;
		}
		s.close();

		long t1 = System.currentTimeMillis();

		System.out.println("Rows=" + rows + ", time(ms)=" + (t1 - t0));
		System.out.println("Blocks accessed estimate: " + proj.blocksAccessed());
		System.out.println("Records output estimate: " + proj.recordsOutput());

		tx.commit();
	}


	private static void querySQL1() throws JSQLParserException {
		// Kreiranje transakcije
		Transaction tx = new Transaction();

		// Kreiranje SQL upita
		String sqlQuery = "select sname, smerName "
	        + "from STUDENT, SMER "
	        + "where smerId = SMId";

		// Parsiranje upita i kreiranje plana izvrsavanja upita
		//Plan p = SimpleDBEngine.planner().createQueryPlan(qry, tx);
		QueryData queryResult =  SimpleDBEngine.planner().getParsingResult(sqlQuery);
		System.out.println("Rezultat Parsiranja:\n"+queryResult);
		Plan p = SimpleDBEngine.planner().createQueryPlanFromParsingResult(queryResult, tx);

		System.out.println("\nLOGICKI PLAN UPITA:");
		p.printPlan(0);

		// Inicijalizacija plana upita, tj. svih operatora u stablu upita koji se izvrsavaju
		Scan s = p.open();

		System.out.println("\nSpisak svih studenata:\n");
		System.out.println("Student\t\t\tSmer");
		System.out.println("-----------------------------------------");
		while (s.next()) {
			String sname = s.getString("sname"); 	//SimpleDB cuva naziv kolona
			String dname = s.getString("smername"); //sa malim slovima (lower case)
			System.out.println(sname + "\t\t" + dname);
		}
		s.close();
		tx.commit();
	}


	/**
	    * Executes the specified SQL query string.
	    * The method calls the query planner to create a plan
	    * for the query. 
	    */
	   public static void executeSQLQuery(String sqlQuery) {
		   Transaction tx = null;
		   try {
	         tx = new Transaction();
	         Plan plan = SimpleDBEngine.planner().createQueryPlan(sqlQuery, tx);
	         Scan scan = plan.open();
	         Schema sch = plan.schema();
	         
	         while (scan.next()) {
	        	 //String sname = scan.getString("sname");
	        	 //int age = scan.getInt("age");
	        	 
	        	 
	         }
	         
	      }
	      catch(RuntimeException e) {
	         tx.rollback();
	         throw e;
	      } catch (JSQLParserException e) {
               throw new RuntimeException(e);
           }
       }
	   
	   /**
	    * Executes the specified SQL update command.
	    * The method sends the command to the update planner,
	    * which executes it.
	   */
	   public static int executeSQLUpdate(String sqlCmd) throws JSQLParserException {
		   Transaction tx = null;
		   try {
	         tx = new Transaction();
	         int result = SimpleDBEngine.planner().executeUpdate(sqlCmd, tx);

	         tx.commit();
	         return result;
	      }
	      catch(RuntimeException | JSQLParserException e) {
	         tx.rollback();
	         throw e;
	      }
	   }
	   
	   public static boolean initStudentDB() {
			// Kreiranje baze podataka, sto podrazumeva fajl sa podacima i podatke u sistemskom katalogu
			return SimpleDBEngine.init("studentdb");
		}
		
		public static void initStudentDBData() throws JSQLParserException {
			String createTableSQL = "create table STUDENT(sid int, sname varchar(25), smerId int, godDipl int)";
			
			MainQueryRunner.executeSQLUpdate(createTableSQL);
			System.out.println("Table STUDENT created.");

			String insertSQLString = "insert into STUDENT(sid, sname, smerId, godDipl) values ";
			String[] studvals = {"(1, 'Milan Petrovic', 1, 2025)",
								 "(2, 'Jovan Jovanovic', 2, 2025)",
								 "(3, 'Ana Mirkovic', 3, 2021)",
								 "(4, 'Maja Spasic', 2, 2023)",
								 "(5, 'Veljko Peric', 3, 2022)",
								 "(6, 'Bojana Mijatovic', 1, 2025)",
								 "(7, 'Lazar Kostic', 2, 2025)",
								 "(8, 'Milica Milic', 3, 2024)",
								 "(9, 'Nikola Urosevic', 2, 2022)"};
			for (int i=0; i<studvals.length; i++)
				MainQueryRunner.executeSQLUpdate(insertSQLString + studvals[i]);
			System.out.println("STUDENT records inserted.");

			createTableSQL = "create table SMER(smid int, smerName varchar(25))";
			MainQueryRunner.executeSQLUpdate(createTableSQL);
			System.out.println("Table SMER created.");

			insertSQLString = "insert into SMER(smid, smerName) values ";
			String[] deptvals = {"(1, 'Softversko inzenjerstvo')",
								 "(2, 'Racunarsko inzenjerstvo')",
								 "(3, 'Racunarske nauke')"};
			for (int i=0; i<deptvals.length; i++)
				MainQueryRunner.executeSQLUpdate(insertSQLString + deptvals[i]);
			System.out.println("SMER records inserted.");

			createTableSQL = "create table PREDMET(PId int, naziv varchar(25), smerId int)";
			MainQueryRunner.executeSQLUpdate(createTableSQL);
			System.out.println("Table PREDMET created.");

			insertSQLString = "insert into PREDMET(PId, naziv, smerId) values ";
			String[] coursevals = {"(12, 'Napredne Baze Podataka', 1)",
								   "(22, 'Distribuirani Sistemi', 2)",
								   "(32, 'Teorija Algoritama', 3)",
								   "(42, 'Diskretna Matematika', 3)",
								   "(52, 'Racunarske Mreze', 2)",
								   "(62, 'Softverske metodologije', 1)"};
			for (int i=0; i<coursevals.length; i++)
				MainQueryRunner.executeSQLUpdate(insertSQLString + coursevals[i]);
			System.out.println("PREDMET records inserted.");

			/*
			s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
			stmt.executeUpdate(s);
			System.out.println("Table SECTION created.");

			s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
			String[] sectvals = {"(13, 12, 'turing', 2004)",
								 "(23, 12, 'turing', 2005)",
								 "(33, 32, 'newton', 2000)",
								 "(43, 32, 'einstein', 2001)",
								 "(53, 62, 'brando', 2001)"};
			for (int i=0; i<sectvals.length; i++)
				stmt.executeUpdate(s + sectvals[i]);
			System.out.println("SECTION records inserted.");

			s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
			stmt.executeUpdate(s);
			System.out.println("Table ENROLL created.");

			s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
			String[] enrollvals = {"(14, 1, 13, 'A')",
								   "(24, 1, 43, 'C' )",
								   "(34, 2, 43, 'B+')",
								   "(44, 4, 33, 'B' )",
								   "(54, 4, 53, 'A' )",
								   "(64, 6, 53, 'A' )"};
			for (int i=0; i<enrollvals.length; i++)
				stmt.executeUpdate(s + enrollvals[i]);
			System.out.println("ENROLL records inserted.");
			*/
		} 
	
}
