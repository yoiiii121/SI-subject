import java.util.Scanner;

import si.si.e1.TestWorldCount;
import si.si.e2.TestManagementDB_SQL;
import si.si.e3.TestManagementDB_Join_JSON;
import si.si.e4.TestManagementTwitter;

public final class Run_Main {

	public static void main(String[] args) throws Exception {
		String sSistemaOperativo = System.getProperty("os.name");
		if (sSistemaOperativo.contains("Windows")) {
			System.setProperty("hadoop.home.dir", "c:\\winutil\\");
			System.setProperty("spark.sql.warehouse.dir",
					"file:///${System.getProperty(\"user.dir\")}/spark-warehouse".replaceAll("\\\\", "/"));
		}
		System.out.println("Program is starting");
		int nexercises = 4;
		int opt;
		String entradaTeclado = "";
		Scanner entradaEscaner = new Scanner(System.in);
		System.out.println("Options:" + " \n1 = Unsorted words Count" + " \n2 = Extraction of one CSV column"
				+ " \n3 = Join of two JSON related tables" + " \n4 = Twitts extraction of Twitter\n");
		do {
			System.out.println("Please: input a number from 1 to " + nexercises);

			entradaTeclado = entradaEscaner.nextLine();

			try {
				opt = Integer.valueOf(entradaTeclado);
			} catch (NumberFormatException ne) {
				opt = nexercises + 1;
			}

			switch (opt) {
			case 1:
				TestWorldCount.main(args);
				break;
			case 2:
				TestManagementDB_SQL.main(args);
				break;
			case 3:
				TestManagementDB_Join_JSON.main(args);
				break;
			case 4:
				TestManagementTwitter.main(args);
				break;
			default:
				System.out.println("Number invalid input the number again");
			}

		} while (opt < 1 || opt > nexercises);
		entradaEscaner.close();
	}

}