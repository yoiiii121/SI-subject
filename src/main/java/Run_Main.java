import java.util.Scanner;

import si.si.e1.TestWorldCount;
import si.si.e10.TestKmeans;
import si.si.e2.TestManagementDB_SQL;
import si.si.e3.TestManagementDB_Join_JSON;
import si.si.e4.TestManagementTwitter;
import si.si.e7.TestSparkSQLMLlibRSContentBased;
import si.si.e8.TestFPGrouwth;
import si.si.e9.TestFPGrouwth2;

public final class Run_Main {

	public static void main(String[] args) throws Exception {

		String sSistemaOperativo = System.getProperty("os.name");

		if (sSistemaOperativo.contains("Windows")) {

			System.setProperty("hadoop.home.dir", "c:\\winutil\\");

			System.setProperty("spark.sql.warehouse.dir",

					"file:///${System.getProperty(\"user.dir\")}/spark-warehouse".replaceAll("\\\\", "/"));

		}
		System.out.println("Program is starting");
		int nexercises = 8;
		int opt;
		String entradaTeclado = "";
		Scanner entradaEscaner = new Scanner(System.in);
		do {
			System.out.println("Please: input a number from 1 to " + nexercises);

			entradaTeclado = entradaEscaner.nextLine();

			try {
				opt = Integer.valueOf(entradaTeclado);
			} catch (NumberFormatException ne) {
				opt = nexercises + 1;
			}

			String[] path = new String[2];

			String computerUser = "Nacho";
			switch (opt) {
			case 1:
				path[0] = "./src/main/java/si/si/e1/";
				TestWorldCount.main(path);
				break;
			case 2:
				path[0] = "./src/main/java/si/si/e2/";
				TestManagementDB_SQL.main(path);
				break;
			case 3:
				path[0] = "./src/main/java/si/si/e3/";
				TestManagementDB_Join_JSON.main(path);
				break;
			case 4:
				System.out.println("if the execution have failed, you should change the value computerUser");
				path[0] = "/home/" + computerUser + "/Escritorio/";
				path[1] = "C:\\Users\\" + computerUser + "\\Desktop\\";
				TestManagementTwitter.main(path);
				break;
			case 5:
				System.out.println("if the execution have failed, you should change the value computerUser");
				path[0] = "/home/" + computerUser + "/Escritorio/";
				path[1] = "C:\\Users\\" + computerUser + "\\Desktop\\";
				TestSparkSQLMLlibRSContentBased.main(path);
				break;
			case 6:
				TestFPGrouwth.main(path);
				break;
			case 7:
				TestFPGrouwth2.main(path);
				break;
			case 8:
				
				TestKmeans.main(path);
				break;
			default:
				System.out.println("Number invalid input the number again");
			}

		} while (opt < 1 || opt > nexercises);
		entradaEscaner.close();
	}

}